package pipeline

import (
	"bytes"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"sync"
)

type FetchPipe struct {
	session  *internals.Session
	db       *storage.QRocksDB
	notifier *internals.Notifier
}

func (f *FetchPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	f.session, ok = in[0].(*internals.Session)
	casted = casted && ok

	f.db, ok = in[1].(*storage.QRocksDB)
	casted = casted && ok

	f.notifier, ok = in[2].(*internals.Notifier)
	casted = casted && ok

	if !casted {
		return pqerror.PipeBuildFailError{PipeName: "fetch"}
	}

	return nil
}

func (f *FetchPipe) Ready(inStream <-chan interface{}) (<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)
	inStreamClosed := make(chan struct{})

	once := sync.Once{}
	go func() {
		defer close(outStream)
		defer close(errCh)
		defer close(inStreamClosed)

		for in := range inStream {
			topic := f.session.Topic()
			once.Do(func() {
				if f.session.State() != internals.ON_SUBSCRIBE {
					err := f.session.SetState(internals.ON_SUBSCRIBE)
					if err != nil {
						errCh <- err
						return
					}
				}
			})

			req := in.(*shapleq_proto.FetchRequest)
			var fetchRes shapleq_proto.FetchResponse

			first := true
			prevKey := storage.NewRecordKeyFromData(topic.Name(), req.StartOffset)

			if req.StartOffset > topic.LastOffset() {
				errCh <- pqerror.InvalidStartOffsetError{
					Topic:       topic.Name(),
					StartOffset: req.StartOffset,
					LastOffset:  topic.LastOffset()}
				return
			}

			it := f.db.Scan(storage.RecordCF)

			go func() {
				for !f.session.IsClosed() {
					currentLastOffset := topic.LastOffset()
					it.Seek(prevKey.Data())
					if !first && it.Valid() {
						it.Next()
					}

					for ; it.Valid() && bytes.HasPrefix(it.Key().Data(), []byte(topic.Name()+"@")); it.Next() {
						key := storage.NewRecordKey(it.Key())
						keyOffset := key.Offset()

						if first {
							if keyOffset != req.StartOffset {
								break
							}
						} else {
							if keyOffset != prevKey.Offset() && keyOffset-prevKey.Offset() != 1 {
								break
							}
						}

						fetchRes.Reset()
						fetchRes = shapleq_proto.FetchResponse{
							Data:       it.Value().Data(),
							LastOffset: currentLastOffset,
							Offset:     keyOffset,
						}

						out, err := message.NewQMessageFromMsg(message.STREAM, &fetchRes)
						if err != nil {
							errCh <- err
							return
						}

						select {
						case <-inStreamClosed:
							return
						case outStream <- out:
							prevKey.SetData(key.Data())
							first = false
						}
					}

					if prevKey.Offset() < topic.LastOffset() {
						continue
					}
					f.waitForNews(inStreamClosed, topic.Name(), prevKey.Offset())
				}
			}()
		}
	}()

	return outStream, errCh, nil
}

func (f FetchPipe) waitForNews(inStreamClosed chan struct{}, topicName string, currentLastOffset uint64) {
	subscribeChan := make(chan bool)
	defer close(subscribeChan)
	subscription := &internals.Subscription{TopicName: topicName, LastFetchedOffset: currentLastOffset, SubscribeChan: subscribeChan}
	f.notifier.RegisterSubscription(subscription)
	select {
	case <-inStreamClosed:
		return
	case <-subscribeChan:
		return
	}
}
