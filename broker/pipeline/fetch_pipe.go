package pipeline

import (
	"bytes"
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
)

type FetchPipe struct {
	session 	*network.Session
	db      	*storage.QRocksDB
	notifier	*internals.Notifier
}

func (f *FetchPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	f.session, ok = in[0].(*network.Session)
	casted = casted && ok

	f.db, ok = in[1].(*storage.QRocksDB)
	casted = casted && ok

	f.notifier, ok = in[2].(*internals.Notifier)
	casted = casted && ok

	if !casted {
		return errors.New("failed to build fetch pipe")
	}

	return nil
}

func (f *FetchPipe) Ready(ctx context.Context, inStream <-chan interface{}, wg *sync.WaitGroup) (
	<-chan interface{}, <-chan error, error) {
	outStream := make(chan interface{})
	errCh := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outStream)
		defer close(errCh)


		for in := range inStream {

			topic := f.session.Topic()

			if f.session.State() != network.ON_SUBSCRIBE {
				err := f.session.SetState(network.ON_SUBSCRIBE)
				if err != nil {
					errCh <- err
					return
				}
			}

			req := in.(*paustq_proto.FetchRequest)
			var fetchRes paustq_proto.FetchResponse

			first := true
			prevKey := storage.NewRecordKeyFromData(topic.Name(), req.StartOffset)

			if req.StartOffset > topic.LastOffset() {
				errCh <- errors.New("invalid start offset")
				return
			}

			for !f.session.IsClosed() {
				currentLastOffset := topic.LastOffset()
				it := f.db.Scan(storage.RecordCF)
				it.Seek(prevKey.Data())
				if !first && it.Valid() {
					it.Next()
				}

				for ;it.Valid() && bytes.HasPrefix(it.Key().Data(), []byte(topic.Name()+"@")); it.Next() {
					key := storage.NewRecordKey(it.Key())
					keyOffset := key.Offset()

					if first {
						if keyOffset != req.StartOffset {
							break
						}
					} else {
						if keyOffset != prevKey.Offset() && keyOffset - prevKey.Offset() != 1 {
							break
						}
					}

					fetchRes.Reset()
					fetchRes = paustq_proto.FetchResponse{
						Data:       it.Value().Data(),
						LastOffset: currentLastOffset,
						Offset:     keyOffset,
					}

					out, err := message.NewQMessageFromMsg(&fetchRes)
					if err != nil {
						errCh <- err
						return
					}

					select {
					case <-ctx.Done():
						return
					case outStream <- out:
						prevKey.SetData(key.Data())
						first = false
					}
				}

				if prevKey.Offset() < topic.LastOffset() {
					continue
				}
				f.waitForNews(topic.Name(), prevKey.Offset())
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	return outStream, errCh, nil
}

func (f FetchPipe) waitForNews(topicName string, currentLastOffset uint64) {
	subscribeChan := make(chan bool)
	defer close(subscribeChan)
	subscription := &internals.Subscription{TopicName: topicName, LastFetchedOffset: currentLastOffset, SubscribeChan: subscribeChan}
	f.notifier.RegisterSubscription(subscription)
	<-subscribeChan
}