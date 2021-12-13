package pipeline

import (
	"bytes"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/zookeeper"
	"runtime"
	"sync"
	"time"
)

type FetchPipe struct {
	session   *internals.Session
	db        *storage.QRocksDB
	zkqClient *zookeeper.ZKQClient
}

func (f *FetchPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	f.session, ok = in[0].(*internals.Session)
	casted = casted && ok

	f.db, ok = in[1].(*storage.QRocksDB)
	casted = casted && ok

	f.zkqClient, ok = in[2].(*zookeeper.ZKQClient)
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
		defer close(errCh)
		defer close(outStream)
		wg := &sync.WaitGroup{}

		defer wg.Wait()
		defer close(inStreamClosed)

		for in := range inStream {
			topicName := f.session.TopicName()
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
			f.session.SetMaxBatchSize(req.GetMaxBatchSize())
			f.session.SetFlushInterval(req.GetFlushInterval())

			first := true
			prevKey := storage.NewRecordKeyFromData(topicName, req.StartOffset)

			it := f.db.Scan(storage.RecordCF)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-inStreamClosed:
						return
					case <-time.After(time.Millisecond * 10):
						if f.session.IsClosed() {
							return
						}

						topicData, err := f.zkqClient.GetTopicData(topicName)
						if err != nil {
							errCh <- err
							return
						}
						currentLastOffset := topicData.LastOffset()
						it.Seek(prevKey.Data())
						if !first && it.Valid() {
							it.Next()
						}

						for ; it.Valid() && bytes.HasPrefix(it.Key().Data(), []byte(topicName+"@")); it.Next() {
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

							value := storage.NewRecordValue(it.Value())
							value.SeqNum()
							fetchRes := message.NewFetchResponseMsg(value.PublishedData(), keyOffset, value.SeqNum(), value.NodeId(), currentLastOffset)

							select {
							case <-inStreamClosed:
								return
							case outStream <- fetchRes:
								prevKey.SetData(key.Data())
								first = false
							}
						}
					}

					runtime.Gosched()
				}
			}()
			runtime.Gosched()
		}
	}()

	return outStream, errCh, nil
}
