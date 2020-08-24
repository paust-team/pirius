package pipeline

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto"
	"runtime"
	"sync"
)

type FetchPipe struct {
	session *internals.Session
	db      *storage.QRocksDB
}

func (f *FetchPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	f.session, ok = in[0].(*internals.Session)
	casted = casted && ok

	f.db, ok = in[1].(*storage.QRocksDB)
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
	wg := sync.WaitGroup{}

	go func() {
		defer close(outStream)
		defer close(errCh)
		defer wg.Wait()

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

			iterCtx, cancel := context.WithCancel(context.Background())

			wg.Add(1)
			go func() {
				defer cancel()
				for {
					select {
					case <-inStreamClosed:
						return
					default:
					}
					runtime.Gosched()
				}
			}()

			go func(req *shapleq_proto.FetchRequest) {
				defer wg.Done()
				resCh := f.continuousIterate(iterCtx, topic, req.StartOffset)

				for {
					select {
					case res, ok := <-resCh:
						if !ok {
							return
						}
						out, err := message.NewQMessageFromMsg(message.STREAM, &res)
						if err != nil {
							errCh <- err
							return
						}
						outStream <- out
					default:
					}
					runtime.Gosched()
				}
			}(in.(*shapleq_proto.FetchRequest))
		}

	}()

	return outStream, errCh, nil
}

func (f *FetchPipe) continuousIterate(ctx context.Context, topic *internals.Topic, startOffset uint64) <-chan shapleq_proto.FetchResponse {

	responseCh := make(chan shapleq_proto.FetchResponse)

	go func() {
		defer close(responseCh)
		it := f.db.Scan(storage.RecordCF)

		recordKey := storage.NewRecordKeyFromData(topic.Name(), startOffset)
		it.Seek(recordKey.Data())
		prefixData := []byte(topic.Name() + "@")
		once := sync.Once{}
		sent := false

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if it.ValidForPrefix(prefixData) {
					key := storage.NewRecordKey(it.Key())
					keyOffset := key.Offset()
					prevOffset := recordKey.Offset()

					// TODO:: handle the order of record
					if keyOffset != startOffset && keyOffset-prevOffset != 1 {
						fmt.Printf("missing offset current=%d, prev=%d", keyOffset, prevOffset)
					}
					responseCh <- shapleq_proto.FetchResponse{
						Data:       it.Value().Data(),
						LastOffset: topic.LastOffset(),
						Offset:     keyOffset,
					}
					recordKey.SetData(it.Key().Data())

					once.Do(func() {
						sent = true
					})

					it.Next()
				} else {
					it.Seek(recordKey.Data())
					if it.ValidForPrefix(prefixData) && sent {
						it.Next()
					}
				}
			}
			runtime.Gosched()
		}
	}()

	return responseCh
}
