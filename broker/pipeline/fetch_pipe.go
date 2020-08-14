package pipeline

import (
	"context"
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

			go func(req *shapleq_proto.FetchRequest) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				resCh := f.continuousIterate(ctx, topic, req.StartOffset)

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
					case <-inStreamClosed:
						return
					}
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

		keyData := storage.NewRecordKeyFromData(topic.Name(), startOffset).Data()
		it.Seek(keyData)
		prefixData := []byte(topic.Name() + "@")

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if it.ValidForPrefix(prefixData) {
					key := storage.NewRecordKey(it.Key())
					keyOffset := key.Offset()
					responseCh <- shapleq_proto.FetchResponse{
						Data:       it.Value().Data(),
						LastOffset: topic.LastOffset(),
						Offset:     keyOffset,
					}
					keyData = it.Key().Data()
					it.Next()
				} else {
					it.Seek(keyData)
					if it.ValidForPrefix(prefixData) {
						it.Next()
					}
				}
			}
			runtime.Gosched()
		}
	}()

	return responseCh
}
