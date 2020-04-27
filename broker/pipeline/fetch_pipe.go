package pipeline

import (
	"bytes"
	"context"
	"errors"
	"github.com/paust-team/paustq/broker/network"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustq_proto "github.com/paust-team/paustq/proto"
	"sync"
	"sync/atomic"
)

type FetchPipe struct {
	session *network.Session
	db      *storage.QRocksDB
}

func (f *FetchPipe) Build(in ...interface{}) error {
	casted := true
	var ok bool

	f.session, ok = in[0].(*network.Session)
	casted = casted && ok

	f.db, ok = in[1].(*storage.QRocksDB)
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

		topic := f.session.Topic()

		for in := range inStream {
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

			if req.StartOffset > atomic.LoadUint64(&topic.Size) {
				errCh <- errors.New("invalid start offset")
				return
			}

			for !f.session.IsClosed() {
				it := f.db.Scan(storage.RecordCF)
				currentLastOffset := atomic.LoadUint64(&topic.Size) - 1

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
					outStream <- out
					prevKey.SetData(key.Data())
					first = false
				}

				if currentLastOffset < atomic.LoadUint64(&topic.Size)-1 {
					continue
				}

				topic.WaitPublish()

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
