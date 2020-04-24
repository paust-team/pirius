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

		for in := range inStream {
			if f.session.State() != network.ON_SUBSCRIBE {
				err := f.session.SetState(network.ON_SUBSCRIBE)
				if err != nil {
					errCh <- err
					return
				}
			}

			req := in.(*paustq_proto.FetchRequest)

			topic := f.session.Topic()

			var fetchRes paustq_proto.FetchResponse
			var lastReadOffset *uint64 = nil
			prevKey := storage.NewRecordKeyFromData(topic.Name(), req.StartOffset)

			for !f.session.IsClosed() {

				it := f.db.Scan(storage.RecordCF)
				expectedLastOffset := atomic.LoadUint64(&topic.Size) - 1

				for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), []byte(topic.Name()+"@")); it.Next() {
					key := storage.NewRecordKey(it.Key())
					keyOffset := key.Offset()

					if lastReadOffset != nil {
						if *lastReadOffset == keyOffset {
							continue
						} else if keyOffset-*lastReadOffset != 1 {
							break
						}
					} else if keyOffset != req.StartOffset {
						break
					}

					fetchRes.Reset()
					fetchRes = paustq_proto.FetchResponse{
						Data:       it.Value().Data(),
						LastOffset: expectedLastOffset,
						Offset:     keyOffset,
					}

					out, err := message.NewQMessageFromMsg(&fetchRes)
					if err != nil {
						errCh <- err
						return
					}
					outStream <- out

					lastReadOffset = &keyOffset
				}

				if lastReadOffset != nil {
					prevKey = storage.NewRecordKeyFromData(topic.Name(), *lastReadOffset)
				}

				if expectedLastOffset < atomic.LoadUint64(&topic.Size)-1 {
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
