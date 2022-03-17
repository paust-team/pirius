package pipeline

import (
	"bytes"
	"encoding/binary"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/pqerror"
	shapleq_proto "github.com/paust-team/shapleq/proto/pb"
	"sync"
	"time"
	"unsafe"
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
		defer close(errCh)
		defer close(outStream)
		wg := &sync.WaitGroup{}

		defer wg.Wait()
		defer close(inStreamClosed)

		for in := range inStream {
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

			for _, topic := range req.Topics {
				for _, offset := range topic.Offsets {
					if offset.StartOffset == 0 {
						errCh <- pqerror.TopicFragmentOffsetNotSetError{}
						return
					}
					wg.Add(1)
					go func(topicName string, offset *shapleq_proto.Topic_FragmentOffset) {
						defer wg.Done()
						f.iterateRecords(topicName, offset.FragmentId, offset.StartOffset, outStream, inStreamClosed)
					}(topic.TopicName, offset)
				}
			}
		}
	}()

	return outStream, errCh, nil
}

func (f *FetchPipe) iterateRecords(topicName string, fragmentId uint32, startOffset uint64, outStream chan interface{}, inStreamClosed chan struct{}) {

	first := true
	prevKey := storage.NewRecordKeyFromData(topicName, fragmentId, startOffset)
	it := f.db.Scan(storage.RecordCF)

	prefix := make([]byte, len(topicName)+1+int(unsafe.Sizeof(uint32(0))))
	copy(prefix, topicName+"@")
	binary.BigEndian.PutUint32(prefix[len(topicName)+1:], fragmentId)
	iterateInterval := time.Millisecond * 10
	timer := time.NewTimer(iterateInterval)
	defer timer.Stop()

	for {
		select {
		case <-inStreamClosed:
			return
		case <-timer.C:
			if f.session.IsClosed() {
				return
			}

			it.Seek(prevKey.Data())
			if !first && it.Valid() {
				it.Next()
			}
			for ; it.Valid() && bytes.HasPrefix(it.Key().Data(), prefix); it.Next() {

				key := storage.NewRecordKey(it.Key())
				keyOffset := key.Offset()
				if first {
					if keyOffset != startOffset {
						break
					}
				} else {
					if keyOffset != prevKey.Offset() && keyOffset-prevKey.Offset() != 1 {
						break
					}
				}

				value := storage.NewRecordValue(it.Value())
				value.SeqNum()
				fetchRes := message.NewFetchResponseMsg(value.PublishedData(), keyOffset, value.SeqNum(), value.NodeId(), topicName, fragmentId)

				select {
				case <-inStreamClosed:
					return
				case outStream <- fetchRes:
					prevKey.SetData(key.Data())
					first = false
				}
			}
		}
		timer.Reset(iterateInterval)
	}
}
