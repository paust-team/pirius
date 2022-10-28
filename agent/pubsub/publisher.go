package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"runtime"
	"time"
	"unsafe"
)

type TopicData struct {
	SeqNum uint64
	Data   []byte
}

type Publisher struct {
	pb.PubSubServer
	PublisherID         string
	DB                  *storage.QRocksDB
	Bootstrapper        *bootstrapping.BootstrapService
	server              *grpc.Server
	NextFragmentOffsets storage.TopicFragmentOffsets // offsets to write
}

func (p *Publisher) PreparePublication(ctx context.Context, topicName string, retentionPeriodSec uint64,
	inStream chan TopicData) (chan error, error) {

	fragmentIds, err := p.findPublicationFragments(topicName)
	if err != nil {
		return nil, err
	}

	// load topic policy and set fragments selecting rule
	topicInfo, err := p.Bootstrapper.GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	getFragmentsToWrite := TopicWritingRule(topicInfo.Options(), fragmentIds)

	// load next offsets
	value, _ := p.NextFragmentOffsets.LoadOrStore(topicName, make(map[uint]uint64))
	nextOffsets := value.(map[uint]uint64)
	for _, fragmentId := range fragmentIds {
		if _, ok := nextOffsets[fragmentId]; ok {
			nextOffsets[fragmentId] = 1
		}
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				logger.Info("stop publish from ctx.Done()")
				return
			case data, ok := <-inStream:
				if !ok {
					logger.Info("stop publish from send buffer closed")
					return
				}
				for _, fragmentId := range getFragmentsToWrite() {
					err := p.onReceiveData(data, topicName, fragmentId, nextOffsets[fragmentId], retentionPeriodSec)
					if err != nil {
						errCh <- err
						return
					}
					nextOffsets[fragmentId]++
					p.NextFragmentOffsets.Store(topicName, nextOffsets)
				}
			}
		}
	}()
	return errCh, nil
}

func (p *Publisher) SetupGrpcServer(ctx context.Context, bindAddress string, port uint) error {
	if p.server == nil {
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		pb.RegisterPubSubServer(grpcServer, p)

		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindAddress, port))
		if err != nil {
			return err
		}

		p.server = grpcServer

		go func() {
			if err = grpcServer.Serve(lis); err != nil {
				logger.Error(err.Error())
			} else {
				logger.Info("grpc server stopped")
			}
		}()

		go func() {
			select {
			case <-ctx.Done():
				p.server.Stop()
			}
		}()
	}

	return nil
}

// find fragment info through topic-fragment discovery
func (p *Publisher) findPublicationFragments(topicName string) (fragmentIds []uint, err error) {
	topicFragmentFrame, err := p.Bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return nil, err
	}
	fragMappings := topicFragmentFrame.FragMappingInfo()
	for fragId, fragInfo := range fragMappings {
		if fragInfo.Active && fragInfo.PublisherId == p.PublisherID {
			fragmentIds = append(fragmentIds, fragId)
		}
	}
	return
}

func (p *Publisher) onReceiveData(data TopicData, topicName string, fragmentId uint, offset uint64, retentionPeriodSec uint64) error {
	expirationDate := storage.GetNowTimestamp() + retentionPeriodSec
	return p.DB.PutRecord(topicName, uint32(fragmentId), offset, data.SeqNum, data.Data, expirationDate)
}

// gRPC implementation

func (p *Publisher) Subscribe(subscription *pb.Subscription, stream pb.PubSub_SubscribeServer) error {
	sendBuf := make(chan *pb.SubscriptionResult_Fetched)
	defer close(sendBuf)

	dontWait := false
	var batched []*pb.SubscriptionResult_Fetched
	maxBatchSize := int(subscription.MaxBatchSize)

	flushIntervalMs := time.Millisecond * time.Duration(subscription.FlushInterval)
	timer := time.NewTimer(flushIntervalMs)
	defer timer.Stop()

	flush := func() error {
		if err := stream.Send(&pb.SubscriptionResult{Magic: 1, Results: batched}); err != nil {
			logger.Error(err.Error())
			return err
		}

		logger.Debug("sent",
			zap.Int("num data", len(batched)),
			zap.Uint64("last seqNum", batched[len(batched)-1].SeqNum))

		timer.Reset(flushIntervalMs)
		batched = nil
		dontWait = false
		return nil
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	for _, offsetInfo := range subscription.Offsets {
		var startOffset uint64
		// when start offset is not set, set current offset(to write) as last offset
		if offsetInfo.StartOffset == nil {
			value, _ := p.NextFragmentOffsets.Load(subscription.TopicName)
			nextOffsets := value.(map[uint]uint64)
			startOffset = nextOffsets[uint(offsetInfo.FragmentId)]
		} else {
			startOffset = *offsetInfo.StartOffset
		}
		go p.onFetchData(ctx, subscription.TopicName, offsetInfo.FragmentId, startOffset, sendBuf)
	}

	for {
		select {
		case <-stream.Context().Done():
			logger.Debug("stream closed from client")
			return nil

		case fetched := <-sendBuf:
			batched = append(batched, fetched)
			if len(batched) >= maxBatchSize || (len(batched) > 0 && dontWait) {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if len(batched) > 0 {
				if err := flush(); err != nil {
					timer.Reset(flushIntervalMs)
				}
			} else {
				// if flush time is over and no data collected,
				// then don't wait until flush interval
				dontWait = true
			}
		}
	}
}

func (p *Publisher) onFetchData(ctx context.Context, topicName string, fragmentId uint32, startOffset uint64, outStream chan *pb.SubscriptionResult_Fetched) {

	prefix := make([]byte, len(topicName)+1+int(unsafe.Sizeof(uint32(0))))
	copy(prefix, topicName+"@")
	binary.BigEndian.PutUint32(prefix[len(topicName)+1:], fragmentId)
	waitInterval := time.Millisecond * 10
	timer := time.NewTimer(waitInterval)
	defer timer.Stop()

	currentOffset := startOffset
	prevKey := storage.NewRecordKeyFromData(topicName, fragmentId, currentOffset)

	iterateCount := 0
	rescanCheckPoint := 0
	rescanThreshold := 1000
	it := p.DB.Scan(storage.RecordCF)
	defer it.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for it.Seek(prevKey.Data()); it.Valid() && bytes.HasPrefix(it.Key().Data(), prefix); it.Next() {
				key := storage.NewRecordKey(it.Key())
				offset := key.Offset()
				key.Free()
				if offset != currentOffset {
					break
				}

				value := storage.NewRecordValue(it.Value())
				topicData := &pb.SubscriptionResult_Fetched{
					FragmentId: fragmentId,
					Offset:     currentOffset,
					SeqNum:     value.SeqNum(),
					Data:       value.PublishedData(),
				}
				value.Free()
				select {
				case <-ctx.Done():
					return
				case outStream <- topicData:
					currentOffset++
					prevKey.SetOffset(currentOffset)
				}
				iterateCount++
				runtime.Gosched()
			}

			if iterateCount-rescanCheckPoint > rescanThreshold {
				rescanCheckPoint = iterateCount
				it.Close()
				it = p.DB.Scan(storage.RecordCF)
			}
		}
		timer.Reset(waitInterval)
	}
}
