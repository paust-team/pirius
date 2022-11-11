package pubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

type TopicData struct {
	SeqNum uint64
	Data   []byte
}

type Publisher struct {
	pb.PubSubServer
	id                    string
	address               string
	db                    *storage.QRocksDB
	bootstrapper          *bootstrapping.BootstrapService
	server                *grpc.Server
	currentPublishOffsets storage.TopicFragmentOffsets // current write offsets
	lastFetchedOffsets    storage.TopicFragmentOffsets // last read offsets
	wg                    sync.WaitGroup
	currentFragMappings   topic.FragMappingInfo
}

func NewPublisher(id string, address string, db *storage.QRocksDB, bootstrapper *bootstrapping.BootstrapService,
	publishedOffsets, fetchedOffsets storage.TopicFragmentOffsets) Publisher {
	return Publisher{
		id:                    id,
		address:               address,
		db:                    db,
		bootstrapper:          bootstrapper,
		server:                nil,
		currentPublishOffsets: publishedOffsets,
		lastFetchedOffsets:    fetchedOffsets,
		wg:                    sync.WaitGroup{},
	}
}

func (p *Publisher) PreparePublication(ctx context.Context, topicName string, retentionPeriodSec uint64,
	inStream chan TopicData) (chan error, error) {

	if err := p.setupGrpcServer(ctx); err != nil {
		return nil, err
	}

	// register watcher for topic fragment info
	fragmentWatchCh, err := p.bootstrapper.WatchFragmentInfoChanged(ctx, topicName)
	if err != nil {
		return nil, err
	}
	logger.Info("watcher for fragments registered", zap.String("topic", topicName))

	// register publisher path and wait for initial rebalance
	if err = p.bootstrapper.AddPublisher(topicName, p.id, p.address); err != nil {
		return nil, err
	}
	select {
	case initialFragment := <-fragmentWatchCh:
		p.currentFragMappings = initialFragment
	case <-time.After(time.Second * constants.InitialRebalanceTimeout):
		return nil, qerror.InvalidStateError{State: fmt.Sprintf("initial rebalance timed out for topic(%s)", topicName)}
	}

	// setup  publishing fragments
	activeFragIds, staleFragIds := p.findPublishingFragments(p.currentFragMappings)
	if len(activeFragIds) == 0 {
		return nil, qerror.InvalidStateError{State: fmt.Sprintf("no active fragments of topic(%s)", topicName)}
	}
	if err != nil {
		return nil, err
	}
	logger.Info("setup publishing fragments",
		zap.Uints("active-fragments", activeFragIds),
		zap.Uints("stale-fragments", staleFragIds))

	// load topic policy and set fragments selecting rule
	topicInfo, err := p.bootstrapper.GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	getFragmentsToWrite := TopicWritingRule(topicInfo.Options(), activeFragIds)

	// write staled fragment's data to active fragment
	var inStreams chan TopicData
	if len(staleFragIds) > 0 {
		transferCh := p.transferStaledRecords(ctx, topicName, staleFragIds)
		inStreams = helper.MergeChannels(inStream, transferCh)
	} else {
		inStreams = inStream
	}

	errCh := make(chan error, 2)
	p.wg.Add(1)
	go func() {
		defer close(errCh)
		defer p.wg.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Info("stop publishing: ctx.Done()")
				return
			case data, ok := <-inStreams:
				if !ok {
					logger.Info("stop publishing: send buffer closed")
					return
				}
				for _, fragmentId := range getFragmentsToWrite() {
					fragKey := storage.NewFragmentKey(topicName, fragmentId)
					value, _ := p.currentPublishOffsets.LoadOrStore(fragKey, uint64(1))
					currentOffset := value.(uint64)
					err = p.onReceiveData(data, topicName, fragmentId, currentOffset, retentionPeriodSec)
					if err != nil {
						errCh <- err
						return
					}
					p.currentPublishOffsets.Store(fragKey, currentOffset+1)
				}
			case fragMappingInfo, ok := <-fragmentWatchCh:
				if !ok {
					logger.Error("stop publishing: watch closed")
					errCh <- qerror.InvalidStateError{State: "watcher channel closed unexpectedly"}
					return
				}
				logger.Info("received new fragment mapping info", zap.String("topic", topicName))

				if p.isMappingUpdated(fragMappingInfo) {
					// reset publishing fragments
					logger.Info("resetting publishing fragments")
					activeFragIds, staleFragIds = p.findPublishingFragments(fragMappingInfo)
					if len(activeFragIds) == 0 {
						err = qerror.InvalidStateError{State: fmt.Sprintf("no active fragments of topic(%s)", topicName)}
						errCh <- err
						logger.Error("failed to reset publishing fragments")
					} else {
						logger.Info("publishing fragments updated",
							zap.Uints("active-fragments", activeFragIds),
							zap.Uints("stale-fragments", staleFragIds))

						// write staled fragment's data to active fragment
						if len(staleFragIds) > 0 {
							transferCh := p.transferStaledRecords(ctx, topicName, staleFragIds)
							inStreams = helper.MergeChannels(inStreams, transferCh)
						}

						p.currentFragMappings = fragMappingInfo
						getFragmentsToWrite = TopicWritingRule(topicInfo.Options(), activeFragIds)
					}
				}
			}
		}
	}()

	return errCh, nil
}

func (p *Publisher) transferStaledRecords(ctx context.Context, topicName string, fragmentIds []uint) chan TopicData {
	staleCh := make(chan TopicData)
	logger.Info("start transferring staled records to active fragments", zap.Uints("fragmentIds", fragmentIds))
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(staleCh)

		for _, staledFragId := range fragmentIds {
			loaded, ok := p.lastFetchedOffsets.Load(storage.NewFragmentKey(topicName, staledFragId))
			if !ok {
				logger.Error("cannot load last fetched offset of staled fragment", zap.Uint("fragmentId", staledFragId))
				continue
			}
			startStaledOffset := loaded.(uint64) + 1
			loaded, ok = p.currentPublishOffsets.Load(storage.NewFragmentKey(topicName, staledFragId))
			if !ok {
				logger.Error("cannot load publish offset of staled fragment", zap.Uint("fragmentId", staledFragId))
				continue
			}
			lastStaledOffset := loaded.(uint64)
			for i := startStaledOffset; i < lastStaledOffset; i++ {
				record, err := p.db.GetRecord(topicName, uint32(staledFragId), i)
				if err != nil {
					logger.Error(err.Error())
					continue
				}
				recordValue := storage.NewRecordValue(record)
				staled := TopicData{
					SeqNum: recordValue.SeqNum(),
					Data:   recordValue.PublishedData(),
				}
				select {
				case <-ctx.Done():
					return
				case staleCh <- staled:
				}
			}
		}
	}()

	return staleCh
}
func (p *Publisher) setupGrpcServer(ctx context.Context) error {
	if p.server == nil {
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		pb.RegisterPubSubServer(grpcServer, p)

		lis, err := net.Listen("tcp", p.address)
		logger.Debug("grpc server listening", zap.String("address", p.address))
		if err != nil {
			return err
		}

		p.server = grpcServer
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
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

func (p *Publisher) onReceiveData(data TopicData, topicName string, fragmentId uint, offset uint64, retentionPeriodSec uint64) error {
	expirationDate := storage.GetNowTimestamp() + retentionPeriodSec
	logger.Debug("write to", zap.String("topic", topicName), zap.Uint("fragmentId", fragmentId), zap.Uint64("offset", offset))
	return p.db.PutRecord(topicName, uint32(fragmentId), offset, data.SeqNum, data.Data, expirationDate)
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

	logger.Info("received new subscription stream", zap.String("topic", subscription.TopicName))

	for _, offsetInfo := range subscription.Offsets {
		var startOffset uint64
		// when start offset is not set, set current offset as last offset
		if offsetInfo.StartOffset == nil {
			value, _ := p.currentPublishOffsets.Load(storage.NewFragmentKey(subscription.TopicName, uint(offsetInfo.FragmentId)))
			currentOffset := value.(uint64)
			startOffset = currentOffset
		} else {
			startOffset = *offsetInfo.StartOffset
		}
		if startOffset == 0 {
			logger.Warn("start offset should be greater than 0. adjust start offset to 1")
			startOffset = 1
		}
		go p.onFetchData(ctx, subscription.TopicName, offsetInfo.FragmentId, startOffset, sendBuf)
	}

	p.wg.Add(1)
	defer p.wg.Done()
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

	p.wg.Add(1)
	defer p.wg.Done()

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
	it := p.db.Scan(storage.RecordCF)
	defer it.Close()

	logger.Debug("start subscription goroutine",
		zap.String("topic", topicName),
		zap.Uint32("fragmentId", fragmentId),
		zap.Uint64("startOffset", startOffset))

	fragIdUint := uint(fragmentId)
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
					p.lastFetchedOffsets.Store(storage.NewFragmentKey(topicName, fragIdUint), currentOffset)
					currentOffset++
					prevKey.SetOffset(currentOffset)
				}
				iterateCount++
				runtime.Gosched()
			}

			if iterateCount-rescanCheckPoint > rescanThreshold {
				rescanCheckPoint = iterateCount
				it.Close()
				it = p.db.Scan(storage.RecordCF)
			}
		}
		timer.Reset(waitInterval)
	}
}

func (p *Publisher) Wait() {
	p.wg.Wait()
}

// helper functions
func (p *Publisher) findPublishingFragments(fragMappings topic.FragMappingInfo) (activeFragments, staleFragments []uint) {
	for fragId, fragInfo := range fragMappings {
		if fragInfo.PublisherId == p.id {
			if fragInfo.State == topic.Active {
				activeFragments = append(activeFragments, fragId)
			} else if fragInfo.State == topic.Stale {
				staleFragments = append(staleFragments, fragId)
			}
		}
	}

	return
}

func (p *Publisher) isMappingUpdated(new topic.FragMappingInfo) bool {
	for fragId, fragInfo := range new {
		if fragInfo.PublisherId == p.id {
			if oldFragInfo, ok := p.currentFragMappings[fragId]; !ok { // when new fragment created
				return true
			} else if fragInfo.State != oldFragInfo.State { // when previous fragment's state is changed
				return true
			}
		}
	}
	return false
}
