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
	"runtime"
	"sync"
	"time"
	"unsafe"
)

type publisherBase struct {
	id                    string
	address               string
	db                    *storage.QRocksDB
	bootstrapper          *bootstrapping.BootstrapService
	currentPublishOffsets storage.TopicFragmentOffsets // current write offsets
	lastFetchedOffsets    storage.TopicFragmentOffsets // last read offsets
	currentFragMappings   topic.FragMappingInfo
}

func (p publisherBase) prepare(ctx context.Context, topicName string) (chan topic.FragMappingInfo, topic.FragMappingInfo, topic.Option, error) {
	var fragMappings topic.FragMappingInfo
	fragmentWatchCh, err := p.bootstrapper.WatchFragmentInfoChanged(ctx, topicName)
	if err != nil {
		return nil, nil, 0, err
	}
	logger.Info("watcher for fragments registered", zap.String("publisher-id", p.id), zap.String("topic", topicName))

	// register publisher path and wait for initial rebalance
	err = p.bootstrapper.AddPublisher(topicName, p.id, p.address)
	if _, ok := err.(qerror.CoordTargetAlreadyExistsError); ok { // if already registered, check fragment info
		fragmentFrame, err := p.bootstrapper.GetTopicFragments(topicName)
		if err != nil {
			return nil, nil, 0, err
		}
		initialFragment := fragmentFrame.FragMappingInfo()
		fragMappings = initialFragment
	} else if err != nil {
		return nil, nil, 0, err
	} else { // wait watch event for initial fragment assignment
		timer := time.After(time.Second * constants.InitialRebalanceTimeout)
		for fragMappings == nil {
			select {
			case initialFragment := <-fragmentWatchCh:
				if activeFragments, _ := p.findPublishingFragments(initialFragment); len(activeFragments) == 0 {
					continue
				}
				fragMappings = initialFragment
			case <-timer:
				return nil, nil, 0, qerror.InvalidStateError{State: fmt.Sprintf("initial rebalance timed out for topic(%s)", topicName)}
			}
		}
	}

	// load topic policy and set fragments selecting rule
	topicInfo, err := p.bootstrapper.GetTopic(topicName)
	if err != nil {
		return nil, nil, 0, err
	}

	return fragmentWatchCh, fragMappings, topicInfo.Options(), nil
}

func (p publisherBase) transferStaledRecords(ctx context.Context, wg *sync.WaitGroup, topicName string, fragmentIds []uint) chan TopicData {
	staleCh := make(chan TopicData)
	logger.Info("start transferring staled records to active fragments",
		zap.String("publisher-id", p.id),
		zap.String("topic", topicName),
		zap.Uints("fragmentIds", fragmentIds))
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(staleCh)

		for _, staledFragId := range fragmentIds {

			var lastStaledOffset uint64
			if loaded, ok := p.currentPublishOffsets.Load(storage.NewFragmentKey(topicName, staledFragId)); !ok {
				logger.Info("skip: no record found for staled fragment.", zap.String("publisher-id", p.id), zap.Uint("fragmentId", staledFragId))
				continue
			} else {
				lastStaledOffset = loaded.(uint64)
			}

			var startStaledOffset uint64
			if loaded, ok := p.lastFetchedOffsets.Load(storage.NewFragmentKey(topicName, staledFragId)); !ok {
				logger.Info("cannot load last fetched offset of staled fragment", zap.String("publisher-id", p.id), zap.Uint("fragmentId", staledFragId))
				startStaledOffset = 1
			} else {
				startStaledOffset = loaded.(uint64) + 1
			}

			for i := startStaledOffset; i < lastStaledOffset; i++ {
				record, err := p.db.GetRecord(topicName, uint32(staledFragId), i)
				if err != nil {
					logger.Error(err.Error(), zap.String("publisher-id", p.id))
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
				default:
					staleCh <- staled
					logger.Debug("write to stale ch",
						zap.String("publisher-id", p.id),
						zap.String("topic", topicName),
						zap.Uint("staledFragmentId", staledFragId), zap.Uint64("offset", i))
				}
			}
		}
	}()

	return staleCh
}

func (p publisherBase) onReceiveData(data TopicData, topicName string, fragmentId uint, offset uint64, retentionPeriodSec uint64) error {
	expirationDate := storage.GetNowTimestamp() + retentionPeriodSec
	logger.Debug("write to", zap.String("publisher-id", p.id), zap.String("topic", topicName), zap.Uint("fragmentId", fragmentId), zap.Uint64("offset", offset))
	return p.db.PutRecord(topicName, uint32(fragmentId), offset, data.SeqNum, data.Data, expirationDate)
}

func (p publisherBase) setupTopicWriter(ctx context.Context, wg *sync.WaitGroup, topicName string, topicOption topic.Option, fragMappings topic.FragMappingInfo) (func() []uint, chan TopicData, error) {
	// setup  publishing fragments
	var transferCh chan TopicData
	activeFragIds, staleFragIds := p.findPublishingFragments(fragMappings)
	if len(activeFragIds) == 0 {
		return nil, nil, qerror.InvalidStateError{State: fmt.Sprintf("no active fragments of topic(%s)", topicName)}
	}

	logger.Info("setup publishing fragments",
		zap.String("publisher-id", p.id),
		zap.Uints("active-fragments", activeFragIds),
		zap.Uints("stale-fragments", staleFragIds))

	getFragmentsToWrite := TopicWritingRule(topicOption, activeFragIds)
	if len(staleFragIds) > 0 {
		transferCh = p.transferStaledRecords(ctx, wg, topicName, staleFragIds)
	}

	return getFragmentsToWrite, transferCh, nil
}

func (p publisherBase) onFetchData(ctx context.Context, wg *sync.WaitGroup, topicName string, fragmentId uint32, startOffset uint64, outStream chan *pb.SubscriptionResult_Fetched) {

	prefix := make([]byte, len(topicName)+1+int(unsafe.Sizeof(uint32(0))))
	copy(prefix, topicName+"@")
	binary.BigEndian.PutUint32(prefix[len(topicName)+1:], fragmentId)
	waitInterval := time.Millisecond * 10
	timer := time.NewTimer(waitInterval)

	currentOffset := startOffset
	prevKey := storage.NewRecordKeyFromData(topicName, fragmentId, currentOffset)

	iterateCount := 0
	rescanCheckPoint := 0
	rescanThreshold := 1000
	it := p.db.Scan(storage.RecordCF)

	logger.Debug("start subscription goroutine",
		zap.String("topic", topicName),
		zap.Uint32("fragmentId", fragmentId),
		zap.Uint64("startOffset", startOffset))

	wg.Add(1)
	fragIdUint := uint(fragmentId)

	go func() {
		defer wg.Done()
		defer timer.Stop()
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
					default:
						outStream <- topicData
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
	}()
}

// helper functions
func (p publisherBase) findPublishingFragments(fragMappings topic.FragMappingInfo) (activeFragments, staleFragments []uint) {
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

func (p publisherBase) isMappingUpdated(new topic.FragMappingInfo) bool {
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

type TopicData struct {
	SeqNum uint64
	Data   []byte
}

type Publisher struct {
	pb.PubSubServer
	publisherBase
	wg sync.WaitGroup
}

func NewPublisher(id string, address string, db *storage.QRocksDB, bootstrapper *bootstrapping.BootstrapService,
	publishedOffsets, fetchedOffsets storage.TopicFragmentOffsets) Publisher {
	return Publisher{
		publisherBase: publisherBase{
			id:                    id,
			address:               address,
			db:                    db,
			bootstrapper:          bootstrapper,
			currentPublishOffsets: publishedOffsets,
			lastFetchedOffsets:    fetchedOffsets,
		},
		wg: sync.WaitGroup{},
	}
}

func (p *Publisher) StartTopicPublication(ctx context.Context, topicName string, retentionPeriodSec uint64,
	inStream chan TopicData) (chan error, error) {

	// register watcher for topic fragment info
	watcherCtx, cancel := context.WithCancel(ctx)
	fragmentWatchCh, fragMappings, topicOption, err := p.prepare(watcherCtx, topicName)
	if err != nil {
		cancel()
		return nil, err
	}
	getFragmentsToWrite, transferCh, err := p.setupTopicWriter(ctx, &p.wg, topicName, topicOption, fragMappings)
	if err != nil {
		cancel()
		return nil, err
	}

	var inStreams chan TopicData
	if transferCh != nil {
		// write staled fragment's data to active fragment
		inStreams = helper.MergeChannels(inStream, transferCh)
	} else {
		inStreams = inStream
	}

	errCh := make(chan error, 2)
	p.wg.Add(1)
	go func() {
		defer close(errCh)
		defer p.wg.Done()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				logger.Info("stop publishing: ctx.Done()", zap.String("publisher-id", p.id))
				return
			case data, ok := <-inStreams:
				if !ok {
					logger.Info("stop publishing: send buffer closed", zap.String("publisher-id", p.id))
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
					logger.Error("stop publishing: watch closed", zap.String("publisher-id", p.id))
					errCh <- qerror.InvalidStateError{State: "watcher channel closed unexpectedly"}
					return
				}
				logger.Info("received new fragment mapping info", zap.String("topic", topicName))

				if p.isMappingUpdated(fragMappingInfo) {
					// reset publishing fragments
					logger.Info("resetting publishing fragments", zap.String("publisher-id", p.id))
					writeFn, staleCh, err := p.setupTopicWriter(ctx, &p.wg, topicName, topicOption, fragMappings)
					if err != nil {
						logger.Error("failed to reset publishing fragments", zap.String("publisher-id", p.id))
						errCh <- err
					} else {
						if staleCh != nil {
							inStreams = helper.MergeChannels(inStreams, staleCh)
						}
						getFragmentsToWrite = writeFn
					}
					p.currentFragMappings = fragMappingInfo
				}
			}
		}
	}()

	return errCh, nil
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
			zap.String("publisher-id", p.id),
			zap.Int("num data", len(batched)),
			zap.Uint64("last seqNum", batched[len(batched)-1].SeqNum))

		timer.Reset(flushIntervalMs)
		batched = nil
		dontWait = false
		return nil
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	logger.Info("received new subscription stream", zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))

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
			logger.Warn("start offset should be greater than 0. adjust start offset to 1", zap.String("publisher-id", p.id))
			startOffset = 1
		}
		p.onFetchData(ctx, &p.wg, subscription.TopicName, offsetInfo.FragmentId, startOffset, sendBuf)
	}

	p.wg.Add(1)
	defer p.wg.Done()
	for {
		select {
		case <-stream.Context().Done():
			logger.Debug("stream closed from client", zap.String("publisher-id", p.id))
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

func (p *Publisher) Wait() {
	p.wg.Wait()
}
