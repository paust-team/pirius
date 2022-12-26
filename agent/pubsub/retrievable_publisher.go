package pubsub

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/logger"
	"github.com/paust-team/shapleq/proto/pb"
	"github.com/paust-team/shapleq/qerror"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"time"
)

type topicContext struct {
	ctx        context.Context
	cancel     context.CancelFunc
	retrieveCh chan []TopicDataResult
	topicWg    *sync.WaitGroup
}

type TopicDataResult struct {
	FragmentId uint32
	SeqNum     uint64
	Data       []byte
}

type RetrievablePublisher struct {
	pb.RetrievablePubSubServer
	publisherBase
	topicContexts sync.Map
	wg            sync.WaitGroup
}

func NewRetrievablePublisher(id string, address string, db *storage.QRocksDB, bootstrapper *bootstrapping.BootstrapService,
	publishedOffsets, fetchedOffsets storage.TopicFragmentOffsets) RetrievablePublisher {
	return RetrievablePublisher{
		publisherBase: publisherBase{
			id:                    id,
			address:               address,
			db:                    db,
			bootstrapper:          bootstrapper,
			currentPublishOffsets: publishedOffsets,
			lastFetchedOffsets:    fetchedOffsets,
		},
		wg:            sync.WaitGroup{},
		topicContexts: sync.Map{},
	}
}

func (p *RetrievablePublisher) StartTopicPublication(ctx context.Context, topicName string, retentionPeriodSec uint64,
	inStream chan TopicData) (chan []TopicDataResult, chan error, error) {

	// register watcher for topic fragment info
	pubCtx, cancel := context.WithCancel(ctx)
	fragmentWatchCh, fragMappings, topicOption, err := p.prepare(pubCtx, topicName)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	getFragmentsToWrite, transferCh, err := p.setupTopicWriter(ctx, &p.wg, topicName, topicOption, fragMappings)
	if err != nil {
		cancel()
		return nil, nil, err
	}

	var inStreams chan TopicData
	if transferCh != nil {
		// write staled fragment's data to active fragment
		inStreams = helper.MergeChannels(inStream, transferCh)
	} else {
		inStreams = inStream
	}

	if _, ok := p.topicContexts.Load(topicName); ok {
		cancel()
		return nil, nil, qerror.InvalidStateError{State: fmt.Sprintf("stream already exists for topic(%s)", topicName)}
	}

	retrieveCh := make(chan []TopicDataResult)
	topicWg := sync.WaitGroup{}
	p.topicContexts.Store(topicName, &topicContext{
		ctx:        pubCtx,
		cancel:     cancel,
		retrieveCh: retrieveCh,
		topicWg:    &topicWg,
	})
	errCh := make(chan error, 2)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(errCh)
		defer func() { // cancel topic context
			cancel()
			topicWg.Wait()
			close(retrieveCh)
			p.topicContexts.Delete(topicName)
		}()
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
			case fragMappings, ok := <-fragmentWatchCh:
				if !ok {
					logger.Error("stop publishing: watch closed", zap.String("publisher-id", p.id))
					errCh <- qerror.InvalidStateError{State: "watcher channel closed unexpectedly"}
					return
				}
				logger.Info("received new fragment mapping info", zap.String("topic", topicName))

				if p.isMappingUpdated(fragMappings) {
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
					p.currentFragMappings = fragMappings
				}
			}
		}
	}()

	return retrieveCh, errCh, nil
}

// gRPC implementation

func (p *RetrievablePublisher) RetrievableSubscribe(stream pb.RetrievablePubSub_RetrievableSubscribeServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}

	// get subscription
	var subscription *pb.Subscription
	switch v := request.Type.(type) {
	case *pb.RetrievableSubscription_Subscription:
		subscription = v.Subscription
	default:
		return qerror.ValidationError{
			Value:   "SubscriptionResult",
			HintMsg: "Initial request should be a Subscription",
		}
	}

	// load topic context
	v, ok := p.topicContexts.Load(subscription.TopicName)
	if !ok {
		return qerror.InvalidStateError{
			State: fmt.Sprintf("context not initialized for topic(%s)", subscription.TopicName),
		}
	}
	topicCtx := v.(*topicContext)
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

	logger.Info("received new retrievable subscription stream", zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))

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

	// handle retrieved topic data
	topicCtx.topicWg.Add(1)
	go func() {
		defer topicCtx.topicWg.Done()
		defer logger.Info("finish receiving retrieve-stream", zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))
		for {
			request, err = stream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Info("stop receiving retrieve-stream: client stream closed",
						zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))
				} else if status.Code(err) == codes.Canceled { // client closing (subscriber context canceled)
					logger.Info("stop receiving retrieve-stream: inner context canceled",
						zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))
				} else {
					logger.Error("error occurred on receiving stream",
						zap.Error(err), zap.String("publisher-id", p.id), zap.String("topic", subscription.TopicName))
				}
				return
			}

			switch v := request.Type.(type) {
			case *pb.RetrievableSubscription_Result:
				var topicDataResults []TopicDataResult
				for _, res := range v.Result.Results {
					topicDataResults = append(topicDataResults, TopicDataResult{
						FragmentId: res.FragmentId,
						SeqNum:     res.SeqNum,
						Data:       res.Data,
					})
				}
				select {
				case <-topicCtx.ctx.Done():
					return
				default:
					topicCtx.retrieveCh <- topicDataResults
					logger.Info("sent to topic retrieve-stream",
						zap.String("topic", subscription.TopicName),
						zap.String("publisher-id", p.id))
				}

			default:
				logger.Error("invalid request of Bidirection stream")
			}
		}
	}()

	p.wg.Add(1)
	defer p.wg.Done()
	// flush to stream when batch is full
	for {
		select {
		case <-topicCtx.ctx.Done():
			logger.Debug("stream closed from topic-ctx.Done()", zap.String("topic", subscription.TopicName), zap.String("publisher-id", p.id))
			return nil
		case <-stream.Context().Done():
			logger.Debug("stream closed from client", zap.String("topic", subscription.TopicName), zap.String("publisher-id", p.id))
			return nil

		case fetched := <-sendBuf:
			batched = append(batched, fetched)
			if len(batched) >= maxBatchSize || (len(batched) > 0 && dontWait) {
				if err := flush(); err != nil {
					logger.Error("error occurred on flushing records", zap.Error(err))
				}
			}
		case <-timer.C:
			if len(batched) > 0 {
				if err := flush(); err != nil {
					logger.Error("error occurred on flushing records", zap.Error(err))
				}
			} else {
				// if flush time is over and no data collected,
				// then don't wait until flush interval
				dontWait = true
			}
		}
	}
}

func (p *RetrievablePublisher) Wait() {
	p.wg.Wait()
}
