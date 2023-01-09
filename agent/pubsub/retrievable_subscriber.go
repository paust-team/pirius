package pubsub

import (
	"context"
	"fmt"
	"github.com/paust-team/pirius/agent/storage"
	"github.com/paust-team/pirius/bootstrapping"
	"github.com/paust-team/pirius/constants"
	"github.com/paust-team/pirius/logger"
	"github.com/paust-team/pirius/proto/pb"
	"github.com/paust-team/pirius/qerror"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"runtime"
	"sync"
	"time"
)

type RetrievableSubscriptionResults struct {
	Results  []SubscriptionResult
	SendBack func([]SubscriptionResult)
}

type RetrievableSubscriber struct {
	subscriberBase
	wg sync.WaitGroup
}

func NewRetrievableSubscriber(id string, bootstrapper *bootstrapping.BootstrapService, subscribedOffsets storage.TopicFragmentOffsets) RetrievableSubscriber {
	return RetrievableSubscriber{
		subscriberBase: subscriberBase{
			id:                   id,
			bootstrapper:         bootstrapper,
			lastSubscribedOffset: subscribedOffsets,
		},
		wg: sync.WaitGroup{},
	}
}

func (s *RetrievableSubscriber) StartTopicSubscription(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan RetrievableSubscriptionResults, chan error, error) {

	// register watcher for subscription info
	watcherCtx, cancel := context.WithCancel(ctx)
	subscriptionWatchCh, subscriptions, err := s.prepare(watcherCtx, topicName)
	if err != nil {
		cancel()
		return nil, nil, err
	}

	subscriptionCtx, subscriptionCtxCancel := context.WithCancel(ctx)
	subscriptionWg := sync.WaitGroup{}
	subscriptionCh, sErrCh, err := s.startSubscriptions(subscriptionCtx, &subscriptionWg, topicName, subscriptions, batchSize, flushInterval)
	if err != nil {
		cancel()
		subscriptionCtxCancel()
		return nil, nil, err
	}
	s.currentSubscriptions = subscriptions
	outStream := make(chan RetrievableSubscriptionResults)
	errStream := make(chan error, 2)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		defer subscriptionCtxCancel()
		for {
			select {
			case <-ctx.Done():
				logger.Info("stop subscribing: ctx.Done()", zap.String("subscriber-id", s.id))
				return
			case result, ok := <-subscriptionCh:
				if !ok {
					logger.Info("stop subscribing: receive buffer closed", zap.String("subscriber-id", s.id))
					return
				}
				select {
				case <-ctx.Done():
					return
				case outStream <- result:
				}
			case err = <-sErrCh:
				if err != nil {
					errStream <- err
				}
			case subscriptionInfo, ok := <-subscriptionWatchCh:
				if !ok {
					logger.Error("stop subscribing: watch closed", zap.String("subscriber-id", s.id))
					errStream <- qerror.InvalidStateError{State: "watcher channel closed unexpectedly"}
					return
				}
				logger.Info("received new subscription info", zap.String("subscriber-id", s.id), zap.String("topic", topicName))
				if s.isSubscriptionUpdated(subscriptionInfo) {
					logger.Info("resetting subscriptions",
						zap.String("subscriber-id", s.id),
						zap.Uints("old-fragments", s.currentSubscriptions),
						zap.Uints("new-fragments", subscriptionInfo[s.id]))
					subscriptionCtxCancel()
					subscriptionWg.Wait()
					subscriptionCtx, subscriptionCtxCancel = context.WithCancel(ctx)
					subscriptions = subscriptionInfo[s.id]
					if len(subscriptions) == 0 { // wait for new subscription
						logger.Info("received empty subscriptions. wait for new subscription",
							zap.String("subscriber-id", s.id),
							zap.Uints("old-fragments", s.currentSubscriptions))
					} else {
						subscriptionCh, sErrCh, err = s.startSubscriptions(subscriptionCtx, &subscriptionWg, topicName, subscriptions, batchSize, flushInterval)
						if err != nil {
							errStream <- err
							return
						}
						logger.Info("succeed to reset subscriptions",
							zap.String("subscriber-id", s.id),
							zap.Uints("old-fragments", s.currentSubscriptions),
							zap.Uints("new-fragments", subscriptionInfo[s.id]))
					}
					s.currentSubscriptions = subscriptions
				} else {
					logger.Info("skip: not newly subscriptions",
						zap.String("subscriber-id", s.id),
						zap.Uints("current-fragments", s.currentSubscriptions),
						zap.Uints("received-fragments", subscriptionInfo[s.id]))
				}
			}
		}
	}()

	return outStream, errStream, nil
}

func (s *RetrievableSubscriber) startSubscriptions(ctx context.Context, subscriptionWg *sync.WaitGroup, topicName string, subscriptionFragments []uint,
	batchSize, flushInterval uint32) (chan RetrievableSubscriptionResults, chan error, error) {

	logger.Info("setup subscription streams", zap.String("subscriber-id", s.id), zap.String("topic", topicName), zap.Uints("fragmentIds", subscriptionFragments))
	endpointMap, err := s.findSubscriptionEndpoints(topicName, subscriptionFragments)
	if err != nil {
		return nil, nil, err
	}

	if len(endpointMap) == 0 {
		return nil, nil, qerror.TargetNotExistError{Target: fmt.Sprintf("publishers of topic '%s', fragments %v", topicName, s.currentSubscriptions)}
	}

	outStream := make(chan RetrievableSubscriptionResults)
	errStream := make(chan error)

	// create subscription stream for each endpoint
	wg := sync.WaitGroup{}
	for endpoint, fragmentIds := range endpointMap {
		opts := grpc.WithInsecure()
		conn, err := grpc.Dial(endpoint, opts)
		if err != nil {
			return nil, nil, err
		}

		// load last offsets
		var subscriptionOffsets []*pb.Subscription_FragmentOffset
		for _, fragmentId := range fragmentIds {
			value, _ := s.lastSubscribedOffset.LoadOrStore(storage.NewFragmentKey(topicName, fragmentId), uint64(0))
			lastFetchedOffset := value.(uint64)

			startOffset := lastFetchedOffset + 1
			subscriptionOffsets = append(subscriptionOffsets, &pb.Subscription_FragmentOffset{
				FragmentId:  uint32(fragmentId),
				StartOffset: &startOffset,
			})
		}

		// start bidirectional subscribe stream
		publisher := pb.NewRetrievablePubSubClient(conn)
		stream, err := publisher.RetrievableSubscribe(ctx)
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		err = stream.Send(&pb.RetrievableSubscription{
			Magic: 1,
			Type: &pb.RetrievableSubscription_Subscription{Subscription: &pb.Subscription{
				Magic:         1,
				TopicName:     topicName,
				Offsets:       subscriptionOffsets,
				MaxBatchSize:  batchSize,
				FlushInterval: flushInterval,
			}},
		})
		if err != nil {
			stream.CloseSend()
			conn.Close()
			return nil, nil, err
		}

		onSendBack := func(res []SubscriptionResult) {
			var batched []*pb.SubscriptionResult_Fetched
			for _, res := range res {
				batched = append(batched, &pb.SubscriptionResult_Fetched{
					FragmentId: uint32(res.FragmentId),
					SeqNum:     res.SeqNum,
					Data:       res.Data,
				})
			}

			err := stream.Send(&pb.RetrievableSubscription{
				Magic: 1,
				Type: &pb.RetrievableSubscription_Result{Result: &pb.SubscriptionResult{
					Magic:   1,
					Results: batched,
				}},
			})
			if err != nil {
				logger.Error("cannot send back to bi-subscribe stream",
					zap.Error(err),
					zap.String("subscriber-id", s.id),
					zap.String("topic", topicName))
			}
		}

		wg.Add(1)
		go func(pubEndpoint string) {
			defer wg.Done()
			defer conn.Close()
			defer stream.CloseSend()
			retryCount := 0
			for {
				select {
				case <-ctx.Done():
					logger.Info("stop subscribe from ctx.Done()",
						zap.String("subscriber-id", s.id),
						zap.String("topic", topicName),
						zap.String("publisher-endpoint", pubEndpoint))
					return
				default:
					subscriptionResult, err := stream.Recv()
					if err != nil {
						if err == io.EOF {
							// TODO :: this is abnormal case. should be restarted?
							logger.Info("stop subscribe from io.EOF",
								zap.String("subscriber-id", s.id),
								zap.String("topic", topicName),
								zap.String("publisher-endpoint", pubEndpoint))
						} else if status.Code(err) == codes.Canceled { // client closing (subscriber context canceled)
							logger.Info("stop subscribe from inner context canceled",
								zap.String("subscriber-id", s.id),
								zap.String("topic", topicName),
								zap.String("publisher-endpoint", pubEndpoint))
						} else if status.Code(err) == codes.Unavailable { // server closing (publisher context canceled)
							logger.Info("stop subscribe from publisher closed",
								zap.String("subscriber-id", s.id),
								zap.String("topic", topicName),
								zap.String("publisher-endpoint", pubEndpoint))
						} else {
							select {
							case <-ctx.Done():
								return
							default:
								logger.Warn("failed to receive from stream. retrying..", zap.Error(err), zap.Int("retry-count", retryCount))
								retryCount++
								if retryCount >= constants.MaxRetryCountForSubscription {
									errStream <- err
								} else {
									time.Sleep(1 * time.Second)
									continue
								}
							}
						}
						return
					}
					retryCount = 0
					fetchedResults := subscriptionResult.Results
					logger.Debug("received",
						zap.String("subscriber-id", s.id),
						zap.String("topic", topicName),
						zap.String("publisher-endpoint", pubEndpoint),
						zap.Int("num data", len(fetchedResults)),
						zap.Uint64("last seqNum", fetchedResults[len(fetchedResults)-1].SeqNum))

					var results []SubscriptionResult

					for _, result := range fetchedResults {
						results = append(results, SubscriptionResult{
							FragmentId: uint(result.FragmentId),
							SeqNum:     result.SeqNum,
							Data:       result.Data,
						})
						s.lastSubscribedOffset.Store(storage.NewFragmentKey(topicName, uint(result.FragmentId)), result.Offset)
					}
					select {
					case <-ctx.Done():
						logger.Info("stop subscribe from ctx.Done()",
							zap.String("subscriber-id", s.id),
							zap.String("topic", topicName),
							zap.String("publisher-endpoint", pubEndpoint))
						return
					default:
						outStream <- RetrievableSubscriptionResults{Results: results, SendBack: onSendBack}
					}
					runtime.Gosched()
				}
			}
		}(endpoint)
	}

	// wait for all subscription to be finished
	subscriptionWg.Add(1)
	go func() {
		defer subscriptionWg.Done()
		defer close(errStream)
		defer close(outStream)
		wg.Wait()
		logger.Info("all subscription streams closed", zap.String("subscriber-id", s.id), zap.String("topic", topicName), zap.Uints("fragmentIds", subscriptionFragments))
	}()

	return outStream, errStream, nil
}

func (s *RetrievableSubscriber) Wait() {
	s.wg.Wait()
}
