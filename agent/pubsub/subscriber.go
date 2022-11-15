package pubsub

import (
	"context"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"runtime"
	"sync"
	"time"
)

type SubscriptionResult struct {
	FragmentId uint
	SeqNum     uint64
	Data       []byte
}

type SubscriptionAddrs map[string][]uint
type Subscriber struct {
	id                   string
	bootstrapper         *bootstrapping.BootstrapService
	lastSubscribedOffset storage.TopicFragmentOffsets // last fetched offsets
	wg                   sync.WaitGroup
	currentSubscriptions []uint
}

func NewSubscriber(id string, bootstrapper *bootstrapping.BootstrapService, subscribedOffsets storage.TopicFragmentOffsets) Subscriber {
	return Subscriber{
		id:                   id,
		bootstrapper:         bootstrapper,
		lastSubscribedOffset: subscribedOffsets,
		wg:                   sync.WaitGroup{},
	}
}

func (s *Subscriber) Wait() {
	s.wg.Wait()
}

func (s *Subscriber) PrepareSubscription(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan []SubscriptionResult, chan error, error) {

	// register watcher for subscription info
	subscriptionWatchCh, err := s.bootstrapper.WatchSubscriptionChanged(ctx, topicName)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("watcher for subscriptions registered")

	// register subscriber path and wait for initial rebalance
	if err = s.bootstrapper.AddSubscriber(topicName, s.id); err != nil {
		return nil, nil, err
	}
	var subscriptions []uint
	select {
	case initialSubscriptions := <-subscriptionWatchCh:
		if _, ok := initialSubscriptions[s.id]; !ok {
			return nil, nil, qerror.InvalidStateError{State: fmt.Sprintf("fragment not exists for subscriber(%s)", s.id)}
		} else if len(initialSubscriptions[s.id]) == 0 {
			return nil, nil, qerror.InvalidStateError{State: fmt.Sprintf("fragment not exists for subscriber(%s)", s.id)}
		}
		subscriptions = initialSubscriptions[s.id]
	case <-time.After(time.Second * constants.InitialRebalanceTimeout):
		return nil, nil, qerror.InvalidStateError{State: fmt.Sprintf("initial rebalance timed out for topic(%s)", topicName)}
	}

	subscriptionCtx, cancel := context.WithCancel(ctx)
	subscriptionWg := sync.WaitGroup{}
	subscriptionCh, sErrCh, err := s.startSubscriptions(subscriptionCtx, &subscriptionWg, topicName, subscriptions, batchSize, flushInterval)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	s.currentSubscriptions = subscriptions
	outStream := make(chan []SubscriptionResult)
	errStream := make(chan error, 2)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
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
					cancel()
					subscriptionWg.Wait()
					subscriptionCtx, cancel = context.WithCancel(ctx)
					subscriptions = subscriptionInfo[s.id]
					subscriptionCh, sErrCh, err = s.startSubscriptions(subscriptionCtx, &subscriptionWg, topicName, subscriptions, batchSize, flushInterval)
					if err != nil {
						errStream <- err
						return
					}
					s.currentSubscriptions = subscriptions
					logger.Info("succeed to reset subscriptions",
						zap.String("subscriber-id", s.id),
						zap.Uints("old-fragments", s.currentSubscriptions),
						zap.Uints("new-fragments", subscriptionInfo[s.id]))
				}
			}
		}
	}()

	return outStream, errStream, nil
}

func (s *Subscriber) startSubscriptions(ctx context.Context, subscriptionWg *sync.WaitGroup, topicName string, subscriptionFragments []uint,
	batchSize, flushInterval uint32) (chan []SubscriptionResult, chan error, error) {

	logger.Info("setup subscription streams", zap.String("subscriber-id", s.id), zap.String("topic", topicName), zap.Uints("fragmentIds", subscriptionFragments))
	endpointMap, err := s.findSubscriptionEndpoints(topicName, subscriptionFragments)
	if err != nil {
		return nil, nil, err
	}

	if len(endpointMap) == 0 {
		return nil, nil, qerror.TargetNotExistError{Target: fmt.Sprintf("publishers of topic '%s', fragments %v", topicName, s.currentSubscriptions)}
	}

	outStream := make(chan []SubscriptionResult)
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

		// start gRPC stream
		publisher := pb.NewPubSubClient(conn)
		stream, err := publisher.Subscribe(ctx, &pb.Subscription{
			Magic:         1,
			TopicName:     topicName,
			Offsets:       subscriptionOffsets,
			MaxBatchSize:  batchSize,
			FlushInterval: flushInterval,
		})
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		wg.Add(1)
		go func(pubEndpoint string) {
			defer wg.Done()
			defer conn.Close()

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
							errStream <- err
						}
						return
					}
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
					case outStream <- results:
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
		wg.Wait()
		logger.Info("all subscription streams closed", zap.String("subscriber-id", s.id), zap.String("topic", topicName), zap.Uints("fragmentIds", subscriptionFragments))
	}()

	return outStream, errStream, nil

}

// start subscription with subscribe RPC

// helper functions
func (s *Subscriber) findSubscriptionEndpoints(topicName string, fragmentIds []uint) (SubscriptionAddrs, error) {
	topicFragmentFrame, err := s.bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return nil, err
	}
	fragMappings := topicFragmentFrame.FragMappingInfo()
	endpoints := make(SubscriptionAddrs)
	for _, fragmentId := range fragmentIds {
		if fragInfo, ok := fragMappings[fragmentId]; ok && fragInfo.State == topic.Active {
			if _, ok := endpoints[fragInfo.Address]; ok {
				endpoints[fragInfo.Address] = append(endpoints[fragInfo.Address], fragmentId)
			} else {
				endpoints[fragInfo.Address] = []uint{fragmentId}
			}
		}
	}

	return endpoints, nil
}

func (s *Subscriber) isSubscriptionUpdated(new topic.SubscriptionInfo) bool {
	newSubscription, ok := new[s.id]
	if !ok {
		return true
	}
	if len(s.currentSubscriptions) != len(newSubscription) ||
		!helper.HasAllElements(newSubscription, s.currentSubscriptions) {
		return true
	}

	return false
}
