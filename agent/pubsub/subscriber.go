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
	"google.golang.org/grpc"
)

type SubscriptionResult struct {
	FragmentId uint
	SeqNum     uint64
	Data       []byte
}

type SubscriptionAddrs map[string][]uint
type Subscriber struct {
	SubscriberID        string
	Bootstrapper        *bootstrapping.BootstrapService
	LastFragmentOffsets storage.TopicFragmentOffsets // last subscribed offsets
}

func (s *Subscriber) PrepareSubscription(ctx context.Context, topicName string, batchSize, flushInterval uint32) (chan []SubscriptionResult, chan error, error) {

	endpointMap, err := s.findSubscriptionEndpoints(topicName)
	if err != nil {
		return nil, nil, err
	}

	if len(endpointMap) == 0 {
		return nil, nil, qerror.TargetNotExistError{Target: fmt.Sprintf("publishers of topic '%s'", topicName)}
	}

	var outStreams []chan []SubscriptionResult
	var errStreams []chan error

	for endpoint, fragmentIds := range endpointMap {
		outStream, errStream, err := s.startSubscription(ctx, endpoint, topicName, fragmentIds, batchSize, flushInterval)
		if err != nil {
			return nil, nil, err
		}
		outStreams = append(outStreams, outStream)
		errStreams = append(errStreams, errStream)
	}

	return helper.MergeChannels(outStreams...), helper.MergeChannels(errStreams...), nil
}

// start subscription with subscribe RPC
func (s *Subscriber) startSubscription(ctx context.Context, endpoint string, topicName string, fragmentIds []uint,
	batchSize, flushInterval uint32) (outCh chan []SubscriptionResult, errCh chan error, err error) {
	opts := grpc.WithInsecure()
	conn, err := grpc.Dial(endpoint, opts)
	if err != nil {
		return
	}

	// load last offsets
	value, _ := s.LastFragmentOffsets.LoadOrStore(topicName, make(map[uint]uint64))
	lastOffsets := value.(map[uint]uint64)
	var subscriptionOffsets []*pb.Subscription_FragmentOffset
	for _, fragmentId := range fragmentIds {
		if _, ok := lastOffsets[fragmentId]; ok {
			lastOffsets[fragmentId] = 0
		}

		startOffset := lastOffsets[fragmentId] + 1
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
		return
	}

	outCh = make(chan []SubscriptionResult)
	errCh = make(chan error)

	go func() {
		defer conn.Close()
		defer close(errCh)
		defer close(outCh)

		for {
			select {
			case <-ctx.Done():
				logger.Info("stop subscribe from ctx.Done()")
				return
			default:
				subscriptionResult, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}
				s.onSubscribe(outCh, subscriptionResult.Results, topicName, lastOffsets)
			}
		}
	}()

	return
}

// find target agent and fragment info from topic-fragment discovery
func (s *Subscriber) findSubscriptionEndpoints(topicName string) (endpoints SubscriptionAddrs, err error) {
	topicSubscriptionFrame, err := s.Bootstrapper.GetTopicSubscriptions(topicName)
	if err != nil {
		return
	}

	fragmentsIds := topicSubscriptionFrame.SubscriptionInfo()[s.SubscriberID]
	if len(fragmentsIds) == 0 {
		return
	}

	topicFragmentFrame, err := s.Bootstrapper.GetTopicFragments(topicName)
	if err != nil {
		return
	}
	fragMappings := topicFragmentFrame.FragMappingInfo()
	for _, fragmentId := range fragmentsIds {
		if fragInfo, ok := fragMappings[fragmentId]; ok && fragInfo.Active {
			if _, ok := endpoints[fragInfo.Address]; ok {
				endpoints[fragInfo.Address] = append(endpoints[fragInfo.Address], fragmentId)
			} else {
				endpoints[fragInfo.Address] = []uint{fragmentId}
			}
		}
	}

	return
}

func (s *Subscriber) onSubscribe(outStream chan []SubscriptionResult, fetchedResults []*pb.SubscriptionResult_Fetched,
	topicName string, lastOffset map[uint]uint64) {
	logger.Debug("received",
		zap.Int("num data", len(fetchedResults)),
		zap.Uint64("last seqNum", fetchedResults[len(fetchedResults)-1].SeqNum))

	var results []SubscriptionResult
	for _, result := range fetchedResults {
		results = append(results, SubscriptionResult{
			FragmentId: uint(result.FragmentId),
			SeqNum:     result.SeqNum,
			Data:       result.Data,
		})
		lastOffset[uint(result.FragmentId)] = result.Offset
		s.LastFragmentOffsets.Store(topicName, lastOffset)
	}
	outStream <- results
}
