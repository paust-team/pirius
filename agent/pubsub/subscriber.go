package pubsub

import (
	"context"
	"github.com/paust-team/shapleq/agent/logger"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type SubscriptionResult struct {
	FragmentId uint32
	SeqNum     uint64
	Data       []byte
}

type Subscriber struct {
	SubscriberID string
	Bootstrapper *bootstrapping.BootstrapService
}

func (s Subscriber) RegisterSubscription(ctx context.Context, topicName string, batchSize, flushInterval uint32) (outStream chan []SubscriptionResult, errCh chan error, err error) {
	// TODO:: find target agent and fragment info from topic-fragment discovery
	//topicSubscriptionFrame, err := s.Bootstrapper.GetTopicSubscriptions(topicName)

	targetAddr := "127.0.0.1:10010"
	var startOffset uint64 = 1
	var fragmentId uint32 = 1

	var conn *grpc.ClientConn
	var stream pb.PubSub_SubscribeClient

	opts := grpc.WithInsecure()
	conn, err = grpc.Dial(targetAddr, opts)
	if err != nil {
		return
	}

	// start gRPC stream
	publisher := pb.NewPubSubClient(conn)

	stream, err = publisher.Subscribe(ctx, &pb.Subscription{
		Magic:         1,
		TopicName:     topicName,
		Offsets:       []*pb.Subscription_FragmentOffset{{FragmentId: fragmentId, StartOffset: startOffset}},
		MaxBatchSize:  batchSize,
		FlushInterval: flushInterval,
	})
	if err != nil {
		return
	}

	outStream = make(chan []SubscriptionResult)
	errCh = make(chan error)

	go func() {
		defer close(outStream)
		defer close(errCh)
		defer conn.Close()

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
				s.onSubscribe(outStream, subscriptionResult.Results)
			}
		}
	}()
	return
}

func (s Subscriber) onSubscribe(outStream chan []SubscriptionResult, fetchedResults []*pb.SubscriptionResult_Fetched) {
	logger.Debug("received",
		zap.Int("num data", len(fetchedResults)),
		zap.Uint64("last seqNum", fetchedResults[len(fetchedResults)-1].SeqNum))

	var results []SubscriptionResult
	for _, result := range fetchedResults {
		results = append(results, SubscriptionResult{
			FragmentId: result.FragmentId,
			SeqNum:     result.SeqNum,
			Data:       result.Data,
		})
	}
	outStream <- results
}
