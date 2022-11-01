package agent_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent"
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/qerror"
	"github.com/paust-team/shapleq/test"
)

var _ = Describe("Agent", func() {

	Context("PubSub", Ordered, func() {
		Describe("Subscribing published data", func() {
			tp := test.NewTestParams()
			var coordClient coordinating.CoordClient
			var topicClient topic.CoordClientTopicWrapper
			var publisher *agent.Instance
			var subscriber *agent.Instance

			BeforeAll(func() {
				coordClient = helper.BuildCoordClient(config.NewAgentConfig())
				topicClient = topic.NewCoordClientTopicWrapper(coordClient)
			})
			AfterAll(func() {
				coordClient.Close()
			})
			BeforeEach(func() {
				tp.Set("topicOption", topic.UniquePerFragment)
				tp.Set("topic", "test_ps_topic")
				tp.Set("fragmentId", uint32(2))
				tp.Set("batchSize", uint32(1))
				tp.Set("flushInterval", uint32(0))

				// prepare topic
				err := topicClient.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", tp.Get("topicOption").(topic.Option)))
				Expect(err).NotTo(HaveOccurred())

				// prepare publisher
				pubConfig := config.NewAgentConfig()
				pubConfig.SetPort(11010)
				pubConfig.SetDBName("test-pub-store")
				pubConfig.SetBindAddress("0.0.0.0")
				publisher = agent.NewInstance(pubConfig)
				err = publisher.Start()
				Expect(err).NotTo(HaveOccurred())

				// prepare subscriber
				subConfig := config.NewAgentConfig()
				subConfig.SetPort(11011)
				subConfig.SetDBName("test-sub-store")
				subscriber = agent.NewInstance(subConfig)

				err = subscriber.Start()
				Expect(err).NotTo(HaveOccurred())
			})
			AfterEach(func() {
				tp.Clear()
				publisher.CleanAllData()
				publisher.Stop()
				subscriber.CleanAllData()
				subscriber.Stop()
				topicClient.DeleteTopic(tp.GetString("topic"))
			})

			When("nothing published to the topic", func() {
				It("cannot start to subscribe", func() {
					_, err := subscriber.StartSubscribe(tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).To(BeAssignableToTypeOf(qerror.TargetNotExistError{}))
				})
			})

			When("few records published to the topic", func() {
				var sendCh chan pubsub.TopicData

				BeforeEach(func() {
					tp.Set("records", [][]byte{
						{'g', 'o', 'o', 'g', 'l', 'e'},
						{'p', 'a', 'u', 's', 't', 'q'},
						{'1', '2', '3', '4', '5', '6'},
					})
					tp.Set("startSeqNum", uint64(1000))

					// setup topic fragment
					fragmentInfo := topic.FragMappingInfo{uint(tp.GetUint32("fragmentId")): topic.FragInfo{
						Active:      true,
						PublisherId: publisher.GetPublisherID(),
						Address:     "127.0.0.1:11010",
					}}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := topicClient.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					// setup subscription
					subscriptionInfo := topic.SubscriptionInfo{subscriber.GetSubscriberID(): []uint{uint(tp.GetUint32("fragmentId"))}}
					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err = topicClient.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
					Expect(err).NotTo(HaveOccurred())

					// publish
					sendCh = make(chan pubsub.TopicData)
					err = publisher.StartPublish(tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())

					for i, record := range tp.GetBytesList("records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i) + tp.GetUint64("startSeqNum"),
							Data:   record,
						}
					}
				})
				AfterEach(func() {
					close(sendCh)
				})

				It("can subscribe all published records", func() {
					recvCh, err := subscriber.StartSubscribe(tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).NotTo(HaveOccurred())

					idx := 0
					totalRecords := len(tp.GetBytesList("records"))
					for subscriptionResult := range recvCh {
						Expect(subscriptionResult).To(HaveLen(1))
						Expect(subscriptionResult[0].SeqNum).To(Equal(tp.GetUint64("startSeqNum") + uint64(idx)))
						Expect(subscriptionResult[0].Data).To(Equal(tp.GetBytesList("records")[idx]))
						idx++
						if idx == totalRecords {
							break
						}
					}
				})
			})
		})
	})
})
