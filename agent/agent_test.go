package agent_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent"
	"github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/agent/storage"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/qerror"
	"github.com/paust-team/shapleq/test"
	"time"
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
				agentConfig := config.NewAgentConfig()
				coordClient = helper.BuildCoordClient(agentConfig.ZKQuorum(), agentConfig.ZKTimeout())
				err := coordClient.Connect()
				Expect(err).NotTo(HaveOccurred())
				topicClient = topic.NewCoordClientTopicWrapper(coordClient)

				// prepare publisher
				pubConfig := config.NewAgentConfig()
				pubConfig.SetPort(11010)
				pubConfig.SetDataDir(constants.DefaultHomeDir + "/test-pub")
				pubConfig.SetBindAddress("0.0.0.0")
				publisher = agent.NewInstance(pubConfig)

				// prepare subscriber
				subConfig := config.NewAgentConfig()
				subConfig.SetPort(11011)
				subConfig.SetDataDir(constants.DefaultHomeDir + "/test-sub")
				subscriber = agent.NewInstance(subConfig)

				// prepare topic
				tp.Set("topic", "test_ps_topic")
				tp.Set("topicOption", topic.UniquePerFragment)
				err = topicClient.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", tp.Get("topicOption").(topic.Option)))
				Expect(err).NotTo(HaveOccurred())

			})
			AfterAll(func() {
				topicClient.DeleteTopic(tp.GetString("topic"))
				coordClient.Close()
			})
			BeforeEach(func() {
				tp.Set("topicOption", topic.UniquePerFragment)
				tp.Set("topic", "test_ps_topic")
				tp.Set("fragmentId", uint32(2))
				tp.Set("batchSize", uint32(1))
				tp.Set("flushInterval", uint32(0))
			})
			AfterEach(func() {
				tp.Clear()
				publisher.CleanAllData()
				subscriber.CleanAllData()
			})

			When("nothing published to the topic", Ordered, func() {
				BeforeAll(func() {
					err := publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					err = subscriber.Start()
					Expect(err).NotTo(HaveOccurred())
				})
				AfterAll(func() {
					publisher.Stop()
					subscriber.Stop()
				})
				It("cannot start to subscribe", func() {
					go func() {
						time.Sleep(1 * time.Second)
						// setup empty subscription
						subscriptionInfo := topic.SubscriptionInfo{subscriber.GetSubscriberID(): []uint{}}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err := topicClient.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					}()
					_, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).To(BeAssignableToTypeOf(qerror.InvalidStateError{}))
				})
			})

			When("few records published to the topic", Ordered, func() {
				var sendCh chan pubsub.TopicData

				BeforeAll(func() {
					err := publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					err = subscriber.Start()
					Expect(err).NotTo(HaveOccurred())

					tp.Set("records", [][]byte{
						{'g', 'o', 'o', 'g', 'l', 'e'},
						{'p', 'a', 'u', 's', 't', 'q'},
						{'1', '2', '3', '4', '5', '6'},
					})
					tp.Set("startSeqNum", uint64(1000))

					go func() {
						time.Sleep(1 * time.Second)
						// setup topic fragment
						fragmentInfo := topic.FragMappingInfo{uint(tp.GetUint32("fragmentId")): topic.FragInfo{
							State:       topic.Active,
							PublisherId: publisher.GetPublisherID(),
							Address:     "127.0.0.1:11010",
						}}
						topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
						err = topicClient.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
						Expect(err).NotTo(HaveOccurred())
					}()

					// publish
					sendCh = make(chan pubsub.TopicData)
					err = publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())

					for i, record := range tp.GetBytesList("records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i) + tp.GetUint64("startSeqNum"),
							Data:   record,
						}
					}
				})
				AfterAll(func() {
					close(sendCh)
					publisher.Stop()
					subscriber.Stop()

					// check whether offsets are stored properly
					loaded, err := storage.LoadAgentMeta(publisher.GetMetaPath())
					Expect(err).NotTo(HaveOccurred())
					loaded.PublishedOffsets.Range(func(k, v interface{}) bool {
						publishedOffset := v.(uint64)
						Expect(publishedOffset).To(Equal(uint64(len(tp.GetBytesList("records")) + 1)))
						return false
					})
					loaded.LastFetchedOffset.Range(func(k, v interface{}) bool {
						lastFetchedOffset := v.(uint64)
						Expect(lastFetchedOffset).To(Equal(uint64(len(tp.GetBytesList("records")))))
						return false
					})

					loaded, err = storage.LoadAgentMeta(subscriber.GetMetaPath())
					Expect(err).NotTo(HaveOccurred())
					loaded.SubscribedOffsets.Range(func(k, v interface{}) bool {
						subscribedOffset := v.(uint64)
						Expect(subscribedOffset).To(Equal(uint64(len(tp.GetBytesList("records")))))
						return false
					})
				})

				It("can subscribe all published records", func() {
					go func() {
						time.Sleep(1 * time.Second)
						// setup subscription
						subscriptionInfo := topic.SubscriptionInfo{subscriber.GetSubscriberID(): []uint{uint(tp.GetUint32("fragmentId"))}}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err := topicClient.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					}()

					recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
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

			When("few records published and staled fragment exists", Ordered, func() {
				var sendCh chan pubsub.TopicData

				BeforeAll(func() {
					err := publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					err = subscriber.Start()
					Expect(err).NotTo(HaveOccurred())

					tp.Set("old-records", [][]byte{
						{'g', 'o', 'o', 'g', 'l', 'e'},
						{'p', 'a', 'u', 's', 't', 'q'},
						{'1', '2', '3', '4', '5', '6'},
					})
					tp.Set("startSeqNumOld", uint64(2000))
					tp.Set("stale-records", [][]byte{
						{'n', 'a', 'v', 'e', 'r', 'e'},
						{'k', 'o', 'r', 'e', 'a', 'n'},
						{'e', 'n', 'g', 'l', 'i', 's'},
					})
					tp.Set("startSeqNumStale", uint64(3000))
					tp.Set("new-records", [][]byte{
						{'n', 'o', 'o', 'g', 'l', 'e'},
						{'n', 'a', 'u', 's', 't', 'q'},
						{'n', '2', '3', '4', '5', '6'},
					})
					tp.Set("newFragmentId", uint32(100))
					tp.Set("startSeqNumNew", uint64(4000))

					// publish old records
					fragmentInfo := topic.FragMappingInfo{
						uint(tp.GetUint32("fragmentId")): topic.FragInfo{
							State:       topic.Active,
							PublisherId: publisher.GetPublisherID(),
							Address:     "127.0.0.1:11010"},
					}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)

					go func() {
						time.Sleep(1 * time.Second)
						// setup old active fragment
						err = topicClient.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
						Expect(err).NotTo(HaveOccurred())
					}()
					sendCh = make(chan pubsub.TopicData)
					err = publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())

					for i, record := range tp.GetBytesList("old-records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i) + tp.GetUint64("startSeqNumOld"),
							Data:   record,
						}
					}

					// subscribe old records
					go func() {
						time.Sleep(1 * time.Second)
						// setup old subscription
						subscriptionInfo := topic.SubscriptionInfo{subscriber.GetSubscriberID(): []uint{uint(tp.GetUint32("fragmentId"))}}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err = topicClient.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					}()
					recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).NotTo(HaveOccurred())

					idx := 0
					totalRecords := len(tp.GetBytesList("old-records"))
					for range recvCh {
						idx++
						if idx == totalRecords {
							break
						}
					}
					subscriber.Stop()
					// publish stale records
					for i, record := range tp.GetBytesList("stale-records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i) + tp.GetUint64("startSeqNumStale"),
							Data:   record,
						}
					}
					publisher.Stop()

					// update fragment to stale and add new fragment
					fragmentInfo[uint(tp.GetUint32("fragmentId"))] = topic.FragInfo{
						State:       topic.Stale,
						PublisherId: publisher.GetPublisherID(),
						Address:     "127.0.0.1:11010",
					}
					fragmentInfo[uint(tp.GetUint32("newFragmentId"))] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: publisher.GetPublisherID(),
						Address:     "127.0.0.1:11010",
					}

					// restart agents
					err = publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					err = subscriber.Start()
					Expect(err).NotTo(HaveOccurred())

					// publish new records
					close(sendCh)
					sendCh = make(chan pubsub.TopicData)
					go func() {
						time.Sleep(1 * time.Second)
						topicFragmentFrame = topic.NewTopicFragmentsFrame(fragmentInfo)
						err = topicClient.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
						Expect(err).NotTo(HaveOccurred())
					}()
					err = publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())

					for i, record := range tp.GetBytesList("new-records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i) + tp.GetUint64("startSeqNumNew"),
							Data:   record,
						}
					}
				})

				It("can subscribe all staled and new records", func() {
					defer subscriber.Stop()
					defer publisher.Stop()
					defer close(sendCh)

					go func() {
						time.Sleep(1 * time.Second)
						// update subscription to new fragment
						subscriptionInfo := topic.SubscriptionInfo{subscriber.GetSubscriberID(): []uint{uint(tp.GetUint32("newFragmentId"))}}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err := topicClient.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					}()
					recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).NotTo(HaveOccurred())

					idx := 0
					totalRecords := append(tp.GetBytesList("stale-records"), tp.GetBytesList("new-records")...)
					for subscriptionResult := range recvCh {
						Expect(subscriptionResult).To(HaveLen(1))
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, totalRecords)).To(BeTrue())
						idx++
						if idx == len(totalRecords) {
							break
						}
					}
				})
			})
		})
	})
})
