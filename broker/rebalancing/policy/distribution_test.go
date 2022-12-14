package policy

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/zk"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/test"
)

var _ = Describe("Distribution", func() {
	var coordClient coordinating.CoordClient
	var bootstrapper *bootstrapping.BootstrapService
	var ruleExecutor FlushableExecutor

	Context("Distribution rule exectuor", Ordered, func() {
		tp := test.NewTestParams()

		BeforeAll(func() {
			coordClient = zk.NewZKCoordClient([]string{"127.0.0.1:2181"}, 5000)
			err := coordClient.Connect()
			Expect(err).NotTo(HaveOccurred())
			err = path.CreatePathsIfNotExist(coordClient)
			Expect(err).NotTo(HaveOccurred())
			bootstrapper = bootstrapping.NewBootStrapService(coordClient)
			ruleExecutor = NewDistributionPolicyExecutor(bootstrapper)

			tp.Set("topic", "test-rule")
			err = bootstrapper.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", topic.UniquePerFragment))
			Expect(err).NotTo(HaveOccurred())
		})
		AfterAll(func() {
			bootstrapper.DeleteTopic(tp.GetString("topic"))
			coordClient.Close()
		})
		BeforeEach(func() {
			tp.Set("topic", "test-rule")
			tp.Set("publisher-id", "test-publisher-1")
			tp.Set("publisher-addr", "192.168.0.1:11010")
			tp.Set("subscriber-id", "test-subscriber-1")

		})
		AfterEach(func() {
			tp.Clear()
		})

		Context("Adding a Publisher", Ordered, func() {
			Describe("the publisher is brand new", func() {
				BeforeAll(func() {
					err := bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber-id"))
					Expect(err).NotTo(HaveOccurred())
				})
				When("on publisher added", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnPublisherAdded(
							tp.GetString("publisher-id"),
							tp.GetString("topic"),
							tp.GetString("publisher-addr"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())

						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						Expect(topicFragmentFrame.FragMappingInfo()).To(HaveLen(1))
						for fragmentId := range topicFragmentFrame.FragMappingInfo() {
							tp.Set("fragment", fragmentId)
						}
					})
					It("has same fragment info with active state", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())

						fragmentInfo := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Active))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))
						Expect(fragmentInfo.Address).To(Equal(tp.GetString("publisher-addr")))
					})
					It("only has active fragments in subscriptions", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(subscriptionInfo).NotTo(BeNil())
						Expect(subscriptionInfo).To(HaveLen(1))
						Expect(subscriptionInfo[0]).To(Equal(tp.GetUint("fragment")))
					})
				})
			})

			Describe("publishers and subscribers are already exist for the topic", Ordered, func() {
				BeforeAll(func() {
					tp.Set("fragment1", uint(10))
					tp.Set("fragment1-other", uint(11))
					tp.Set("fragment2", uint(20))
					tp.Set("fragment2-other", uint(21))
					tp.Set("publisher1", "test-pub-1")
					tp.Set("publisher2", "test-pub-2")
					tp.Set("subscriber1", "test-sub-1")
					tp.Set("subscriber2", "test-sub-2")

					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher1"),
						Address:     "127.0.0.1:11011",
					}
					fragmentInfo[tp.GetUint("fragment1-other")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher2"),
						Address:     "127.0.0.1:11011",
					}
					fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher1"),
						Address:     "127.0.0.1:11011",
					}
					fragmentInfo[tp.GetUint("fragment2-other")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher2"),
						Address:     "127.0.0.1:11011",
					}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					subscriptionInfo := make(topic.SubscriptionInfo)
					err = bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber1"))
					Expect(err).NotTo(HaveOccurred())
					err = bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber2"))
					Expect(err).NotTo(HaveOccurred())
					subscriptionInfo[tp.GetString("subscriber1")] = []uint{tp.GetUint("fragment1"), tp.GetUint("fragment2")}
					subscriptionInfo[tp.GetString("subscriber2")] = []uint{tp.GetUint("fragment1-other"), tp.GetUint("fragment2-other")}
					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
					Expect(err).NotTo(HaveOccurred())
				})
				When("on publisher added", Ordered, func() {
					var topicFragMappings topic.FragMappingInfo
					var subscriptions topic.SubscriptionInfo
					numPublishers := 3

					BeforeAll(func() {
						err := ruleExecutor.OnPublisherAdded(
							tp.GetString("publisher-id"),
							tp.GetString("topic"),
							tp.GetString("publisher-addr"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())

						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						topicFragMappings = topicFragmentFrame.FragMappingInfo()
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						subscriptions = topicSubscriptionFrame.SubscriptionInfo()
					})
					It("has equal number of new fragments with number of subscribers", func() {
						numNewFragments := len(topicFragMappings) - 4
						numSubscribers := len(subscriptions)
						Expect(numNewFragments).To(Equal(numSubscribers))
					})
					It("has distributed fragments over subscribers", func() {
						Expect(subscriptions[tp.GetString("subscriber1")]).To(HaveLen(numPublishers))
						Expect(subscriptions[tp.GetString("subscriber2")]).To(HaveLen(numPublishers))

						hasSameElement := helper.HasSameElement(
							subscriptions[tp.GetString("subscriber1")],
							subscriptions[tp.GetString("subscriber2")])
						Expect(hasSameElement).To(BeFalse())
					})
				})
			})
		})

		Context("Removing a Publisher", func() {
			Describe("only one subscriber exists", func() {
				Describe("other publishers exist (3 publishers)", Ordered, func() {
					numPublishers := 3
					BeforeAll(func() {
						tp.Set("fragment1", uint(10))
						tp.Set("fragment2", uint(20))
						tp.Set("fragment3", uint(30))
						tp.Set("publisher-other-1", "test-pub-ot1")
						tp.Set("publisher-other-2", "test-pub-ot2")

						fragmentInfo := make(topic.FragMappingInfo)
						fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
							State:       topic.Active,
							PublisherId: tp.GetString("publisher-id"),
							Address:     tp.GetString("publisher-addr"),
						}
						fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
							State:       topic.Active,
							PublisherId: tp.GetString("publisher-other-1"),
							Address:     tp.GetString("publisher-addr"),
						}
						fragmentInfo[tp.GetUint("fragment3")] = topic.FragInfo{
							State:       topic.Active,
							PublisherId: tp.GetString("publisher-other-2"),
							Address:     tp.GetString("publisher-addr"),
						}
						topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
						err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
						Expect(err).NotTo(HaveOccurred())

						subscriptionInfo := make(topic.SubscriptionInfo)
						subscriptionInfo[tp.GetString("subscriber-id")] = []uint{tp.GetUint("fragment1"), tp.GetUint("fragment2"), tp.GetUint("fragment3")}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					})
					When("on publisher removed", func() {
						BeforeAll(func() {
							err := ruleExecutor.OnPublisherRemoved(
								tp.GetString("publisher-id"),
								tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())
							err = ruleExecutor.Flush()
							Expect(err).NotTo(HaveOccurred())
						})
						It("only the fragments of the removed publisher should be excluded", func() {
							topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())
							subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
							Expect(subscriptionInfo).To(HaveLen(numPublishers - 1))
							Expect(helper.IsContains(tp.GetUint("fragment1"), subscriptionInfo)).To(BeFalse())
						})
						It("only the fragments of the removed publisher should be inactive state", func() {
							topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())

							fragmentInfo := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment1")]
							Expect(fragmentInfo).NotTo(BeNil())
							Expect(fragmentInfo.State).To(Equal(topic.Inactive))
							Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))

							fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment2")]
							Expect(fragmentInfo).NotTo(BeNil())
							Expect(fragmentInfo.State).To(Equal(topic.Active))
							Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-other-1")))

							fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment3")]
							Expect(fragmentInfo).NotTo(BeNil())
							Expect(fragmentInfo.State).To(Equal(topic.Active))
							Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-other-2")))
						})
					})
				})
				Describe("other publisher does not exist", func() {
					BeforeAll(func() {
						tp.Set("fragment1", uint(10))
						fragmentInfo := make(topic.FragMappingInfo)
						fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
							State:       topic.Active,
							PublisherId: tp.GetString("publisher-id"),
							Address:     tp.GetString("publisher-addr"),
						}

						topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
						err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
						Expect(err).NotTo(HaveOccurred())

						subscriptionInfo := make(topic.SubscriptionInfo)
						subscriptionInfo[tp.GetString("subscriber-id")] = []uint{tp.GetUint("fragment1")}
						topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
						err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
						Expect(err).NotTo(HaveOccurred())
					})
					When("on publisher removed", Ordered, func() {
						BeforeAll(func() {
							err := ruleExecutor.OnPublisherRemoved(
								tp.GetString("publisher-id"),
								tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())
							err = ruleExecutor.Flush()
							Expect(err).NotTo(HaveOccurred())
						})
						It("has same fragment info with inactive state", func() {
							topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())

							fragmentInfo := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment1")]
							Expect(fragmentInfo).NotTo(BeNil())
							Expect(fragmentInfo.State).To(Equal(topic.Inactive))
							Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))
						})
						It("doesn't have any fragment in subscriptions", func() {
							topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
							Expect(err).NotTo(HaveOccurred())
							subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
							Expect(subscriptionInfo).To(HaveLen(0))
						})
					})
				})
			})
			Describe("many subscribers exist (2 publishers, 3 subscribers)", Ordered, func() {
				var fragmentInfo topic.FragMappingInfo
				numPublishers := 2
				BeforeAll(func() {
					fragmentInfo = make(topic.FragMappingInfo)

					// for publisher1
					tp.Set("fragment1-for-pub1", uint(10))
					tp.Set("fragment2-for-pub1", uint(20))
					tp.Set("fragment3-for-pub1", uint(30))
					fragmentInfo[tp.GetUint("fragment1-for-pub1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment2-for-pub1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment3-for-pub1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}

					// for publisher2
					tp.Set("publisher-id2", "test-pub-2")

					tp.Set("fragment1-for-pub2", uint(40))
					tp.Set("fragment2-for-pub2", uint(50))
					tp.Set("fragment3-for-pub2", uint(60))
					fragmentInfo[tp.GetUint("fragment1-for-pub2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id2"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment2-for-pub2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id2"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment3-for-pub2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id2"),
						Address:     tp.GetString("publisher-addr"),
					}

					// setup subscribers
					tp.Set("subscriber-id2", "test-sub-2")
					tp.Set("subscriber-id3", "test-sub-3")
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					subscriptionInfo := make(topic.SubscriptionInfo)
					subscriptionInfo[tp.GetString("subscriber-id")] = []uint{tp.GetUint("fragment1-for-pub1"), tp.GetUint("fragment1-for-pub2")}
					subscriptionInfo[tp.GetString("subscriber-id2")] = []uint{tp.GetUint("fragment2-for-pub1"), tp.GetUint("fragment2-for-pub2")}
					subscriptionInfo[tp.GetString("subscriber-id3")] = []uint{tp.GetUint("fragment3-for-pub1"), tp.GetUint("fragment3-for-pub2")}

					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
					Expect(err).NotTo(HaveOccurred())
				})
				When("on publisher removed", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnPublisherRemoved(
							tp.GetString("publisher-id"),
							tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())
					})
					It("only the fragments of the removed publisher should be excluded", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())

						subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(subscriptionInfo).To(HaveLen(numPublishers - 1))
						Expect(helper.IsContains(tp.GetUint("fragment1-for-pub1"), subscriptionInfo)).To(BeFalse())

						subscriptionInfo = topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id2")]
						Expect(subscriptionInfo).To(HaveLen(numPublishers - 1))
						Expect(helper.IsContains(tp.GetUint("fragment2-for-pub1"), subscriptionInfo)).To(BeFalse())

						subscriptionInfo = topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id3")]
						Expect(subscriptionInfo).To(HaveLen(numPublishers - 1))
						Expect(helper.IsContains(tp.GetUint("fragment3-for-pub1"), subscriptionInfo)).To(BeFalse())
					})
					It("only the fragments of the removed publisher should be inactive state", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())

						fragmentInfo := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment1-for-pub1")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Inactive))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))

						fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment2-for-pub1")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Inactive))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))

						fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment3-for-pub1")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Inactive))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))

						fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment1-for-pub2")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Active))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id2")))

						fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment2-for-pub2")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Active))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id2")))

						fragmentInfo = topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment3-for-pub2")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Active))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id2")))
					})
				})
			})
		})

		Context("Adding a Subscriber", func() {
			Describe("only one publisher exists", Ordered, func() {
				prevNumFragments := 1
				numPublishers := 1
				BeforeAll(func() {
					tp.Set("fragment-prev", uint(10))
					tp.Set("subscriber-id-prev", "test-subs-2")
					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment-prev")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     "127.0.0.1:11011",
					}

					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					subscriptionInfo := make(topic.SubscriptionInfo)
					subscriptionInfo[tp.GetString("subscriber-id-prev")] = []uint{tp.GetUint("fragment-prev")}
					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
					Expect(err).NotTo(HaveOccurred())
				})
				When("on subscriber added", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnSubscriberAdded(
							tp.GetString("subscriber-id"),
							tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())
					})
					It("has one more active fragment in fragment info", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						Expect(topicFragmentFrame.FragMappingInfo()).To(HaveLen(prevNumFragments + 1))
					})
					It("has different fragments with previous subscriber", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						prevSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id-prev")]
						curSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(prevSubscriptionInfo).To(HaveLen(numPublishers))
						Expect(curSubscriptionInfo).To(HaveLen(numPublishers))

						for i := 0; i < numPublishers; i++ {
							Expect(helper.IsContains(curSubscriptionInfo[i], prevSubscriptionInfo)).To(BeFalse())
						}
					})
				})
			})

			Describe("many publishers exist (3 publishers)", Ordered, func() {
				numPrevSubscribers := 1
				numPublishers := 3
				BeforeAll(func() {
					tp.Set("fragment-prev1", uint(10))
					tp.Set("fragment-prev2", uint(20))
					tp.Set("fragment-prev3", uint(30))
					tp.Set("subscriber-id-prev", "test-subs-2")
					tp.Set("publisher-id2", "test-pubs-2")
					tp.Set("publisher-id3", "test-pubs-3")

					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment-prev1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     "127.0.0.1:11011",
					}
					fragmentInfo[tp.GetUint("fragment-prev2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id2"),
						Address:     "127.0.0.1:11011",
					}
					fragmentInfo[tp.GetUint("fragment-prev3")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id3"),
						Address:     "127.0.0.1:11011",
					}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					subscriptionInfo := make(topic.SubscriptionInfo)
					subscriptionInfo[tp.GetString("subscriber-id-prev")] = []uint{tp.GetUint("fragment-prev1"), tp.GetUint("fragment-prev2"), tp.GetUint("fragment-prev3")}
					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
					Expect(err).NotTo(HaveOccurred())
				})
				When("on subscriber added", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnSubscriberAdded(
							tp.GetString("subscriber-id"),
							tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())
					})
					It("has (num_publishers) more active fragment in fragment info", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						Expect(topicFragmentFrame.FragMappingInfo()).To(HaveLen(numPrevSubscribers*numPublishers + numPublishers))
					})
					It("has different fragments with previous subscriber", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						prevSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id-prev")]
						curSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(prevSubscriptionInfo).To(HaveLen(numPublishers))
						Expect(curSubscriptionInfo).To(HaveLen(numPublishers))

						for i := 0; i < numPublishers; i++ {
							Expect(helper.IsContains(curSubscriptionInfo[i], prevSubscriptionInfo)).To(BeFalse())
						}
					})
				})
			})
		})

		Context("Removing a Subscriber", Ordered, func() {
			numPublishers := 2
			BeforeAll(func() {
				tp.Set("fragment1", uint(10))
				tp.Set("fragment2", uint(20))
				tp.Set("fragment-other1", uint(30))
				tp.Set("fragment-other2", uint(40))
				tp.Set("subscriber-id2", "test-subs-2")
				tp.Set("publisher-id2", "test-pubs-2")

				fragmentInfo := make(topic.FragMappingInfo)
				fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id"),
					Address:     "127.0.0.1:11011",
				}
				fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id"),
					Address:     "127.0.0.1:11011",
				}
				fragmentInfo[tp.GetUint("fragment-other1")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id2"),
					Address:     "127.0.0.1:11011",
				}
				fragmentInfo[tp.GetUint("fragment-other2")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id2"),
					Address:     "127.0.0.1:11011",
				}
				topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
				err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
				Expect(err).NotTo(HaveOccurred())

				subscriptionInfo := make(topic.SubscriptionInfo)
				subscriptionInfo[tp.GetString("subscriber-id")] = []uint{tp.GetUint("fragment1"), tp.GetUint("fragment-other1")}
				subscriptionInfo[tp.GetString("subscriber-id2")] = []uint{tp.GetUint("fragment2"), tp.GetUint("fragment-other2")}
				topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
				err = bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
				Expect(err).NotTo(HaveOccurred())
			})

			When("on subscriber removed", Ordered, func() {
				BeforeAll(func() {
					err := ruleExecutor.OnSubscriberRemoved(
						tp.GetString("subscriber-id"),
						tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					err = ruleExecutor.Flush()
					Expect(err).NotTo(HaveOccurred())
				})
				It("only removed subscriber1's subscription", func() {
					topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
					Expect(subscriptionInfo).To(HaveLen(0))

					subscriptionInfo = topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id2")]
					Expect(subscriptionInfo).To(HaveLen(numPublishers))
				})
				It("only the fragments of the removed subscription should be stale state", func() {
					topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					fragmentMappings := topicFragmentFrame.FragMappingInfo()
					Expect(fragmentMappings[tp.GetUint("fragment1")].State).To(Equal(topic.Stale))
					Expect(fragmentMappings[tp.GetUint("fragment2")].State).To(Equal(topic.Active))
					Expect(fragmentMappings[tp.GetUint("fragment-other1")].State).To(Equal(topic.Stale))
					Expect(fragmentMappings[tp.GetUint("fragment-other2")].State).To(Equal(topic.Active))
				})
			})
		})
	})
})
