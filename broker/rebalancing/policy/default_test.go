package policy_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/broker/rebalancing/policy"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/zk"
	"github.com/paust-team/shapleq/test"
)

var _ = Describe("Default", func() {
	var coordClient coordinating.CoordClient
	var bootstrapper *bootstrapping.BootstrapService
	var ruleExecutor policy.FlushableExecutor

	Context("Default rule exectuor", Ordered, func() {
		tp := test.NewTestParams()

		BeforeAll(func() {
			coordClient = zk.NewZKCoordClient([]string{"127.0.0.1:2181"}, 5000)
			err := coordClient.Connect()
			Expect(err).NotTo(HaveOccurred())
			err = path.CreatePathsIfNotExist(coordClient)
			Expect(err).NotTo(HaveOccurred())
			bootstrapper = bootstrapping.NewBootStrapService(coordClient)
			ruleExecutor = policy.NewDefaultPolicyExecutor(bootstrapper)

			tp.Set("topic", "test-rule")
			err = bootstrapper.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", 0))
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

		Context("Adding a Publisher", func() {
			Describe("the publisher had published before", Ordered, func() {
				BeforeAll(func() {
					tp.Set("fragment", uint(10))
					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment")] = topic.FragInfo{
						State:       topic.Inactive,
						PublisherId: tp.GetString("publisher-id"),
						Address:     "",
					}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
					Expect(err).NotTo(HaveOccurred())

					err = bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber-id"))
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
					})
					It("has same fragment mappings with active state", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())

						fragmentInfo := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment")]
						Expect(fragmentInfo).NotTo(BeNil())
						Expect(fragmentInfo.State).To(Equal(topic.Active))
						Expect(fragmentInfo.PublisherId).To(Equal(tp.GetString("publisher-id")))
						Expect(fragmentInfo.Address).To(Equal(tp.GetString("publisher-addr")))
					})
					It("only has active fragment in subscriptions", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(subscriptionInfo).NotTo(BeNil())
						Expect(subscriptionInfo).To(HaveLen(1))
						Expect(subscriptionInfo[0]).To(Equal(tp.GetUint("fragment")))
					})
				})
			})

			Describe("the publisher is brand new", Ordered, func() {
				BeforeAll(func() {
					subscriptionInfo := make(topic.SubscriptionInfo)
					subscriptionInfo[tp.GetString("subscriber-id")] = []uint{} // blank subscription
					topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
					err := bootstrapper.UpdateTopicSubscriptions(tp.GetString("topic"), topicSubscriptionFrame)
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
		})

		Context("Removing a Publisher", func() {
			Describe("the publisher have published to single fragment", Ordered, func() {
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

			Describe("the publisher have published to multiple fragments", Ordered, func() {
				BeforeAll(func() {
					tp.Set("fragment1", uint(10))
					tp.Set("fragment2", uint(20))
					tp.Set("publisher2-id", "test-publisher2-id")
					tp.Set("publisher2-addr", "127.0.0.1:11011")
					tp.Set("fragment3", uint(30))

					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment3")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher2-id"),
						Address:     tp.GetString("publisher2-addr"),
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
				When("on publisher removed", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnPublisherRemoved(
							tp.GetString("publisher-id"),
							tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())
					})
					It("only the fragments of the removed publisher should be inactivated", func() {
						topicFragmentFrame, err := bootstrapper.GetTopicFragments(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())

						fragment1Info := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment1")]
						Expect(fragment1Info).NotTo(BeNil())
						Expect(fragment1Info.State).To(Equal(topic.Inactive))
						Expect(fragment1Info.PublisherId).To(Equal(tp.GetString("publisher-id")))

						fragment2Info := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment2")]
						Expect(fragment2Info).NotTo(BeNil())
						Expect(fragment2Info.State).To(Equal(topic.Inactive))
						Expect(fragment2Info.PublisherId).To(Equal(tp.GetString("publisher-id")))

						fragment3Info := topicFragmentFrame.FragMappingInfo()[tp.GetUint("fragment3")]
						Expect(fragment3Info).NotTo(BeNil())
						Expect(fragment3Info.State).To(Equal(topic.Active))
						Expect(fragment3Info.PublisherId).To(Equal(tp.GetString("publisher2-id")))
					})
					It("only has active fragments in subscriptions", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(subscriptionInfo).To(HaveLen(1))
						Expect(subscriptionInfo[0]).To(Equal(tp.GetUint("fragment3")))
					})
				})
			})
		})

		Context("Adding a Subscriber", func() {
			Describe("previous subscriber doesn't exists", Ordered, func() {
				BeforeAll(func() {
					tp.Set("fragment1", uint(10))
					tp.Set("fragment2", uint(20))
					tp.Set("publisher2-id", "test-publisher2-id")
					tp.Set("publisher2-addr", "127.0.0.1:11011")
					tp.Set("fragment3", uint(30))
					tp.Set("cur-subscriber-id", "test-cur-subscriber-id")

					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
						State:       topic.Inactive,
						PublisherId: tp.GetString("publisher2-id"),
						Address:     tp.GetString("publisher2-addr"),
					}
					fragmentInfo[tp.GetUint("fragment3")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
					err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
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

					It("only has active fragments in subscriptions", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						Expect(subscriptionInfo).To(HaveLen(2))

						if subscriptionInfo[0] < subscriptionInfo[1] {
							Expect(subscriptionInfo[0]).To(Equal(tp.GetUint("fragment1")))
							Expect(subscriptionInfo[1]).To(Equal(tp.GetUint("fragment3")))
						} else {
							Expect(subscriptionInfo[1]).To(Equal(tp.GetUint("fragment1")))
							Expect(subscriptionInfo[0]).To(Equal(tp.GetUint("fragment3")))
						}
					})
				})
			})
			Describe("previous subscriber exists", Ordered, func() {
				BeforeAll(func() {
					tp.Set("fragment1", uint(10))
					tp.Set("fragment2", uint(20))
					tp.Set("publisher2-id", "test-publisher2-id")
					tp.Set("publisher2-addr", "127.0.0.1:11011")
					tp.Set("fragment3", uint(30))
					tp.Set("cur-subscriber-id", "test-cur-subscriber-id")

					fragmentInfo := make(topic.FragMappingInfo)
					fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher-id"),
						Address:     tp.GetString("publisher-addr"),
					}
					fragmentInfo[tp.GetUint("fragment3")] = topic.FragInfo{
						State:       topic.Active,
						PublisherId: tp.GetString("publisher2-id"),
						Address:     tp.GetString("publisher2-addr"),
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
				When("on subscriber added", Ordered, func() {
					BeforeAll(func() {
						err := ruleExecutor.OnSubscriberAdded(
							tp.GetString("cur-subscriber-id"),
							tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						err = ruleExecutor.Flush()
						Expect(err).NotTo(HaveOccurred())
					})

					It("has same fragments with previous subscriber", func() {
						topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
						Expect(err).NotTo(HaveOccurred())
						prevSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
						curSubscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("cur-subscriber-id")]
						Expect(len(prevSubscriptionInfo)).To(Equal(len(curSubscriptionInfo)))
					})
				})
			})
		})

		Context("Removing a Subscriber", Ordered, func() {
			BeforeAll(func() {
				tp.Set("fragment1", uint(10))
				tp.Set("fragment2", uint(20))

				fragmentInfo := make(topic.FragMappingInfo)
				fragmentInfo[tp.GetUint("fragment1")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id"),
					Address:     tp.GetString("publisher-addr"),
				}
				fragmentInfo[tp.GetUint("fragment2")] = topic.FragInfo{
					State:       topic.Active,
					PublisherId: tp.GetString("publisher-id"),
					Address:     tp.GetString("publisher-addr"),
				}

				topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
				err := bootstrapper.UpdateTopicFragments(tp.GetString("topic"), topicFragmentFrame)
				Expect(err).NotTo(HaveOccurred())

				subscriptionInfo := make(topic.SubscriptionInfo)
				subscriptionInfo[tp.GetString("subscriber-id")] = []uint{tp.GetUint("fragment1"), tp.GetUint("fragment2")}
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
				It("has empty fragments in removed subscription", func() {
					topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id")]
					Expect(subscriptionInfo).To(HaveLen(0))
				})
			})
		})
	})
})
