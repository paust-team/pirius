package topic_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/inmemory"
	"github.com/paust-team/shapleq/qerror"
	"sort"
)

var _ = Describe("CoordClient", func() {
	var coordClient coordinating.CoordClient
	var topicClient topic.CoordClientTopicWrapper

	Context("Topic", Ordered, func() {
		var testTopic, testDescription string
		var testOptions topic.Option

		BeforeAll(func() {
			coordClient = inmemory.NewInMemCoordClient()
			topicClient = topic.NewCoordClientTopicWrapper(coordClient)
			testTopic = "test-topic"
			testDescription = "test-topic-desc"
			testOptions = topic.UniquePerFragment
		})
		AfterAll(func() {
			coordClient.Close()
		})
		BeforeEach(func() {
			err := topicClient.CreateTopic(testTopic, topic.NewTopicFrame(testDescription, testOptions))
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			topicClient.DeleteTopic(testTopic)
		})

		Describe("Fetching a topic", func() {
			var err error

			When("the topic exists", func() {
				var topicFrame topic.Frame
				BeforeEach(func() {
					topicFrame, err = topicClient.GetTopic(testTopic)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should be equal to expected", func() {
					Expect(topicFrame.Description()).To(Equal(testDescription))
					Expect(topicFrame.Options()).To(Equal(testOptions))
				})
			})

			When("the topic not exists", func() {
				nonExistTopic := "no-exist-topic"
				BeforeEach(func() {
					_, err = topicClient.GetTopic(nonExistTopic)
				})
				It("error occurred", func() {
					Expect(err).To(MatchError(qerror.TopicNotExistError{Topic: nonExistTopic}))
				})
			})
		})

		Describe("Fetching topic list", func() {
			var err error
			var topics []string

			BeforeEach(func() {
				topics, err = topicClient.GetTopics()
				Expect(err).NotTo(HaveOccurred())
			})

			It("should have 1 item", func() {
				Expect(topics).To(HaveLen(1))
			})
			It("should have same item", func() {
				Expect(topics[0]).To(Equal(testTopic))
			})

			When("one more topic is added", Ordered, func() {
				testTopic2 := "test-topic2"

				BeforeAll(func() {
					err = topicClient.CreateTopic(testTopic2, topic.NewTopicFrame("", 0))
					Expect(err).NotTo(HaveOccurred())

					topics, err = topicClient.GetTopics()
					Expect(err).NotTo(HaveOccurred())
				})
				AfterAll(func() {
					topicClient.DeleteTopic(testTopic2)
				})

				It("should be has 2 item", func() {
					Expect(topics).To(HaveLen(2))
				})
				It("should be has same item", func() {
					sort.Strings(topics)
					Expect(topics[0]).To(Equal(testTopic))
					Expect(topics[1]).To(Equal(testTopic2))
				})
			})
		})

		Describe("Deleting a topic", func() {
			var err error

			BeforeAll(func() {
				err = topicClient.DeleteTopic(testTopic)
				Expect(err).NotTo(HaveOccurred())
			})

			When("fetching a deleted topic", func() {
				var err error
				BeforeEach(func() {
					_, err = topicClient.GetTopic(testTopic)
				})
				It("error occurred", func() {
					Expect(err).To(MatchError(qerror.TopicNotExistError{Topic: testTopic}))
				})
			})
		})
	})

	Context("TopicFragments", Ordered, func() {
		var testTopic string

		BeforeAll(func() {
			coordClient = inmemory.NewInMemCoordClient()
			topicClient = topic.NewCoordClientTopicWrapper(coordClient)
			testTopic = "test-topic-frag"
		})
		AfterAll(func() {
			coordClient.Close()
		})
		BeforeEach(func() {
			err := topicClient.CreateTopic(testTopic, topic.NewTopicFrame("", 0))
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			topicClient.DeleteTopic(testTopic)
		})

		Describe("Fetching topic fragments", func() {
			var err error

			When("the topic fragments exist", func() {
				var topicFragmentFrame topic.FragmentsFrame
				BeforeEach(func() {
					topicFragmentFrame, err = topicClient.GetTopicFragments(testTopic)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should be empty", func() {
					Expect(topicFragmentFrame.FragMappingInfo()).To(HaveLen(0))
				})
			})

			When("the topic fragments not exist", func() {
				nonExistTopic := "no-exist-topic"
				BeforeEach(func() {
					_, err = topicClient.GetTopicFragments(nonExistTopic)
				})
				It("error occurred", func() {
					Expect(err).To(MatchError(qerror.TopicNotExistError{Topic: nonExistTopic}))
				})
			})
		})

		Describe("Updating topic fragments", func() {
			var err error
			var fragment1Id uint = 10
			fragment1Info := topic.FragInfo{
				Active:      true,
				PublisherId: "test-publisher-1",
				Address:     "192.168.0.1:11010",
			}
			BeforeAll(func() {
				fragmentInfo := make(topic.FragMappingInfo)
				fragmentInfo[fragment1Id] = fragment1Info
				topicFragmentFrame := topic.NewTopicFragmentsFrame(fragmentInfo)
				err = topicClient.UpdateTopicFragments(testTopic, topicFragmentFrame)
				Expect(err).NotTo(HaveOccurred())
			})

			When("fetching updated topic fragments", func() {
				var topicFragmentFrame topic.FragmentsFrame
				BeforeEach(func() {
					topicFragmentFrame, err = topicClient.GetTopicFragments(testTopic)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should have same item", func() {
					fragmentInfo := topicFragmentFrame.FragMappingInfo()[fragment1Id]
					Expect(fragmentInfo).NotTo(BeNil())
					Expect(fragmentInfo.Active).To(Equal(fragment1Info.Active))
					Expect(fragmentInfo.PublisherId).To(Equal(fragment1Info.PublisherId))
					Expect(fragmentInfo.Address).To(Equal(fragment1Info.Address))
				})
			})
		})
	})

	Context("TopicSubscriptions", Ordered, func() {
		var testTopic string

		BeforeAll(func() {
			coordClient = inmemory.NewInMemCoordClient()
			topicClient = topic.NewCoordClientTopicWrapper(coordClient)
			testTopic = "test-topic-subscriptions"
		})
		AfterAll(func() {
			coordClient.Close()
		})
		BeforeEach(func() {
			err := topicClient.CreateTopic(testTopic, topic.NewTopicFrame("", 0))
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			topicClient.DeleteTopic(testTopic)
		})

		Describe("Fetching topic subscriptions", func() {
			var err error

			When("the topic subscriptions exist", func() {
				var topicSubscriptionFrame topic.SubscriptionsFrame
				BeforeEach(func() {
					topicSubscriptionFrame, err = topicClient.GetTopicSubscriptions(testTopic)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should be empty", func() {
					Expect(topicSubscriptionFrame.SubscriptionInfo()).To(HaveLen(0))
				})
			})

			When("the topic subscriptions not exist", func() {
				nonExistTopic := "no-exist-topic"
				BeforeEach(func() {
					_, err = topicClient.GetTopicSubscriptions(nonExistTopic)
				})
				It("error occurred", func() {
					Expect(err).To(MatchError(qerror.TopicNotExistError{Topic: nonExistTopic}))
				})
			})
		})

		Describe("Updating topic fragments", func() {
			var err error
			var fragment1Id uint = 10
			subscriber1Id := "test-subscriber-1"

			BeforeAll(func() {
				subscriptionInfo := make(topic.SubscriptionInfo)
				subscriptionInfo[subscriber1Id] = []uint{fragment1Id}
				topicSubscriptionFrame := topic.NewTopicSubscriptionsFrame(subscriptionInfo)
				err = topicClient.UpdateTopicSubscriptions(testTopic, topicSubscriptionFrame)
				Expect(err).NotTo(HaveOccurred())
			})

			When("fetching updated topic subscriptions", func() {
				var topicSubscriptionFrame topic.SubscriptionsFrame
				BeforeEach(func() {
					topicSubscriptionFrame, err = topicClient.GetTopicSubscriptions(testTopic)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should have same item", func() {
					subscriptionInfo := topicSubscriptionFrame.SubscriptionInfo()[subscriber1Id]
					Expect(subscriptionInfo).NotTo(BeNil())
					Expect(subscriptionInfo).To(HaveLen(1))
					Expect(subscriptionInfo[0]).To(Equal(fragment1Id))
				})
			})
		})
	})
})
