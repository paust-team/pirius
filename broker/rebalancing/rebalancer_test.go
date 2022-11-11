package rebalancing_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/bootstrapping"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/broker/rebalancing"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/coordinating/zk"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/test"
	"time"
)

var _ = Describe("Rebalancer", func() {
	var coordClient coordinating.CoordClient
	var bootstrapper *bootstrapping.BootstrapService
	var rebalancer rebalancing.Rebalancer
	var cancelRebalancer context.CancelFunc
	Context("Perform rebalancing from topic event", func() {
		tp := test.NewTestParams()

		BeforeEach(func() {
			tp.Set("broker-host", "127.0.0.1:1101")
			tp.Set("topic", "test-rebalance")
			tp.Set("publisher-id", "test-publisher-1")
			tp.Set("publisher-addr", "192.168.0.1:11010")
			tp.Set("subscriber-id1", "test-subscriber-1")
			tp.Set("subscriber-id2", "test-subscriber-2")

			// setup coordinator
			coordClient = zk.NewZKCoordClient([]string{"127.0.0.1:2181"}, 5000)
			err := coordClient.Connect()
			Expect(err).NotTo(HaveOccurred())
			err = path.CreatePathsIfNotExist(coordClient)
			Expect(err).NotTo(HaveOccurred())
			bootstrapper = bootstrapping.NewBootStrapService(coordClient)

			// add initial broker
			err = bootstrapper.AddBroker(tp.GetString("broker-host"))
			Expect(err).NotTo(HaveOccurred())

		})
		AfterEach(func() {
			tp.Clear()
			cancelRebalancer()
			coordClient.Close()
		})

		Context("Topic has no option", Ordered, func() {
			BeforeAll(func() {
				err := bootstrapper.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", 0))
				Expect(err).NotTo(HaveOccurred())

				// run rebalancer
				rebalancer = rebalancing.NewRebalancer(bootstrapper, tp.GetString("broker-host"))
				ctx, cancel := context.WithCancel(context.Background())
				cancelRebalancer = cancel
				err = rebalancer.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				err = bootstrapper.AddPublisher(tp.GetString("topic"), tp.GetString("publisher-id"), tp.GetString("publisher-addr"))
				Expect(err).NotTo(HaveOccurred())
			})
			AfterAll(func() {
				bootstrapper.DeleteTopic(tp.GetString("topic"))
			})
			When("two subscriber appears", Ordered, func() {
				BeforeAll(func() {
					err := bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber-id1"))
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(100 * time.Millisecond)

					err = bootstrapper.AddSubscriber(tp.GetString("topic"), tp.GetString("subscriber-id2"))
					Expect(err).NotTo(HaveOccurred())
				})
				It("should be processed by default rule executor", func() {
					By("fragments are assigned equally to all subscribers")
					time.Sleep(500 * time.Millisecond) // wait for rebalancing
					topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					subscription1 := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id1")]
					subscription2 := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id2")]
					Expect(len(subscription1)).To(Equal(len(subscription2)))
					Expect(helper.HasAllElements(subscription1, subscription2)).To(BeTrue())
				})
			})
		})

		Context("Topic has UniquePerFragment option", Ordered, func() {
			BeforeAll(func() {
				err := bootstrapper.CreateTopic(tp.GetString("topic"), topic.NewTopicFrame("", topic.UniquePerFragment))
				Expect(err).NotTo(HaveOccurred())

				// run rebalancer
				rebalancer = rebalancing.NewRebalancer(bootstrapper, tp.GetString("broker-host"))
				ctx, cancel := context.WithCancel(context.Background())
				cancelRebalancer = cancel
				err = rebalancer.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				err = bootstrapper.AddPublisher(tp.GetString("topic"), tp.GetString("publisher-id"), tp.GetString("publisher-addr"))
				Expect(err).NotTo(HaveOccurred())
			})
			AfterAll(func() {
				bootstrapper.DeleteTopic(tp.GetString("topic"))
			})
			When("two subscriber appears", func() {
				It("should be processed by distribution rule executor", func() {
					By("fragments are distributed to all subscribers")
					time.Sleep(500 * time.Millisecond) // wait for rebalancing
					topicSubscriptionFrame, err := bootstrapper.GetTopicSubscriptions(tp.GetString("topic"))
					Expect(err).NotTo(HaveOccurred())
					subscription1 := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id1")]
					subscription2 := topicSubscriptionFrame.SubscriptionInfo()[tp.GetString("subscriber-id2")]
					Expect(len(subscription1)).To(Equal(len(subscription2)))
					Expect(helper.HasSameElement(subscription1, subscription2)).To(BeFalse())
				})
			})
		})
	})
})
