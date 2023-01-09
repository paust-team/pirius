package broker_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/pirius/bootstrapping/broker"
	"github.com/paust-team/pirius/bootstrapping/path"
	"github.com/paust-team/pirius/coordinating"
	"github.com/paust-team/pirius/coordinating/zk"
	"github.com/paust-team/pirius/test"
)

var _ = Describe("Coordclient", func() {
	var coordClient coordinating.CoordClient
	var brokerClient broker.CoordClientBrokerWrapper

	Context("Broker", Ordered, func() {
		BeforeEach(func() {
			coordClient = zk.NewZKCoordClient([]string{"127.0.0.1:2181"}, 5000)
			err := coordClient.Connect()
			Expect(err).NotTo(HaveOccurred())
			err = path.CreatePathsIfNotExist(coordClient)
			Expect(err).NotTo(HaveOccurred())
			brokerClient = broker.NewCoordClientWrapper(coordClient)
		})
		AfterEach(func() {
			coordClient.Close()
		})

		Describe("Adding a broker", func() {
			tp := test.NewTestParams()
			BeforeEach(func() {
				tp.Set("host", "127.0.0.1:1101")
				err := brokerClient.AddBroker(tp.GetString("host"))
				Expect(err).NotTo(HaveOccurred())
			})
			When("fetching broker list", func() {
				It("must have 1 item", func() {
					brokers, err := brokerClient.GetBrokers()
					Expect(err).NotTo(HaveOccurred())
					Expect(brokers).NotTo(BeNil())
					Expect(brokers).To(HaveLen(1))
				})
			})
			When("reconnecting the coordClient", func() {
				BeforeEach(func() {
					coordClient.Close()
					coordClient = zk.NewZKCoordClient([]string{"127.0.0.1:2181"}, 5000)
					err := coordClient.Connect()
					Expect(err).NotTo(HaveOccurred())
					brokerClient = broker.NewCoordClientWrapper(coordClient)
				})

				It("must have nil brokers", func() {
					brokers, err := brokerClient.GetBrokers()
					Expect(err).NotTo(HaveOccurred())
					Expect(brokers).To(BeNil())
				})
			})
		})

		Describe("Watching a brokers path", func() {
			var ctx context.Context
			var cancel context.CancelFunc
			var watchCh chan []string
			tp := test.NewTestParams()

			BeforeEach(func() {
				tp.Set("host", "127.0.0.1:1101")
				err := brokerClient.AddBroker(tp.GetString("host"))
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel = context.WithCancel(context.Background())
				watchCh, err = brokerClient.WatchBrokersPathChanged(ctx)
				Expect(err).NotTo(HaveOccurred())
			})
			AfterEach(func() {
				cancel()
			})

			When("adding a new broker", func() {
				BeforeEach(func() {
					tp.Set("host2", "127.0.0.1:1102")
					err := brokerClient.AddBroker(tp.GetString("host2"))
					Expect(err).NotTo(HaveOccurred())
				})
				It("should be able to receive watch event", func() {
					newBrokers := <-watchCh
					Expect(newBrokers).To(HaveLen(2))
					broker1, err := brokerClient.GetBroker(newBrokers[0])
					Expect(err).NotTo(HaveOccurred())
					Expect(broker1).To(Equal(tp.GetString("host")))

					broker2, err := brokerClient.GetBroker(newBrokers[1])
					Expect(err).NotTo(HaveOccurred())
					Expect(broker2).To(Equal(tp.GetString("host2")))
				})
			})
		})
	})
})
