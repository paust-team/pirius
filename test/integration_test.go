package test

import (
	"context"
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent"
	config2 "github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/proto/pb"
	"time"
)

var _ = Describe("IntegrationTest", Ordered, func() {
	var brokerInstance *broker.Instance
	var brokerFailedCh chan error
	tp := NewTestParams()

	BeforeAll(func() {
		tp.Set("topic", "test_ps_topic")
		brokerFailedCh = make(chan error)
		go func() {
			brokerCfg := config.NewBrokerConfig()
			brokerInstance = broker.NewInstance(brokerCfg)
			err := brokerInstance.Start()
			brokerFailedCh <- err
		}()
		var err error
		select {
		case <-brokerFailedCh:
			err = errors.New("cannot start a broker instance")
		case <-time.After(time.Second * 2):
			By("broker instance started")
		}
		Expect(err).NotTo(HaveOccurred())

		By("create topic through CreateTopic RPC")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		option := uint32(pb.TopicOption_UNIQUE_PER_FRAGMENT)
		createTopicRequest := &pb.CreateTopicRequest{
			Magic:       1,
			Name:        tp.GetString("topic"),
			Description: "",
			Options:     &option,
		}
		_, err = brokerInstance.CreateTopic(ctx, createTopicRequest)
		Expect(err).NotTo(HaveOccurred())

	})
	AfterAll(func() {
		By("delete topic through DeleteTopic RPC")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		deleteTopicRequest := &pb.TopicRequestWithName{Magic: 1, Name: tp.GetString("topic")}
		_, err := brokerInstance.DeleteTopic(ctx, deleteTopicRequest)
		Expect(err).NotTo(HaveOccurred())
		brokerInstance.Stop()
		err = <-brokerFailedCh
		Expect(err).NotTo(HaveOccurred())
		close(brokerFailedCh)
	})

	Context("Single pubsub", func() {
		var publisher *agent.Instance
		var subscriber *agent.Instance

		BeforeEach(func() {
			By("prepare a publisher")
			pubConfig := config2.NewAgentConfig()
			pubConfig.SetPort(11010)
			pubConfig.SetDataDir(constants.DefaultHomeDir + "/integration-pub")
			pubConfig.SetBindAddress("0.0.0.0")
			publisher = agent.NewInstance(pubConfig)
			err := publisher.Start()
			Expect(err).NotTo(HaveOccurred())

			By("prepare a subscriber")
			subConfig := config2.NewAgentConfig()
			subConfig.SetPort(11011)
			subConfig.SetDataDir(constants.DefaultHomeDir + "/integration-sub")
			subscriber = agent.NewInstance(subConfig)
			err = subscriber.Start()
			Expect(err).NotTo(HaveOccurred())

			tp.Set("records", [][]byte{
				{'g', 'o', 'o', 'g', 'l', 'e'},
				{'p', 'a', 'u', 's', 't', 'q'},
				{'1', '2', '3', '4', '5', '6'},
				{'a', 'b', 'c', 'd', 'e', 'f'},
				{'q', 'w', 'e', 'r', 't', 'y'},
				{'a', 's', 'd', 'f', 'g', 'h'},
			})
		})

		AfterEach(func() {
			publisher.Stop()
			subscriber.Stop()
			publisher.CleanAllData()
			subscriber.CleanAllData()
		})

		When("subscribing while publishing ", func() {
			BeforeEach(func() {
				tp.Set("batchSize", uint32(1))
				tp.Set("flushInterval", uint32(0))
				By("start publishing slowly")
				sendCh := make(chan pubsub.TopicData)
				err := publisher.StartPublish(tp.GetString("topic"), sendCh)
				Expect(err).NotTo(HaveOccurred())
				go func() {
					for i, record := range tp.GetBytesList("records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i),
							Data:   record,
						}
						time.Sleep(500 * time.Millisecond)
					}
				}()
			})
			It("can subscribe all publishing records", func(ctx SpecContext) {
				By("start subscribing")
				recvCh, err := subscriber.StartSubscribe(tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
				Expect(err).NotTo(HaveOccurred())

				idx := 0
				totalRecords := len(tp.GetBytesList("records"))
				for subscriptionResult := range recvCh {
					Expect(subscriptionResult).To(HaveLen(1))
					Expect(subscriptionResult[0].SeqNum).To(Equal(uint64(idx)))
					Expect(subscriptionResult[0].Data).To(Equal(tp.GetBytesList("records")[idx]))
					idx++
					if idx == totalRecords {
						break
					}
				}
			}, SpecTimeout(6*time.Second))
		})

		When("subscribing with batched", func() {
			var sendCh chan pubsub.TopicData
			BeforeEach(func() {
				tp.Set("batchSize", uint32(2))
				tp.Set("flushInterval", uint32(500))

				By("start publishing fastly")
				sendCh = make(chan pubsub.TopicData)
				err := publisher.StartPublish(tp.GetString("topic"), sendCh)
				Expect(err).NotTo(HaveOccurred())
				go func() {
					for i, record := range tp.GetBytesList("records") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i),
							Data:   record,
						}
					}
				}()
			})
			AfterEach(func() {
				close(sendCh)
			})
			It("number of subscription result should be equal to batch-size", func(ctx SpecContext) {
				By("start subscribing")
				recvCh, err := subscriber.StartSubscribe(tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
				Expect(err).NotTo(HaveOccurred())

				idx := 0
				batchSize := int(tp.GetUint32("batchSize"))
				totalRecords := len(tp.GetBytesList("records"))
				for subscriptionResult := range recvCh {
					Expect(subscriptionResult).To(HaveLen(batchSize))
					for i := 0; i < batchSize; i++ {
						Expect(subscriptionResult[i].SeqNum).To(Equal(uint64(idx + i)))
						Expect(subscriptionResult[i].Data).To(Equal(tp.GetBytesList("records")[idx+i]))
					}

					idx += batchSize
					if idx == totalRecords {
						break
					}
				}
			}, SpecTimeout(6*time.Second))
		})
	})

	Context("Multiple pubsub", func() {
		//TODO: implement me
	})

	Context("Publisher Scaling", func() {
		//TODO: implement me
	})

	Context("Subscriber Scaling", func() {
		//TODO: implement me
	})
})
