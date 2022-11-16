package test

import (
	"context"
	"errors"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/paust-team/shapleq/agent"
	config2 "github.com/paust-team/shapleq/agent/config"
	"github.com/paust-team/shapleq/agent/pubsub"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/constants"
	"github.com/paust-team/shapleq/helper"
	"github.com/paust-team/shapleq/proto/pb"
	"sync"
	"time"
)

var _ = Describe("IntegrationTest_WithDistributionRule", Ordered, func() {
	var brokerInstance *broker.Instance
	var brokerFailedCh chan error
	tp := NewTestParams()

	BeforeAll(func() {
		tp.Set("topic", "test_ps_topic")
		tp.Set("records1", [][]byte{
			{'g', 'o', 'o', 'g', 'l', 'e'},
			{'p', 'a', 'u', 's', 't', 'q'},
			{'1', '2', '3', '4', '5', '6'},
			{'a', 'b', 'c', 'd', 'e', 'f'},
			{'q', 'w', 'e', 'r', 't', 'y'},
			{'a', 's', 'd', 'f', 'g', 'h'},
		})

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
				err := publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
				Expect(err).NotTo(HaveOccurred())
				go func() {
					for i, record := range tp.GetBytesList("records1") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i),
							Data:   record,
						}
						time.Sleep(500 * time.Millisecond)
					}
				}()
			})
			It("can subscribe all publishing records1", func(ctx SpecContext) {
				By("start subscribing")
				recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
				Expect(err).NotTo(HaveOccurred())

				idx := 0
				totalRecords := len(tp.GetBytesList("records1"))
				for subscriptionResult := range recvCh {
					Expect(subscriptionResult).To(HaveLen(1))
					Expect(subscriptionResult[0].SeqNum).To(Equal(uint64(idx)))
					Expect(subscriptionResult[0].Data).To(Equal(tp.GetBytesList("records1")[idx]))
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
				err := publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
				Expect(err).NotTo(HaveOccurred())
				go func() {
					for i, record := range tp.GetBytesList("records1") {
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
				recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
				Expect(err).NotTo(HaveOccurred())

				idx := 0
				batchSize := int(tp.GetUint32("batchSize"))
				totalRecords := len(tp.GetBytesList("records1"))
				for subscriptionResult := range recvCh {
					Expect(subscriptionResult).To(HaveLen(batchSize))
					for i := 0; i < batchSize; i++ {
						Expect(subscriptionResult[i].SeqNum).To(Equal(uint64(idx + i)))
						Expect(subscriptionResult[i].Data).To(Equal(tp.GetBytesList("records1")[idx+i]))
					}

					idx += batchSize
					if idx == totalRecords {
						break
					}
				}
			}, SpecTimeout(6*time.Second))
		})
	})

	Context("Publisher Scaling", Ordered, func() {
		var publishers []*agent.Instance
		var subscribers []*agent.Instance
		var sendChs []chan pubsub.TopicData
		var wg sync.WaitGroup
		BeforeAll(func() {
			tp.Set("port1", uint(11010))
			tp.Set("port2", uint(11011))
			tp.Set("batchSize", uint32(1))
			tp.Set("flushInterval", uint32(0))
			tp.Set("records2", [][]byte{
				{'z', 'o', 'o', 'g', 'l', 'e'},
				{'z', 'a', 'u', 's', 't', 'q'},
				{'z', '2', '3', '4', '5', '6'},
				{'z', 'b', 'c', 'd', 'e', 'f'},
				{'z', 'w', 'e', 'r', 't', 'y'},
				{'z', 's', 'd', 'f', 'g', 'h'},
			})
		})
		BeforeEach(func() {
			By("prepare a 2 subscribers")
			numSubscribers := 2
			for i := 0; i < numSubscribers; i++ {
				subConfig := config2.NewAgentConfig()
				subConfig.SetDataDir(fmt.Sprintf("%s/integration2-sub%d", constants.DefaultHomeDir, i))
				subscriber := agent.NewInstance(subConfig)
				err := subscriber.Start()
				Expect(err).NotTo(HaveOccurred())
				subscribers = append(subscribers, subscriber)
			}
			wg = sync.WaitGroup{}
		})
		AfterEach(func() {
			for _, publisher := range publishers {
				publisher.Stop()
				publisher.CleanAllData()
			}
			for _, subscriber := range subscribers {
				subscriber.Stop()
				subscriber.CleanAllData()
			}
			wg.Wait()
			for _, ch := range sendChs {
				close(ch)
			}

			publishers = []*agent.Instance{}
			subscribers = []*agent.Instance{}
			sendChs = []chan pubsub.TopicData{}
		})

		Describe("Scale up publisher 1 to 2", func() {
			BeforeEach(func() {
				By("prepare a publisher")
				pubConfig := config2.NewAgentConfig()
				pubConfig.SetPort(tp.GetUint("port1"))
				pubConfig.SetDataDir(constants.DefaultHomeDir + "/integration2-pub")
				pubConfig.SetBindAddress("0.0.0.0")
				publisher := agent.NewInstance(pubConfig)
				err := publisher.Start()
				Expect(err).NotTo(HaveOccurred())
				publishers = append(publishers, publisher)

				By("start publishing slowly")
				sendCh := make(chan pubsub.TopicData)
				err = publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
				Expect(err).NotTo(HaveOccurred())
				go func() {
					for i, record := range tp.GetBytesList("records1") {
						sendCh <- pubsub.TopicData{
							SeqNum: uint64(i),
							Data:   record,
						}
						time.Sleep(500 * time.Millisecond)
					}
				}()
			})

			When("a new publisher connected while subscribing", func() {
				var recvChs []chan []pubsub.SubscriptionResult
				BeforeEach(func() {
					By("prepare a new publisher")
					pubConfig := config2.NewAgentConfig()
					pubConfig.SetPort(tp.GetUint("port2"))
					pubConfig.SetDataDir(constants.DefaultHomeDir + "/integration2-pub2")
					pubConfig.SetBindAddress("0.0.0.0")
					publisher := agent.NewInstance(pubConfig)
					err := publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					publishers = append(publishers, publisher)

					By("start subscribing")
					for _, subscriber := range subscribers {
						recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
						Expect(err).NotTo(HaveOccurred())
						recvChs = append(recvChs, recvCh)
					}

					By("start publishing fastly")
					sendCh := make(chan pubsub.TopicData)
					sendChs = append(sendChs, sendCh)
					err = publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())
					go func() {
						for i, record := range tp.GetBytesList("records2") {
							sendCh <- pubsub.TopicData{
								SeqNum: uint64(i),
								Data:   record,
							}
							time.Sleep(50 * time.Millisecond)
						}
					}()
				})
				It("can subscribe all publisher's records", func(ctx SpecContext) {
					totalRecords := append(tp.GetBytesList("records1"), tp.GetBytesList("records2")...)
					var receivedRecords [][]byte
					recvCh := helper.MergeChannels(recvChs...)
					for subscriptionResult := range recvCh {
						Expect(subscriptionResult).To(HaveLen(int(tp.GetUint32("batchSize"))))
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, totalRecords)).To(BeTrue())
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, receivedRecords)).To(BeFalse())
						receivedRecords = append(receivedRecords, subscriptionResult[0].Data)

						if len(receivedRecords) == len(totalRecords) {
							break
						}
					}
				}, SpecTimeout(10*time.Second))
			})
		})

		Describe("Scale down publisher 2 to 1", func() {
			BeforeEach(func() {
				By("prepare a publishers")
				numPublishers := 2
				for i := 0; i < numPublishers; i++ {
					pubConfig := config2.NewAgentConfig()
					pubConfig.SetPort(tp.GetUint(fmt.Sprintf("port%d", i+1)))
					pubConfig.SetDataDir(fmt.Sprintf("%s/integration2-pub%d", constants.DefaultHomeDir, i))
					pubConfig.SetBindAddress("0.0.0.0")
					publisher := agent.NewInstance(pubConfig)
					err := publisher.Start()
					Expect(err).NotTo(HaveOccurred())
					publishers = append(publishers, publisher)
				}

				By("start publishing slowly")
				for i, publisher := range publishers {
					sendCh := make(chan pubsub.TopicData, 10)
					sendChs = append(sendChs, sendCh)
					err := publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						for j, record := range tp.GetBytesList(fmt.Sprintf("records%d", idx+1)) {
							sendCh <- pubsub.TopicData{
								SeqNum: uint64(j),
								Data:   record,
							}
							time.Sleep(500 * time.Millisecond)
						}
					}(i)
				}
			})
			When("a publisher disconnected while subscribing", func() {
				var recvChs []chan []pubsub.SubscriptionResult
				BeforeEach(func() {
					By("start subscribing")
					for _, subscriber := range subscribers {
						recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
						Expect(err).NotTo(HaveOccurred())
						recvChs = append(recvChs, recvCh)
					}

					By("stop publisher 2")
					publishers[1].Stop()
					publishers[1].CleanAllData()
					publishers = publishers[0:1]
				})
				It("can only subscribe active publisher records", func(ctx SpecContext) {
					idx := 0
					totalRecords := append(tp.GetBytesList("records1"), tp.GetBytesList("records2")...)
					activeRecords := tp.GetBytesList("records1")
					var receivedRecords [][]byte
					recvCh := helper.MergeChannels(recvChs...)
					for subscriptionResult := range recvCh {
						Expect(subscriptionResult).To(HaveLen(int(tp.GetUint32("batchSize"))))
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, totalRecords)).To(BeTrue())
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, receivedRecords)).To(BeFalse())
						receivedRecords = append(receivedRecords, subscriptionResult[0].Data)

						if helper.IsContainBytes(subscriptionResult[0].Data, activeRecords) {
							idx++
						}
						if idx == len(activeRecords) {
							break
						}
					}
				}, SpecTimeout(10*time.Second))
			})
		})
	})

	Context("Subscriber Scaling", func() {
		var publishers []*agent.Instance
		var subscribers []*agent.Instance
		var sendChs []chan pubsub.TopicData
		var wg sync.WaitGroup
		BeforeAll(func() {
			tp.Set("port1", uint(11010))
			tp.Set("port2", uint(11011))
			tp.Set("batchSize", uint32(1))
			tp.Set("flushInterval", uint32(0))
			tp.Set("records2", [][]byte{
				{'z', 'o', 'o', 'g', 'l', 'e'},
				{'z', 'a', 'u', 's', 't', 'q'},
				{'z', '2', '3', '4', '5', '6'},
				{'z', 'b', 'c', 'd', 'e', 'f'},
				{'z', 'w', 'e', 'r', 't', 'y'},
				{'z', 's', 'd', 'f', 'g', 'h'},
			})
		})
		BeforeEach(func() {
			By("prepare a 2 publishers")
			numPublishers := 2
			for i := 0; i < numPublishers; i++ {
				pubConfig := config2.NewAgentConfig()
				pubConfig.SetPort(tp.GetUint(fmt.Sprintf("port%d", i+1)))
				pubConfig.SetDataDir(fmt.Sprintf("%s/integration3-pub%d", constants.DefaultHomeDir, i))
				pubConfig.SetBindAddress("0.0.0.0")
				publisher := agent.NewInstance(pubConfig)
				err := publisher.Start()
				Expect(err).NotTo(HaveOccurred())
				publishers = append(publishers, publisher)
			}
			wg = sync.WaitGroup{}
		})
		AfterEach(func() {
			for _, publisher := range publishers {
				publisher.Stop()
				publisher.CleanAllData()
			}
			for _, subscriber := range subscribers {
				subscriber.Stop()
				subscriber.CleanAllData()
			}
			wg.Wait()
			for _, ch := range sendChs {
				close(ch)
			}

			publishers = []*agent.Instance{}
			subscribers = []*agent.Instance{}
			sendChs = []chan pubsub.TopicData{}
		})
		Describe("Scale up subscriber 1 to 2", func() {
			var recvChs []chan []pubsub.SubscriptionResult

			BeforeEach(func() {
				By("start publishing slowly")
				for i, publisher := range publishers {
					sendCh := make(chan pubsub.TopicData, 10)
					sendChs = append(sendChs, sendCh)
					err := publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						for j, record := range tp.GetBytesList(fmt.Sprintf("records%d", idx+1)) {
							sendCh <- pubsub.TopicData{
								SeqNum: uint64(j),
								Data:   record,
							}
							time.Sleep(500 * time.Millisecond)
						}
					}(i)
				}

				By("prepare a subscriber1")
				subConfig := config2.NewAgentConfig()
				subConfig.SetDataDir(constants.DefaultHomeDir + "/integration3-sub")
				subscriber := agent.NewInstance(subConfig)
				err := subscriber.Start()
				Expect(err).NotTo(HaveOccurred())

				By("subscriber1: start subscribing")
				recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
				Expect(err).NotTo(HaveOccurred())
				subscribers = append(subscribers, subscriber)
				recvChs = append(recvChs, recvCh)
			})

			When("a new publisher connected while subscribing", func() {
				BeforeEach(func() {
					By("prepare a subscriber2")
					subConfig := config2.NewAgentConfig()
					subConfig.SetDataDir(constants.DefaultHomeDir + "/integration3-sub2")
					subscriber := agent.NewInstance(subConfig)
					err := subscriber.Start()
					Expect(err).NotTo(HaveOccurred())

					By("subscriber2: start subscribing")
					recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
					Expect(err).NotTo(HaveOccurred())
					subscribers = append(subscribers, subscriber)
					recvChs = append(recvChs, recvCh)
				})
				It("can subscribe all publisher's records", func(ctx SpecContext) {
					totalRecords := append(tp.GetBytesList("records1"), tp.GetBytesList("records2")...)
					var receivedRecords [][]byte
					recvCh := helper.MergeChannels(recvChs...)
					for subscriptionResult := range recvCh {
						Expect(subscriptionResult).To(HaveLen(int(tp.GetUint32("batchSize"))))
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, totalRecords)).To(BeTrue())
						Expect(helper.IsContainBytes(subscriptionResult[0].Data, receivedRecords)).To(BeFalse())
						receivedRecords = append(receivedRecords, subscriptionResult[0].Data)
						if len(receivedRecords) == len(totalRecords) {
							break
						}
					}
				}, SpecTimeout(10*time.Second))
			})
		})

		Describe("Scale down subscriber 2 to 1", func() {
			BeforeEach(func() {
				By("start publishing slowly")
				for i, publisher := range publishers {
					sendCh := make(chan pubsub.TopicData, 10)
					sendChs = append(sendChs, sendCh)
					err := publisher.StartPublish(context.Background(), tp.GetString("topic"), sendCh)
					Expect(err).NotTo(HaveOccurred())
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						for j, record := range tp.GetBytesList(fmt.Sprintf("records%d", idx+1)) {
							sendCh <- pubsub.TopicData{
								SeqNum: uint64(j),
								Data:   record,
							}
							time.Sleep(500 * time.Millisecond)
						}
					}(i)
				}

				By("prepare a subscribers")
				numSubscribers := 2
				for i := 0; i < numSubscribers; i++ {
					subConfig := config2.NewAgentConfig()
					subConfig.SetDataDir(fmt.Sprintf("%s/integration3-sub%d", constants.DefaultHomeDir, i))
					subscriber := agent.NewInstance(subConfig)
					err := subscriber.Start()
					Expect(err).NotTo(HaveOccurred())
					subscribers = append(subscribers, subscriber)
				}
			})
			When("a subscriber disconnected while subscribing", func() {
				var recvChs []chan []pubsub.SubscriptionResult
				BeforeEach(func() {
					By("start subscribing")
					for _, subscriber := range subscribers {
						recvCh, err := subscriber.StartSubscribe(context.Background(), tp.GetString("topic"), tp.GetUint32("batchSize"), tp.GetUint32("flushInterval"))
						Expect(err).NotTo(HaveOccurred())
						recvChs = append(recvChs, recvCh)
					}

					By("stop subscriber2")
					go func() {
						time.Sleep(500 * time.Millisecond)
						subscribers[1].Stop()
						subscribers[1].CleanAllData()
						subscribers = subscribers[0:1]
					}()
				})
				It("active subscriber can subscribe all publisher's records", func() {
					totalRecords := append(tp.GetBytesList("records1"), tp.GetBytesList("records2")...)
					var receivedRecords [][]byte
					swg := sync.WaitGroup{}
					swg.Add(1)
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						for {
							select {
							case <-ctx.Done():
								return
							case result, ok := <-recvChs[1]:
								By("check removed subscriber channel")
								if ok {
									Expect(result).To(HaveLen(int(tp.GetUint32("batchSize"))))
									Expect(helper.IsContainBytes(result[0].Data, totalRecords)).To(BeTrue())
									receivedRecords = append(receivedRecords, result[0].Data)
								}

							case result, ok := <-recvChs[0]:
								if !ok {
									return
								}
								By("check active subscriber channel")
								Expect(result).To(HaveLen(int(tp.GetUint32("batchSize"))))
								Expect(helper.IsContainBytes(result[0].Data, totalRecords)).To(BeTrue())
								Expect(helper.IsContainBytes(result[0].Data, receivedRecords)).To(BeFalse())
								receivedRecords = append(receivedRecords, result[0].Data)
							}
						}
					}()

					By("Wait for 5sec and check all records have received")
					select {
					case <-time.After(5 * time.Second):
						cancel()
						Expect(receivedRecords).To(HaveLen(len(totalRecords)))
					}
				})
			})
		})
	})
})
