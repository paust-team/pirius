package integration_test

import (
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"sync"
	"testing"
	"time"
)

func Sleep(sec int) {
	time.Sleep(time.Duration(sec) * time.Second)
}

var defaultLogLevel = logger.Info

type brokerTestContext struct {
	config   *config.BrokerConfig
	instance *broker.Broker
	wg       sync.WaitGroup
}

func newBrokerTestContext(port uint, zkAddrs []string, dataDir string, logDir string) *brokerTestContext {
	cfg := config.NewBrokerConfig()
	cfg.SetPort(port)
	cfg.SetLogLevel(defaultLogLevel)
	cfg.SetZKQuorum(zkAddrs)
	cfg.SetDataDir(dataDir)
	cfg.SetLogDir(logDir)

	return &brokerTestContext{
		config:   cfg,
		instance: broker.NewBroker(cfg),
		wg:       sync.WaitGroup{},
	}
}

func (b *brokerTestContext) start() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.instance.Start()
	}()
}

func (b *brokerTestContext) stop() {
	b.instance.Stop()
	b.wg.Wait()
	b.instance.Clean()
}

type producerTestContext struct {
	config    *config2.ProducerConfig
	nodeId    string
	instance  *client.Producer
	wg        sync.WaitGroup
	publishCh chan *client.PublishData
	t         *testing.T
}

func newProducerTestContext(nodeId string, topic string, t *testing.T) *producerTestContext {
	producerConfig := config2.NewProducerConfig()
	producerConfig.SetLogLevel(defaultLogLevel)
	producerConfig.SetServerAddresses([]string{"127.0.0.1:2181"})
	producer := client.NewProducer(producerConfig, topic)

	return &producerTestContext{
		config:    producerConfig,
		nodeId:    nodeId,
		instance:  producer,
		wg:        sync.WaitGroup{},
		publishCh: make(chan *client.PublishData),
		t:         t,
	}
}

func (p *producerTestContext) start() {
	if err := p.instance.Connect(); err != nil {
		p.t.Fatal(err)
	}
}

func (p *producerTestContext) stop() {
	close(p.publishCh)
	p.instance.Close()
}

func (p *producerTestContext) asyncPublish(records [][]byte) *producerTestContext {
	fragmentCh, pubErrCh, err := p.instance.AsyncPublish(p.publishCh)
	if err != nil {
		p.t.Fatal(err)
	}

	p.wg.Add(1)
	go func(recordLength int) {
		defer p.wg.Done()
		published := 0
		for {
			select {
			case err, ok := <-pubErrCh:
				if !ok {
					return
				}
				if err != nil {
					p.t.Error(err)
					return
				}
			case _, ok := <-fragmentCh:
				if !ok {
					return
				}
				//fmt.Printf("publish succeed. fragmentId=%d offset=%d\n", fragment.FragmentId, fragment.LastOffset)
				published++
				if published == recordLength {
					fmt.Printf("publisher(%s) is finished\n", p.nodeId)
					return
				}
			}
		}
	}(len(records))

	go func(recordsToPublish [][]byte) {
		for index, record := range recordsToPublish {
			p.publishCh <- &client.PublishData{
				Data:   record,
				NodeId: p.nodeId,
				SeqNum: uint64(index),
			}
		}
	}(records)

	return p
}

func (p *producerTestContext) waitFinished() {
	p.wg.Wait()
}

type consumerTestContext struct {
	config       *config2.ConsumerConfig
	nodeId       string
	instance     *client.Consumer
	wg           sync.WaitGroup
	onCompleteFn func()
	t            *testing.T
}

func newConsumerTestContext(nodeId string, topic string, fragmentOffsets map[uint32]uint64, t *testing.T) *consumerTestContext {
	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(defaultLogLevel)
	consumerConfig.SetServerAddresses([]string{"127.0.0.1:2181"})
	consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
	return &consumerTestContext{
		config:   consumerConfig,
		nodeId:   nodeId,
		instance: consumer,
		wg:       sync.WaitGroup{},
		t:        t,
	}
}

func (c *consumerTestContext) start() {
	if err := c.instance.Connect(); err != nil {
		c.t.Fatal(err)
	}
}

func (c *consumerTestContext) stop() {
	c.instance.Close()
}

func (c *consumerTestContext) onSubscribe(maxBatchSize, flushInterval uint32, fn func(*client.SubscribeResult) bool) *consumerTestContext {

	receiveCh, subErrCh, err := c.instance.Subscribe(maxBatchSize, flushInterval)
	if err != nil {
		c.t.Fatal(err)
	}
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		for {
			select {
			case received, ok := <-receiveCh:
				if !ok {
					return
				}
				if finished := fn(received); finished {
					c.onCompleteFn()
					return
				}

			case err := <-subErrCh:
				c.t.Error(err)
				return
			}
		}
	}()

	return c
}

func (c *consumerTestContext) onComplete(fn func()) *consumerTestContext {
	c.onCompleteFn = fn
	return c
}

func (c *consumerTestContext) waitFinished() {
	c.wg.Wait()
}

type ShapleQTestContext struct {
	logLevel          logger.LogLevel
	brokerPorts       []uint
	zkAddrs           []string
	zkTimeoutMS       uint
	zkFlushIntervalMS uint
	zkClient          *zookeeper.ZKQClient
	brokers           []*brokerTestContext
	producers         []*producerTestContext
	consumers         []*consumerTestContext
	running           bool
	adminClient       *client.Admin
	t                 *testing.T
}

func DefaultShapleQTestContext(t *testing.T) *ShapleQTestContext {
	return &ShapleQTestContext{
		logLevel:          defaultLogLevel,
		brokerPorts:       []uint{1101},
		zkAddrs:           []string{"127.0.0.1:2181"},
		zkTimeoutMS:       3000,
		zkFlushIntervalMS: 2000,
		brokers:           []*brokerTestContext{},
		producers:         []*producerTestContext{},
		consumers:         []*consumerTestContext{},
		running:           false,
		t:                 t,
	}
}

func (s *ShapleQTestContext) Start() {
	s.zkClient = zookeeper.NewZKQClient(s.zkAddrs, s.zkTimeoutMS, s.zkFlushIntervalMS)
	if err := s.zkClient.Connect(); err != nil {
		s.t.Fatal(err)
	}
	// Start brokers
	var brokerAddrs []string
	for index, port := range s.brokerPorts {
		dataDir := fmt.Sprintf("%s/data-test/broker-%d", common.DefaultHomeDir, index)
		logDir := fmt.Sprintf("%s/log-test/broker-%d", common.DefaultHomeDir, index)
		brokerContext := newBrokerTestContext(port, s.zkAddrs, dataDir, logDir)
		s.brokers = append(s.brokers, brokerContext)
		brokerContext.start()
		brokerAddrs = append(brokerAddrs, fmt.Sprintf("127.0.0.1:%d", port))
	}

	Sleep(1) // wait for starting brokers..

	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(defaultLogLevel)
	adminConfig.SetServerAddresses(brokerAddrs)
	s.adminClient = client.NewAdmin(adminConfig)
	if err := s.adminClient.Connect(); err != nil {
		s.t.Fatal(err)
	}

	s.running = true
}

func (s *ShapleQTestContext) Stop() {
	if s.running {
		for _, ctx := range s.producers {
			ctx.stop()
		}
		for _, ctx := range s.consumers {
			ctx.stop()
		}
		for _, ctx := range s.brokers {
			ctx.stop()
		}
		s.zkClient.RemoveAllPath()
		s.zkClient.Close()
		s.adminClient.Close()
	}
}

func (s *ShapleQTestContext) SetupTopicAndFragments(topic string, fragmentCount int) map[uint32]uint64 {
	fragmentOffsets := make(map[uint32]uint64)

	if err := s.adminClient.CreateTopic(topic, "test"); err != nil {
		s.t.Fatal(err)
	}

	for i := 0; i < fragmentCount; i++ {
		fragment, err := s.adminClient.CreateFragment(topic)
		if err != nil {
			s.t.Fatal(err)
		}
		fragmentOffsets[fragment.Id] = 1 // set start offset with 1
	}
	return fragmentOffsets
}

func (s *ShapleQTestContext) AddProducerContext(nodeId string, topic string) *producerTestContext {
	ctx := newProducerTestContext(nodeId, topic, s.t)
	ctx.start()
	s.producers = append(s.producers, ctx)
	return ctx
}

func (s *ShapleQTestContext) AddConsumerContext(nodeId string, topic string, fragmentOffsets map[uint32]uint64) *consumerTestContext {
	ctx := newConsumerTestContext(nodeId, topic, fragmentOffsets, s.t)
	ctx.start()
	s.consumers = append(s.consumers, ctx)
	return ctx
}
