package integration_test

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/paust-team/shapleq/broker"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/client"
	config2 "github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/zookeeper"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var defaultLogLevel = logger.Info

type brokerTestContext struct {
	config   *config.BrokerConfig
	instance *broker.Broker
	wg       sync.WaitGroup
}

func newBrokerTestContext(port uint, timeout int, zkAddrs []string, dataDir string, logDir string) *brokerTestContext {
	cfg := config.NewBrokerConfig()
	cfg.SetPort(port)
	cfg.SetLogLevel(defaultLogLevel)
	cfg.SetZKQuorum(zkAddrs)
	cfg.SetDataDir(dataDir)
	cfg.SetLogDir(logDir)
	cfg.SetTimeout(timeout)

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
	onErrorFn func(error)
}

func newProducerTestContext(nodeId string, topic string) *producerTestContext {
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
	}
}

func (p *producerTestContext) start() error {
	return p.instance.Connect()
}

func (p *producerTestContext) stop() {
	close(p.publishCh)
	p.instance.Close()
}

func (p *producerTestContext) asyncPublish(records [][]byte) *producerTestContext {
	fragmentCh, pubErrCh, err := p.instance.AsyncPublish(p.publishCh)
	if err != nil {
		if p.onErrorFn != nil {
			p.onErrorFn(err)
		}
		return p
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
					if p.onErrorFn != nil {
						p.onErrorFn(err)
					}
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

func (p *producerTestContext) onError(fn func(error)) *producerTestContext {
	p.onErrorFn = fn
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
	onErrorFn    func(error)
}

func newConsumerTestContext(nodeId string, topic string, fragmentOffsets map[uint32]uint64) *consumerTestContext {
	consumerConfig := config2.NewConsumerConfig()
	consumerConfig.SetLogLevel(defaultLogLevel)
	consumerConfig.SetServerAddresses([]string{"127.0.0.1:2181"})
	consumer := client.NewConsumer(consumerConfig, topic, fragmentOffsets)
	return &consumerTestContext{
		config:   consumerConfig,
		nodeId:   nodeId,
		instance: consumer,
		wg:       sync.WaitGroup{},
	}
}

func (c *consumerTestContext) start() error {
	return c.instance.Connect()
}

func (c *consumerTestContext) stop() {
	c.instance.Close()
}

func (c *consumerTestContext) onSubscribe(maxBatchSize, flushInterval uint32, fn func(*client.SubscribeResult) bool) *consumerTestContext {

	receiveCh, subErrCh, err := c.instance.Subscribe(maxBatchSize, flushInterval)
	if err != nil {
		if c.onErrorFn != nil {
			c.onErrorFn(err)
		}
		return c
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
					if c.onCompleteFn != nil {
						c.onCompleteFn()
					}
					return
				}

			case err := <-subErrCh:
				if c.onErrorFn != nil {
					c.onErrorFn(err)
				}
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

func (c *consumerTestContext) onError(fn func(error)) *consumerTestContext {
	c.onErrorFn = fn
	return c
}

func (c *consumerTestContext) waitFinished() {
	c.wg.Wait()
}

type ShapleQTestContext struct {
	logLevel          logger.LogLevel
	brokerPorts       []uint
	brokerTimeout     int
	zkAddrs           []string
	zkTimeoutMS       uint
	zkFlushIntervalMS uint
	zkClient          *zookeeper.ZKQClient
	brokers           []*brokerTestContext
	producers         []*producerTestContext
	consumers         []*consumerTestContext
	running           bool
	t                 *testing.T
	params            *TestParams
}

func DefaultShapleQTestContext(testName string, t *testing.T) *ShapleQTestContext {
	return &ShapleQTestContext{
		logLevel:          defaultLogLevel,
		brokerPorts:       []uint{1101},
		zkAddrs:           []string{"127.0.0.1:2181"},
		zkTimeoutMS:       3000,
		zkFlushIntervalMS: 2000,
		brokerTimeout:     3000,
		brokers:           []*brokerTestContext{},
		producers:         []*producerTestContext{},
		consumers:         []*consumerTestContext{},
		running:           false,
		t:                 t,
		params:            predefinedTestParams[testName],
	}
}

func (s *ShapleQTestContext) WithBrokerTimeout(timeout int) *ShapleQTestContext {
	s.brokerTimeout = timeout
	return s
}

func (s *ShapleQTestContext) RunBrokers() *ShapleQTestContext {
	// Start brokers
	for index, port := range s.brokerPorts {
		dataDir := fmt.Sprintf("%s/data-test/broker-%d", common.DefaultHomeDir, index)
		logDir := fmt.Sprintf("%s/log-test/broker-%d", common.DefaultHomeDir, index)
		brokerContext := newBrokerTestContext(port, s.brokerTimeout, s.zkAddrs, dataDir, logDir)
		s.brokers = append(s.brokers, brokerContext)
		brokerContext.start()
	}

	Sleep(1) // wait for starting brokers..

	s.running = true
	return s
}

func (s *ShapleQTestContext) SetupTopics() *ShapleQTestContext {
	if s.params != nil {
		// set fragment offsets
		adminClient := s.CreateAdminClient()
		if err := adminClient.Connect(); err != nil {
			s.t.Fatal(err)
		}
		fragmentOffsets := make(map[uint32]uint64)
		if err := adminClient.CreateTopic(s.params.topic, s.params.topicDescription); err != nil {
			s.t.Fatal(err)
		}

		for i := 0; i < s.params.fragmentCount; i++ {
			fragment, err := adminClient.CreateFragment(s.params.topic)
			if err != nil {
				s.t.Fatal(err)
			}
			fragmentOffsets[fragment.Id] = 1 // set start offset with 1
		}
		s.params.fragmentOffsets = fragmentOffsets
	}
	return s
}

func (s *ShapleQTestContext) Terminate() {
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
		zkClient := zookeeper.NewZKQClient(s.zkAddrs, s.zkTimeoutMS, s.zkFlushIntervalMS)
		if err := zkClient.Connect(); err != nil {
			s.t.Fatal(err)
		}
		defer zkClient.Close()
		zkClient.RemoveAllPath()
	}
}

func (s *ShapleQTestContext) CreateAdminClient() *client.Admin {
	adminConfig := config2.NewAdminConfig()
	adminConfig.SetLogLevel(defaultLogLevel)
	adminConfig.SetServerAddresses([]string{fmt.Sprintf("127.0.0.1:%d", s.brokerPorts[0])})
	return client.NewAdmin(adminConfig)
}

func (s *ShapleQTestContext) AddProducerContext(nodeId string, topic string) *producerTestContext {
	ctx := newProducerTestContext(nodeId, topic)
	if err := ctx.start(); err != nil {
		s.t.Error(err)
	} else {
		s.producers = append(s.producers, ctx)
	}
	return ctx
}

func (s *ShapleQTestContext) AddConsumerContext(nodeId string, topic string, fragmentOffsets map[uint32]uint64) *consumerTestContext {
	ctx := newConsumerTestContext(nodeId, topic, fragmentOffsets)
	if err := ctx.start(); err != nil {
		s.t.Error(err)
	} else {
		s.consumers = append(s.consumers, ctx)
	}
	return ctx
}

func (s *ShapleQTestContext) TestParams() *TestParams {
	return s.params
}

// common methods
func Sleep(sec int) {
	time.Sleep(time.Duration(sec) * time.Second)
}

func getRecordsFromFile(fileName string) [][]byte {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var records [][]byte
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		data := []byte(scanner.Text())
		records = append(records, data)
	}
	return records
}

func contains(s [][]byte, e []byte) bool {
	for _, a := range s {
		if bytes.Compare(a, e) == 0 {
			return true
		}
	}
	return false
}

// test parameters
type records [][]byte
type TestParams struct {
	topic                 string
	topicDescription      string
	brokerCount           int
	producerCount         int
	consumerCount         int
	fragmentCount         int
	testRecords           []records // length of test-records should be equal to producerCount
	nodeId                string
	consumerBatchSize     uint32
	consumerFlushInterval uint32
	fragmentOffsets       map[uint32]uint64
}

var predefinedTestParams = map[string]*TestParams{
	"TestConnect": {
		topic:                 "topic1",
		brokerCount:           1,
		consumerCount:         1,
		producerCount:         1,
		fragmentCount:         1,
		testRecords:           []records{},
		consumerBatchSize:     1,
		consumerFlushInterval: 0,
	},
	"TestPubSub": {
		topic:         "topic2",
		brokerCount:   1,
		consumerCount: 1,
		producerCount: 1,
		fragmentCount: 1,
		testRecords: []records{
			{
				{'g', 'o', 'o', 'g', 'l', 'e'},
				{'p', 'a', 'u', 's', 't', 'q'},
				{'1', '2', '3', '4', '5', '6'},
			},
		},
		consumerBatchSize:     1,
		consumerFlushInterval: 0,
	},
	"TestMultiClient": {
		topic:         "topic3",
		brokerCount:   1,
		consumerCount: 5,
		producerCount: 3,
		fragmentCount: 1,
		testRecords: []records{
			getRecordsFromFile("data1.txt"),
			getRecordsFromFile("data2.txt"),
			getRecordsFromFile("data3.txt"),
		},
		consumerBatchSize:     1,
		consumerFlushInterval: 0,
	},
	"TestBatchedFetch": {
		topic:         "topic4",
		brokerCount:   1,
		consumerCount: 1,
		producerCount: 2,
		fragmentCount: 1,
		testRecords: []records{
			getRecordsFromFile("data1.txt"),
			getRecordsFromFile("data2.txt"),
		},
		consumerBatchSize:     32,
		consumerFlushInterval: 100,
	},

	// RPC tests
	"TestHeartBeat": {
		topic:            "rpc-topic1",
		topicDescription: "test-description1",
	},
	"TestCreateTopicAndFragment": {
		topic:            "rpc-topic2",
		topicDescription: "test-description2",
	},
	"TestDeleteTopicAndFragment": {
		topic:            "rpc-topic3",
		topicDescription: "test-description3",
	},
	"TestDescribeFragment": {
		topic:            "rpc-topic4",
		topicDescription: "test-description4",
	},
}
