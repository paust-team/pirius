package shapleQ

import (
	"encoding/csv"
	"fmt"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type BenchShapleQClient struct {
	admin  *client.Admin
	config *config.ClientConfigBase
}

func NewBenchShapleQClient(brokerHost string, brokerPort uint, timeout time.Duration) *BenchShapleQClient {
	clientConfig := config.NewClientConfigBase()
	clientConfig.SetLogLevel(logger.Error)
	clientConfig.SetServerAddresses([]string{fmt.Sprintf("%s:%d", brokerHost, brokerPort)})
	clientConfig.SetBrokerTimeout(int(timeout.Milliseconds()))

	return &BenchShapleQClient{
		config: clientConfig,
	}
}

func (s *BenchShapleQClient) setupAdminClient() {
	admin := client.NewAdmin(&config.AdminConfig{s.config})
	s.admin = admin
}

func (s *BenchShapleQClient) CreateTopic(topic string) {
	if s.admin == nil {
		s.setupAdminClient()
	}
	if err := s.admin.Connect(); err != nil {
		panic(err)
	}
	defer s.admin.Close()

	if err := s.admin.CreateTopic(topic, ""); err != nil {
		panic(err)
	}
}

func (s *BenchShapleQClient) DeleteTopic(topic string) {
	if s.admin == nil {
		s.setupAdminClient()
	}
	if err := s.admin.Connect(); err != nil {
		panic(err)
	}
	defer s.admin.Close()

	if err := s.admin.DeleteTopic(topic); err != nil {
		panic(err)
	}
}

func (s *BenchShapleQClient) RunProducer(id string, topic string, filePath string, numData int) (startTimestamp, endTimestamp int64) {

	producer := client.NewProducer(&config.ProducerConfig{s.config}, []string{
		topic,
	})

	if err := producer.Connect(); err != nil {
		log.Fatalln(err)
	}

	defer producer.Close()

	publishCh := make(chan *client.PublishData)
	defer close(publishCh)

	testFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer testFile.Close()

	reader := csv.NewReader(testFile)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()

	wg := sync.WaitGroup{}
	wg.Add(1)

	partitionCh, errCh, err := producer.AsyncPublish(publishCh)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		defer wg.Done()
		receivedCount := 0
		for {
			select {
			case <-partitionCh:
				receivedCount++
				if receivedCount == numData {
					return
				}
			case err := <-errCh:
				log.Fatal(err)
			}
			runtime.Gosched()
		}
	}()

	startTimestamp = time.Now().UnixNano() / 1000000
	for i, record := range records {
		publishCh <- &client.PublishData{
			Data:   []byte(record[0]),
			NodeId: id,
			SeqNum: uint64(i),
		}
		if i+1 == numData {
			break
		}
		runtime.Gosched()
	}

	wg.Wait()
	endTimestamp = time.Now().UnixNano() / 1000000
	return
}

func (s *BenchShapleQClient) RunConsumer(id string, topic string, numData int) (startTimestamp, endTimestamp int64) {

	consumer := client.NewConsumer(&config.ConsumerConfig{ClientConfigBase: s.config}, []*common.Topic{
		common.NewTopic(topic, []uint32{0}, 1, 1),
	})

	if err := consumer.Connect(); err != nil {
		log.Fatalln(err)
	}
	defer consumer.Close()

	subscribeCh, errCh, err := consumer.Subscribe()
	if err != nil {
		log.Fatalln(err)
	}

	receivedCount := 0
	startTimestamp = time.Now().UnixNano() / 1000000
	for {
		select {
		case _, ok := <-subscribeCh:
			if ok {
				receivedCount++
				if numData == receivedCount {
					endTimestamp = time.Now().UnixNano() / 1000000
					return
				}

			} else {
				log.Fatalln("not enough data")
			}

		case err := <-errCh:
			log.Fatalln(err)
		}
		runtime.Gosched()
	}

	return
}
