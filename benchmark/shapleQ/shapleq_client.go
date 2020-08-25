package shapleQ

import (
	"encoding/csv"
	"github.com/paust-team/shapleq/client"
	"github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"log"
	"os"
	"runtime"
	"time"
)

type BenchShapleQClient struct {
	admin  *client.Admin
	config *config.ClientConfigBase
}

func NewBenchShapleQClient(brokerHost string, brokerPort uint, timeout time.Duration) *BenchShapleQClient {
	clientConfig := config.NewClientConfigBase()
	clientConfig.SetLogLevel(logger.Error)
	clientConfig.SetBrokerHost(brokerHost)
	clientConfig.SetBrokerPort(brokerPort)
	clientConfig.SetTimeout(int(timeout.Milliseconds()))

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

	if err := s.admin.CreateTopic(topic, "", 1, 1); err != nil {
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

func (s *BenchShapleQClient) RunProducer(id int, topic string, filePath string, numData int) int64 {

	producer := client.NewProducer(&config.ProducerConfig{s.config}, topic)

	if err := producer.Connect(); err != nil {
		log.Fatalln(err)
	}

	defer producer.Close()

	publishCh := make(chan []byte)
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

	startTimestamp := time.Now().UnixNano() / 1000000
	for i, record := range records {
		if _, err := producer.Publish([]byte(record[0])); err != nil {
			log.Fatalln(err)
		}
		if i+1 == numData {
			break
		}
	}

	return time.Now().UnixNano()/1000000 - startTimestamp
}

func (s *BenchShapleQClient) RunConsumer(id int, topic string, numData int) int64 {

	consumer := client.NewConsumer(&config.ConsumerConfig{s.config}, topic)

	if err := consumer.Connect(); err != nil {
		log.Fatalln(err)
	}
	defer consumer.Close()

	subscribeCh, errCh, err := consumer.Subscribe(0)
	if err != nil {
		log.Fatalln(err)
	}

	var startTimestamp int64 = 0
	receivedCount := 0
	startTimestamp = time.Now().UnixNano() / 1000000
	for {
		select {
		case _, ok := <-subscribeCh:
			if ok {
				receivedCount++
				if numData == receivedCount {
					return time.Now().UnixNano()/1000000 - startTimestamp
				}
			} else {
				log.Fatalln("not enough data")
			}

		case err := <-errCh:
			log.Fatalln(err)
		}
		runtime.Gosched()
	}

	return time.Now().UnixNano()/1000000 - startTimestamp
}
