package kafka

import (
	"context"
	"encoding/csv"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type BenchKafkaClient struct {
	admin      *kafka.AdminClient
	timeout    time.Duration
	brokerHost string
}

func NewBenchKafkaClient(brokerHost string, timeout time.Duration) *BenchKafkaClient {
	return &BenchKafkaClient{timeout: timeout, brokerHost: brokerHost}
}

func (k *BenchKafkaClient) setupAdminClient() {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": k.brokerHost,
	})
	if err != nil {
		panic(err)
	}
	k.admin = admin
}
func (k *BenchKafkaClient) CreateTopic(topic string) {
	if k.admin == nil {
		k.setupAdminClient()
	}
	if _, err := k.admin.CreateTopics(context.Background(),
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1}},
		kafka.SetAdminOperationTimeout(k.timeout)); err != nil {

		panic(err)
	}
}

func (k *BenchKafkaClient) DeleteTopic(topic string) {
	if k.admin == nil {
		k.setupAdminClient()
	}
	if _, err := k.admin.DeleteTopics(context.Background(), []string{topic},
		kafka.SetAdminOperationTimeout(k.timeout)); err != nil {
		panic(err)
	}
}

func (k *BenchKafkaClient) RunProducer(id int, topic string, filePath string, numData int) (startTimestamp int64) {

	p, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":                     k.brokerHost,
			"batch.size":                            1,
			"linger.ms":                             0,
			"acks":                                  1,
			"retries":                               5,
			"max.in.flight.requests.per.connection": 1,
			"request.timeout.ms":                    k.timeout,
		})

	if err != nil {
		log.Fatalln(err)
	}

	defer p.Close()

	testFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(testFile)
	}
	defer testFile.Close()

	reader := csv.NewReader(testFile)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()

	deliveryChan := make(chan kafka.Event)

	startTimestamp = time.Now().UnixNano() / 1000000

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		receivedCount := 0
		for {
			select {
			case event := <-deliveryChan:
				m := event.(*kafka.Message)

				if m.TopicPartition.Error != nil {
					log.Fatalln(m.TopicPartition.Error)
				}
				receivedCount++
				if receivedCount == numData {
					return
				}
			}
			runtime.Gosched()
		}
	}()

	for _, record := range records {
		sendRecord := []byte(record[0])
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          sendRecord,
		}, deliveryChan)

		if err != nil {
			log.Fatalln(err)
		}
		runtime.Gosched()
	}

	wg.Wait()
	return
}

func (k *BenchKafkaClient) RunConsumer(id int, topic string, numData int) (endTimestamp int64) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     k.brokerHost,
		"broker.address.family": "v4",
		"group.id":              fmt.Sprintf("mygroup-%d", id),
		"session.timeout.ms":    100000,
		"request.timeout.ms":    k.timeout,
		"auto.offset.reset":     "earliest",
	})

	if err != nil {
		log.Fatalln(err)
	}

	defer c.Close()

	err = c.Subscribe(topic, nil)

	if err != nil {
		log.Fatalln(err)
	}

	receivedCount := 0

	for {
		_, err := c.ReadMessage(k.timeout)
		if err != nil {
			log.Fatalln(err)
		} else {
			receivedCount++
			if numData == receivedCount {
				endTimestamp = time.Now().UnixNano() / 1000000
				return
			}
		}
		runtime.Gosched()
	}

	return
}
