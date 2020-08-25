package kafka

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"runtime"
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

func (k *BenchKafkaClient) RunProducer(id int, topic string, filePath string, numData int) int64 {

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

	startTimestamp := time.Now().UnixNano() / 1000000

	for i, record := range records {
		sendRecord := []byte(record[0])
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          sendRecord,
		}, deliveryChan)

		if err != nil {
			log.Fatalln(err)
		}

		event := <-deliveryChan
		m := event.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Fatalln(m.TopicPartition.Error)
		}

		if bytes.Compare(m.Value, sendRecord) != 0 {
			log.Fatalln("record not matched")
		}
		if i+1 == numData {
			break
		}
	}

	return time.Now().UnixNano()/1000000 - startTimestamp
}

func (k *BenchKafkaClient) RunConsumer(id int, topic string, numData int) int64 {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     k.brokerHost,
		"broker.address.family": "v4",
		"group.id":              fmt.Sprintf("mygroup-%d", id),
		"session.timeout.ms":    100000,
		"request.timeout.ms":    k.timeout,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    false,
	})

	if err != nil {
		log.Fatalln(err)
	}

	defer c.Close()

	err = c.Subscribe(topic, nil)

	if err != nil {
		log.Fatalln(err)
	}

	var startTimestamp int64 = 0
	receivedCount := 0
	startTimestamp = time.Now().UnixNano() / 1000000

	for {
		_, err := c.ReadMessage(k.timeout)
		if err != nil {
			log.Fatalln(err)
		} else {
			c.Commit()
			receivedCount++
			if numData == receivedCount {
				return time.Now().UnixNano()/1000000 - startTimestamp
			}
		}
		runtime.Gosched()
	}

	return time.Now().UnixNano()/1000000 - startTimestamp
}
