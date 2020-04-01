package producer

import (
	"context"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/paustqpb"
	"log"
	"time"
)

type SourceData struct {
	Topic string
	Data []byte
}

type Producer struct {
	client 			*client.Client
	sourceChannel	chan SourceData
	publishing		bool
}

func NewProducer(hostUrl string, timeout time.Duration) *Producer {
	ctx := context.Background()
	c := client.NewClient(ctx, hostUrl, timeout)
	producer := &Producer{client: c, sourceChannel: make(chan SourceData), publishing: false}

	return producer
}

func (p *Producer) receiveResult(ch chan client.ResultData) {
	select {
	case res := <- ch:
		if res.Error != nil {
			log.Fatal("Error on read")
			break
		}
		putRespMsg, err := paustqpb.ParsePutResponseMsg(res.Data)
		if err != nil {
			log.Fatal("Failed to parse data to PutResponse")
		} else if putRespMsg.ErrorCode != 0{
			log.Fatal("PutResponse Error: ", putRespMsg.ErrorCode)
		} else {
			fmt.Println("Success writing Topic: ", putRespMsg.TopicName)
		}
	}
}

func (p *Producer) startPublish() {
	for {
		select {
		case sourceData := <-p.sourceChannel:

			protoMsg, protoErr := paustqpb.NewPutRequestMsg(sourceData.Topic, sourceData.Data)

			if protoErr != nil {
				log.Fatal("Error to creating PutRequest message")
			} else {
				err := p.client.Write(protoMsg)
				if err != nil {
					log.Fatal(err)
				} else {
					onReceiveResponse := make(chan client.ResultData)
					go p.receiveResult(onReceiveResponse)
					go p.client.Read(onReceiveResponse)
				}
			}
		case <- p.client.Ctx.Done():
			return
		}
	}
}

func (p *Producer) Publish(topic string, data []byte) {
	if p.publishing == false {
		p.publishing = true
		p.startPublish()
	}
	p.sourceChannel <- SourceData{topic, data}
}

func (p *Producer) Connect() error {
	return p.client.Connect()
}

func (p *Producer) Close() error {
	p.publishing = false
	return p.client.Close()
}