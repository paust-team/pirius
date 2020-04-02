package producer

import (
	"context"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"log"
	"time"
)

type SourceData struct {
	Topic string
	Data []byte
}

type ResendableResponseData struct {
	sourceData			SourceData
	responseCh 			chan client.ReceivedData
}

type Producer struct {
	client 				*client.Client
	sourceChannel		chan SourceData
	publishing			bool
}

func NewProducer(hostUrl string, timeout time.Duration) *Producer {
	ctx := context.Background()
	c := client.NewClient(ctx, hostUrl, timeout, paustq_proto.SessionType_PUBLISHER)

	producer := &Producer{client: c, sourceChannel: make(chan SourceData), publishing: false}

	return producer
}

func (p *Producer) waitResponse(resendableData ResendableResponseData) {

	go p.client.Read(resendableData.responseCh)

	select {
	case res := <- resendableData.responseCh:
		if res.Error != nil {
			log.Fatal("Error on read")
			break
		}
		putRespMsg, err := message.ParsePutResponseMsg(res.Data)
		if err != nil {
			log.Fatal("Failed to parse data to PutResponse")
		} else if putRespMsg.ErrorCode != 0{
			log.Fatal("PutResponse Error: ", putRespMsg.ErrorCode)
		} else {
			fmt.Println("Success writing Topic: ", putRespMsg.TopicName)
		}
	case <- time.After(time.Second * p.client.Timeout):
		log.Fatal("Receive PutResponse Timeout. Resend data..: ")
		p.sourceChannel <- resendableData.sourceData
		return
	}
}

func (p *Producer) startPublish() {
	for {
		select {
		case sourceData := <-p.sourceChannel:

			protoMsg, protoErr := message.NewPutRequestMsg(sourceData.Topic, sourceData.Data)

			if protoErr != nil {
				log.Fatal("Failed to create PutRequest message")
			} else {
				err := p.client.Write(protoMsg)
				if err != nil {
					log.Fatal(err)
				} else {
					resendableData := ResendableResponseData{sourceData:sourceData, responseCh: make(chan client.ReceivedData)}
					go p.waitResponse(resendableData)
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