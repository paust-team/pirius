package producer

import (
	"context"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"log"
	"time"
)

type SourceData struct {
	Topic string
	Data  []byte
}

type ResendableResponseData struct {
	sourceData SourceData
	responseCh chan client.ReceivedData
}

type Producer struct {
	ctx         		context.Context
	client				*client.Client
	sourceChannel     	chan SourceData
	publishing        	bool
	onReceiveResponse 	chan paustq_proto.PutResponse
}

func NewProducer(ctx context.Context, hostUrl string, timeout time.Duration) *Producer {
	c := client.NewClient(ctx, hostUrl, timeout, paustq_proto.SessionType_PUBLISHER)

	producer := &Producer{ctx: ctx, client: c, sourceChannel: make(chan SourceData), publishing: false, onReceiveResponse: nil}

	return producer
}

func (p *Producer) WithOnReceiveResponse(receiveCh chan paustq_proto.PutResponse) *Producer {
	p.onReceiveResponse = receiveCh
	return p
}

func (p *Producer) waitResponse(resendableData ResendableResponseData) {

	go p.client.Read(resendableData.responseCh, p.client.Timeout)

	select {
	case res := <-resendableData.responseCh:
		if res.Error != nil {
			log.Fatal("Error on read: timeout!")
		} else {
			putRespMsg := &paustq_proto.PutResponse{}
			err := message.UnPackTo(res.Data, putRespMsg)
			if err != nil {
				log.Fatal("Failed to parse data to PutResponse")
			} else {
				if p.onReceiveResponse != nil {
					p.onReceiveResponse <- *putRespMsg
				}
			}
		}

	case <- p.ctx.Done():
		return
	case <-time.After(time.Second * 10):
		log.Fatal("Receive PutResponse Timeout. Resend data..: ")
		p.sourceChannel <- resendableData.sourceData
		return
	}
}

func (p *Producer) startPublish() {
	if !p.publishing {
		return
	}
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
					resendableData := ResendableResponseData{sourceData: sourceData, responseCh: make(chan client.ReceivedData)}
					go p.waitResponse(resendableData)
				}
			}
		case <- p.ctx.Done():
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (p *Producer) Publish(topic string, data []byte) {
	if p.publishing == false {
		p.publishing = true
		go p.startPublish()
	}
	p.sourceChannel <- SourceData{topic, data}
}

func (p *Producer) Connect() error {
	return p.client.Connect()
}

func (p *Producer) Close() error {
	p.publishing = false
	_, cancel := context.WithCancel(p.ctx)
	cancel()
	return p.client.Close()
}
