package producer

import (
	"context"
	"fmt"
	"github.com/elon0823/paustq/client"
	"github.com/elon0823/paustq/message"
	"github.com/elon0823/paustq/proto"
	"log"
	"sync"
	"time"
)

type SourceData struct {
	Topic string
	Data  []byte
}

type ResendableResponseData struct {
	requestData []byte
	responseCh  chan client.ReceivedData
}

type Producer struct {
	ctx           context.Context
	client        *client.Client
	sourceChannel chan SourceData
	publishing    bool
	waitGroup     sync.WaitGroup
}

func NewProducer(ctx context.Context, hostUrl string, timeout time.Duration) *Producer {
	c := client.NewClient(ctx, hostUrl, timeout, paustq_proto.SessionType_PUBLISHER)

	producer := &Producer{ctx: ctx, client: c, sourceChannel: make(chan SourceData), publishing: false}

	return producer
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
			} else if putRespMsg.ErrorCode != 0 {
				log.Fatal("PutResponse has error code: ", putRespMsg.ErrorCode)
			}
			p.waitGroup.Done()
		}

	case <-p.ctx.Done():
		p.waitGroup.Done()
		return
	case <-time.After(time.Second * 10):
		fmt.Println("Wait Response Timeout.. Resend Put Request")
		close(resendableData.responseCh)
		resendableData := ResendableResponseData{requestData: resendableData.requestData, responseCh: make(chan client.ReceivedData)}
		go p.waitResponse(resendableData)
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
			requestData, err := message.NewPutRequestMsgData(sourceData.Topic, sourceData.Data)

			if err != nil {
				log.Fatal("Failed to create PutRequest message")
			} else {
				err := p.client.Write(requestData)
				if err != nil {
					log.Fatal(err)
				} else {
					resendableData := ResendableResponseData{requestData: requestData, responseCh: make(chan client.ReceivedData)}
					p.waitGroup.Add(1)
					go p.waitResponse(resendableData)
				}
			}
		case <-p.ctx.Done():
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

func (p *Producer) WaitAllPublishResponse() {
	if p.publishing {
		p.waitGroup.Wait()
	}
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
