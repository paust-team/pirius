package client

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/paust-team/shapleq/client/config"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	shapleqproto "github.com/paust-team/shapleq/proto/pb"
	"net"
	"sync"
)

type Admin struct {
	socket *network.Socket
	config *config.AdminConfig
	sync.Mutex
	connected bool
	logger    *logger.QLogger
}

func NewAdmin(config *config.AdminConfig) *Admin {

	l := logger.NewQLogger("Admin-client", config.LogLevel())
	return &Admin{
		Mutex:     sync.Mutex{},
		config:    config,
		connected: false,
		logger:    l,
	}
}

func (a *Admin) WithConnection(socket *network.Socket) *Admin {
	a.socket = socket
	a.Lock()
	a.connected = true
	a.Unlock()
	return a
}

func (a *Admin) Connect() error {
	brokerAddresses := a.config.ServerAddresses()
	if len(brokerAddresses) > 0 {
		for _, brokerAddress := range brokerAddresses {
			conn, err := net.Dial("tcp", brokerAddress)
			if err != nil {
				a.logger.Error(err)
				continue
			}

			a.socket = network.NewSocket(conn, a.config.BrokerTimeout(), a.config.BrokerTimeout())
			a.Lock()
			a.connected = true
			a.Unlock()
			return nil
		}
	} else {
		err := pqerror.ConfigValueNotSetError{Key: "bootstrap.servers"}
		a.logger.Error(err)
		return err
	}

	err := pqerror.DialFailedError{Addr: fmt.Sprintf("%s", brokerAddresses)}
	a.logger.Error(err)
	return err
}

func (a *Admin) Close() {
	a.Lock()
	a.connected = false
	a.Unlock()
	a.socket.Close()
}

func (a *Admin) callAndUnpackTo(requestMsg proto.Message, responseMsg proto.Message) error {

	a.Lock()
	if !a.connected {
		a.Unlock()
		return errors.New("admin client is not connected to broker")
	}
	a.Unlock()

	sendMsg, err := message.NewQMessageFromMsg(message.TRANSACTION, requestMsg)
	if err != nil {
		a.logger.Error(err)
		return err
	}
	if err := a.socket.Write(sendMsg); err != nil {
		a.logger.Error(err)
		return err
	}

	receivedMsg, err := a.socket.Read()
	if err != nil {
		a.logger.Error(err)
		return err
	}

	if _, err := receivedMsg.UnpackTo(responseMsg); err != nil {
		err = errors.New("unhandled error occurred")
		a.logger.Error(err)
		return err
	}
	return nil
}

// topic RPCs

func (a *Admin) CreateTopic(topicName string, topicDescription string) error {

	request := message.NewCreateTopicRequestMsg(topicName, topicDescription)
	response := &shapleqproto.CreateTopicResponse{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return err
	}

	if response.ErrorCode != 0 {
		a.logger.Error(response.ErrorMessage)
		return err
	}
	return nil
}

func (a *Admin) DeleteTopic(topicName string) error {

	request := message.NewDeleteTopicRequestMsg(topicName)
	response := &shapleqproto.DeleteTopicResponse{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return err
	}

	if response.ErrorCode != 0 {
		a.logger.Error(response.ErrorMessage)
		return err
	}
	return nil
}

func (a *Admin) DescribeTopic(topicName string) (*shapleqproto.DescribeTopicResponse, error) {

	request := message.NewDescribeTopicRequestMsg(topicName)
	response := &shapleqproto.DescribeTopicResponse{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return nil, err
	}

	if response.ErrorCode != 0 {
		a.logger.Error(response.ErrorMessage)
		return nil, err
	}
	return response, nil
}

func (a *Admin) ListTopic() (*shapleqproto.ListTopicResponse, error) {

	request := message.NewListTopicRequestMsg()
	response := &shapleqproto.ListTopicResponse{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return nil, err
	}

	if response.ErrorCode != 0 {
		a.logger.Error(response.ErrorMessage)
		return nil, err
	}
	return response, nil
}

// fragment RPCs

func (a *Admin) CreateFragment(topicName string) (*shapleqproto.Fragment, error) {

	request := message.NewCreateTopicFragmentRequestMsg(topicName)
	response := &shapleqproto.CreateFragmentResponse{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return nil, err
	}

	if response.ErrorCode != 0 {
		a.logger.Error(response.ErrorMessage)
		return nil, err
	}
	return response.Fragment, nil
}

// connection RPCs

func (a *Admin) Heartbeat(msg string, brokerId uint64) (*shapleqproto.Pong, error) {

	request := message.NewPingMsg(msg, brokerId)
	response := &shapleqproto.Pong{}

	err := a.callAndUnpackTo(request, response)
	if err != nil {
		a.logger.Error(err)
		return nil, err
	}

	return response, nil
}
