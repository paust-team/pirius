package client

import (
	"errors"
	"github.com/golang/protobuf/proto"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/network"
	paustqproto "github.com/paust-team/paustq/proto"
	"net"
	"sync"
)

type Admin struct {
	socket *network.Socket
	brokerAddr string
	timeout uint
	mu sync.Mutex
	connected bool
	logger *logger.QLogger
}

func NewAdmin(brokerAddr string) *Admin {
	var defaultTimeout uint = 3 // second
	l := logger.NewQLogger("Admin-client", logger.Info)

	return &Admin{
		timeout:    defaultTimeout,
		connected:  false,
		brokerAddr: brokerAddr,
		logger:     l,
		mu: sync.Mutex{},
	}
}

func (client *Admin) WithTimeout(timeout uint) *Admin {
	client.timeout = timeout
	return client
}

func (client *Admin) WithLogLevel(level logger.LogLevel) *Admin {
	client.logger.SetLogLevel(level)
	return client
}

func (client *Admin) Connect() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	conn, err := net.Dial("tcp", client.brokerAddr)
	if err != nil {
		client.logger.Error(err)
		return err
	}

	client.socket = network.NewSocket(conn, client.timeout, client.timeout)
	client.connected = true
	return nil
}

func (client *Admin) Close() {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.connected = false
	client.socket.Close()
}

func (client *Admin) callAndUnpackTo(requestMsg proto.Message, responseMsg proto.Message) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.connected {
		return errors.New("admin client is not connected to broker")
	}

	sendMsg, err := message.NewQMessageFromMsg(requestMsg)
	if err != nil {
		client.logger.Error(err)
		return err
	}
	if err := client.socket.Write(sendMsg); err != nil {
		client.logger.Error(err)
		return err
	}

	receivedMsg, err := client.socket.Read()
	if err != nil {
		client.logger.Error(err)
		return err
	}

	if err := receivedMsg.UnpackTo(responseMsg); err != nil {
		err = errors.New("unhandled error occurred")
		client.logger.Error(err)
		return err
	}
	return nil
}

func (client *Admin) CreateTopic(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {

	request := message.NewCreateTopicRequestMsg(topicName, topicMeta, numPartitions, replicationFactor)
	response := &paustqproto.CreateTopicResponse{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return err
	}

	if response.ErrorCode != 0 {
		client.logger.Error()
		return err
	}
	return nil
}

func (client *Admin) DeleteTopic(topicName string) error {

	request := message.NewDeleteTopicRequestMsg(topicName)
	response := &paustqproto.DeleteTopicResponse{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return err
	}

	if response.ErrorCode != 0 {
		client.logger.Error()
		return err
	}
	return nil
}

func (client *Admin) DescribeTopic(topicName string) (*paustqproto.DescribeTopicResponse, error) {

	request := message.NewDescribeTopicRequestMsg(topicName)
	response := &paustqproto.DescribeTopicResponse{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return nil, err
	}

	if response.ErrorCode != 0 {
		client.logger.Error()
		return nil, err
	}
	return response, nil
}

func (client *Admin) ListTopic() (*paustqproto.ListTopicResponse, error) {

	request := message.NewListTopicRequestMsg()
	response := &paustqproto.ListTopicResponse{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return nil, err
	}

	if response.ErrorCode != 0 {
		client.logger.Error()
		return nil, err
	}
	return response, nil
}

func (client *Admin) Heartbeat(msg string, brokerId uint64) (*paustqproto.Pong, error) {

	request := message.NewPingMsg(msg, brokerId)
	response := &paustqproto.Pong{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return nil, err
	}

	return response, nil
}
