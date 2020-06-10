package client

import (
	"errors"
	"github.com/golang/protobuf/proto"
	logger "github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/network"
	paustqproto "github.com/paust-team/paustq/proto"
	"net"
)

type AdminClient struct {
	socket *network.Socket
	brokerAddr string
	timeout uint
	Connected bool
	logger *logger.QLogger
}

func NewAdminClient(brokerAddr string) *AdminClient {
	var defaultTimeout uint = 3 // second
	l := logger.NewQLogger("Admin-client", logger.Info)

	return &AdminClient{
		timeout:    defaultTimeout,
		Connected:  false,
		brokerAddr: brokerAddr,
		logger:     l,
	}
}

func (client *AdminClient) WithTimeout(timeout uint) *AdminClient {
	client.timeout = timeout
	return client
}

func (client *AdminClient) WithLogLevel(level logger.LogLevel) *AdminClient {
	client.logger.SetLogLevel(level)
	return client
}

func (client *AdminClient) Connect() error {
	conn, err := net.Dial("tcp", client.brokerAddr)
	if err != nil {
		client.logger.Error(err)
		return err
	}

	client.socket = network.NewSocket(conn, client.timeout, client.timeout)
	client.Connected = true
	return nil
}

func (client *AdminClient) Close() {
	client.Connected = false
	client.socket.Close()
}

func (client *AdminClient) callAndUnpackTo(requestMsg proto.Message, responseMsg proto.Message) error {

	if !client.Connected {
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

func (client *AdminClient) CreateTopic(topicName string, topicMeta string, numPartitions uint32, replicationFactor uint32) error {

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

func (client *AdminClient) DeleteTopic(topicName string) error {

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

func (client *AdminClient) DescribeTopic(topicName string) (*paustqproto.DescribeTopicResponse, error) {

	if !client.Connected {
		return nil, errors.New("admin client is not connected to broker")
	}

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

func (client *AdminClient) ListTopic() (*paustqproto.ListTopicResponse, error) {

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

func (client *AdminClient) Heartbeat(msg string, brokerId uint64) (*paustqproto.Pong, error) {

	request := message.NewPingMsg(msg, brokerId)
	response := &paustqproto.Pong{}

	err := client.callAndUnpackTo(request, response)
	if err != nil {
		client.logger.Error(err)
		return nil, err
	}

	return response, nil
}
