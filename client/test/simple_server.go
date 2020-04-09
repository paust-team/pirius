package test

import (
	"fmt"
	"github.com/paust-team/paustq/client"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/proto"
	"net"
	"reflect"
	"time"
)

type TcpServer struct {
	Port      string
	endAccept chan bool
	endWrite  chan bool
	endRead   chan bool
	sink      chan<- TopicData
	source    <-chan []byte
}

type TopicData struct {
	Data  []byte
	Topic string
}

type Session struct {
	conn  net.Conn
	topic string
}

func NewSession(conn net.Conn) *Session {
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	return &Session{conn: conn}
}

func (sess *Session) SetTopic(topic string) {
	sess.topic = topic
}

func (sess *Session) Write(msgData []byte) error {
	data, err := message.Serialize(msgData)
	if err != nil {
		return err
	}
	_, err = sess.conn.Write(data)
	return err
}

func (sess *Session) Read(receiveCh chan<- client.ReceivedData) {

	var totalData []byte
	for {
		readBuffer := make([]byte, 1024)
		n, err := sess.conn.Read(readBuffer)
		if err != nil {
			if n > 0 {
				receiveCh <- client.ReceivedData{err, nil}
			}
		} else {

			totalData = append(totalData, readBuffer[:n]...)
			data, err := message.Deserialize(totalData)

			if err != nil {
				if data != nil {
					continue
				}
				receiveCh <- client.ReceivedData{err, nil}
				return
			}
			connReqMsg := &paustq_proto.ConnectRequest{}
			if err = message.UnPackTo(data, connReqMsg); err == nil {

				sess.topic = connReqMsg.TopicName

				connResMsg, err := message.NewConnectResponseMsgData(0)
				if err != nil {
					fmt.Println("Failed to create ConnectResponse message")
				}
				if err = sess.Write(connResMsg); err != nil {
					fmt.Println("Failed to write data to connection")
				}
				receiveCh <- client.ReceivedData{nil, nil}
			} else {
				receiveCh <- client.ReceivedData{nil, data}
			}
			return
		}
	}
}

func NewTcpServer(port string, sink chan<- TopicData, source <-chan []byte) *TcpServer {
	return &TcpServer{port, make(chan bool), make(chan bool), make(chan bool),
		sink, source}
}

func (server *TcpServer) accept(onAccepted chan net.Conn, listener net.Listener) {

	listener.(*net.TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
	conn, err := listener.Accept()

	if err != nil {
		onAccepted <- nil
	} else {
		onAccepted <- conn
	}
}

func (server *TcpServer) handleAccept(listener net.Listener) {

	onAccepted := make(chan net.Conn)

	for {
		go server.accept(onAccepted, listener)

		select {
		case conn := <-onAccepted:
			if conn != nil {
				go server.handleConnection(conn)
			}
		case <-server.endAccept:
			close(server.endAccept)
			return // runs into "defer listener.Close()"
		}
	}
}

func (server *TcpServer) handleWrite(sess *Session) {
	for {
		select {
		case data := <-server.source:
			if err := sess.Write(data); err != nil {
				fmt.Println("Failed to write data to connection")
			}
		case <-server.endWrite:
			close(server.endWrite)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (server *TcpServer) handleRead(sess *Session) {

	onReceiveResponse := make(chan client.ReceivedData)

	for {
		go sess.Read(onReceiveResponse)

		select {
		case <-server.endRead:
			close(server.endRead)
			return
		case res := <-onReceiveResponse:
			if res.Error != nil {
				fmt.Println(res.Error)
				break
			}
			if res.Data != nil && server.sink != nil {
				server.sink <- TopicData{Topic: sess.topic, Data: res.Data}
			}
		}
	}
}

func (server *TcpServer) handleConnection(conn net.Conn) {

	sess := NewSession(conn)

	go server.handleRead(sess)
	go server.handleWrite(sess)
}

func (server *TcpServer) StartListen() error {
	listener, err := net.Listen("tcp", server.Port)
	if err != nil {
		fmt.Println("Listen error: ", err)
		return err
	}

	go server.handleAccept(listener)
	return nil
}

func (server *TcpServer) Stop() {
	server.endAccept <- true
	server.endWrite <- true
	server.endRead <- true

	if server.sink != nil {
		close(server.sink)
	}
}

func contains(s interface{}, e interface{}) bool {
	arrV := reflect.ValueOf(s)

	if arrV.Kind() == reflect.Slice {
		for i := 0; i < arrV.Len(); i++ {
			if arrV.Index(i).Interface() == e {
				return true
			}
		}
	}

	return false
}
