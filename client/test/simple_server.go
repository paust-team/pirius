package test

import (
	"fmt"
	"github.com/elon0823/paustq/client"
	"net"
	"reflect"
	"time"
)

type TcpServer struct {
	Port      string
	endAccept chan bool
	endWrite  chan bool
	endRead   chan bool
	sink      chan<- []byte
	source    <-chan []byte
}

func NewTcpServer(port string, sink chan<- []byte, source <-chan []byte) *TcpServer {
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

func (server *TcpServer) write(data []byte, conn net.Conn) error {
	_, err := conn.Write(data)
	return err
}

func (server *TcpServer) read(receiveCh chan<- client.ReceivedData, conn net.Conn) {

	readBuffer := make([]byte, 1024)
	n, err := conn.Read(readBuffer)
	if err != nil {
		receiveCh <- client.ReceivedData{err, nil}
	} else {
		receiveCh <- client.ReceivedData{err, readBuffer[0:n]}
	}
}

func (server *TcpServer) handleAccept(listener net.Listener) {

	onAccepted := make(chan net.Conn)

	for {
		go server.accept(onAccepted, listener)

		select {
		case conn := <- onAccepted:
			if conn != nil {
				go server.handleConnection(conn)
			}
		case <-server.endAccept:
			close(server.endAccept)
			return // runs into "defer listener.Close()"
		}
	}
}

func (server *TcpServer) handleWrite(conn net.Conn) {
	for {
		select {
		case data := <-server.source:
			if err := server.write(data, conn); err != nil {
				fmt.Println("Failed to write data to connection")
			}
		case <-server.endWrite:
			close(server.endWrite)
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (server *TcpServer) handleRead(conn net.Conn) {

	onReceiveResponse := make(chan client.ReceivedData)

	for {
		go server.read(onReceiveResponse, conn)

		select {
		case <-server.endRead:
			close(server.endRead)
			return
		case res := <- onReceiveResponse:
			if res.Error != nil {
				break
			}
			if server.sink != nil {
				server.sink <- res.Data
			}
		}
	}
}

func (server *TcpServer) handleConnection(conn net.Conn) {

	conn.SetDeadline(time.Now().Add(3 * time.Second))
	go server.handleRead(conn)
	go server.handleWrite(conn)
}

func (server *TcpServer) StartListen() error {
	fmt.Println("Start Listen")
	listener, err := net.Listen("tcp", server.Port)
	if err != nil {
		fmt.Println("Listen error: ", err)
		return err
	}

	go server.handleAccept(listener)
	return nil
}

func (server *TcpServer) Stop() {
	fmt.Println("Stop Server")
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