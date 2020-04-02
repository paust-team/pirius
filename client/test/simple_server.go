package test

import (
	"fmt"
	"net"
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

func (server *TcpServer) accept(listener net.Listener) {

	for {
		select {
		case <-server.endAccept:
			close(server.endAccept)
			return // runs into "defer listener.Close()"
		default: // nothing to do
		}

		listener.(*net.TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go server.handleConnection(conn)
	}
}

func (server *TcpServer) handleWrite(conn net.Conn) {
	for {
		select {
		case data := <-server.source:
			_, err := conn.Write(data)
			if err != nil {
				fmt.Println("Failed to write data to connection")
			}
		case <-server.endWrite:
			close(server.endWrite)
			return
		case <-time.After(time.Second * 2):
			break
		}
	}
}

func (server *TcpServer) handleRead(conn net.Conn) {

	for {
		select {
		case <-server.endRead:
			close(server.endRead)
			return
		default:
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)

		if err != nil {
			if n > 0 {
				fmt.Println("Failed to read data from connection")
			}
			continue
		}
		if server.sink != nil {
			server.sink <- buf[0:n]
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

	go server.accept(listener)
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
