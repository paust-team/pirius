package rpc

import (
	"errors"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"log"
	"time"
)

// TODO:: Temporary structure
type Session struct {
	StreamReaderWriter     	*common.StreamReaderWriter
	Topic       			string
	Offset      			uint64
	SessionType 			paustqproto.SessionType
	Connected   			bool
}

type StreamServiceServer struct {
	DB  		*storage.QRocksDB
}

func NewStreamServiceServer(db *storage.QRocksDB) *StreamServiceServer {
	return &StreamServiceServer{DB: db}
}

func (s *StreamServiceServer) FlowStream(stream paustqproto.StreamService_FlowStreamServer) error {
	sess := &Session{StreamReaderWriter: common.NewStreamReaderWriter(stream)}
	log.Println("start stream")
	for {
		msg, err := sess.StreamReaderWriter.RecvMsg()
		if err != nil {
			return err
		}
		if msg == nil { // end stream
			break
		}
		finishStream, err := s.HandleFlow(sess, msg)
		if err != nil {
			return err
		}

		if finishStream {
			break
		}
	}

	log.Println("end stream")
	return nil
}

// TODO:: will be replaced using pipeline package
func (s *StreamServiceServer) HandleFlow(session *Session, receivedMsg *message.QMessage) (bool, error) {

	if receivedMsg.Is(&paustqproto.ConnectRequest{}) { // connect
		log.Println("Received connect request")
		connectRequestMsg := &paustqproto.ConnectRequest{}
		if err := receivedMsg.UnpackTo(connectRequestMsg); err != nil {
			return false, err
		}

		session.Topic = connectRequestMsg.TopicName
		session.SessionType = connectRequestMsg.SessionType
		session.Connected = true

		respMsg, err := message.NewQMessageWithConnectResponse()
		if err != nil {
			return false, err
		}
		if err := session.StreamReaderWriter.SendMsg(respMsg); err != nil {
			return false, err
		}
	} else if receivedMsg.Is(&paustqproto.PutRequest{}) { // put
		log.Println("Received put request")
		if !session.Connected {
			return false, errors.New("initial connect request required")
		}

		if session.SessionType != paustqproto.SessionType_PUBLISHER {
			return false, errors.New("session type `publisher` required to operate `put`")
		}

		putRequestMsg := &paustqproto.PutRequest{}
		if err := receivedMsg.UnpackTo(putRequestMsg); err != nil {
			return false, err
		}

		if err := s.DB.PutRecord(session.Topic, session.Offset, putRequestMsg.Data); err != nil {
			return false, err
		}
		// TODO:: This works on single producer only. To support multiple producer, manage topic offset globally.
		log.Printf("put record with offset %d", session.Offset)
		session.Offset++

		respMsg, err := message.NewQMessageWithPutResponse()
		if err != nil {
			return false, err
		}
		if err := session.StreamReaderWriter.SendMsg(respMsg); err != nil {
			return false, err
		}

	} else if receivedMsg.Is(&paustqproto.FetchRequest{}) { //fetch
		log.Println("Received fetch request")
		if !session.Connected {
			return false, errors.New("initial connect request required")
		}

		if session.SessionType != paustqproto.SessionType_SUBSCRIBER {
			return false, errors.New("session type `subscriber` required to operate `fetch`")
		}

		fetchRequestMsg := &paustqproto.FetchRequest{}
		if err := receivedMsg.UnpackTo(fetchRequestMsg); err != nil {
			return false, err
		}

		session.Offset = fetchRequestMsg.StartOffset
		counter := 0

		for {
			result, err := s.DB.GetRecord(session.Topic, session.Offset)
			log.Printf("get record with offset %d", session.Offset)
			if err != nil {
				return false, err
			}

			// TODO:: Wait for topic with cv.wait and should check whether any producer publishing to topic exists or not
			if !result.Exists() {
				// for testing, wait 5 sec to new record
				if counter == 5 {
					return true, nil// end stream
				}
				counter++
				time.Sleep(1*time.Second)
				continue
			}

			counter = 0
			session.Offset++

			respMsg, err := message.NewQMessageWithFetchResponse(result.Data(), session.Offset)
			if err != nil {
				return false, err
			}
			if err = session.StreamReaderWriter.SendMsg(respMsg); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}