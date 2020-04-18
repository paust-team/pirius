package rpc

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/message"
	paustqproto "github.com/paust-team/paustq/proto"
	"io"
	"time"
)

// TODO:: Temporary structure
type Session struct {
	Stream      paustqproto.StreamService_FlowStreamServer
	Topic       string
	Offset      uint64
	SessionType paustqproto.SessionType
	Connected   bool
}

type StreamServiceServer struct {
	DB  		*storage.QRocksDB
}

func (sess *Session) Send(msg *any.Any) error {
	responseBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return sess.Stream.Send(&paustqproto.StreamResponse{Magic: -1, ResponseBytes: responseBytes})
}

func NewStreamServiceServer(db *storage.QRocksDB) *StreamServiceServer {
	return &StreamServiceServer{DB: db}
}

func (s *StreamServiceServer) FlowStream(stream paustqproto.StreamService_FlowStreamServer) error {

	sess := &Session{Stream: stream}
	var totalData []byte
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		totalData = append(totalData, in.RequestBytes...)
		data, err := message.Deserialize(totalData)

		if err != nil {
			if data != nil { // Wait for rest chunks
				continue
			}
			return err
		}

		anyMsg := &any.Any{}
		if err := proto.Unmarshal(data, anyMsg); err != nil {
			return err
		}

		if err = s.HandleFlow(sess, anyMsg); err != nil {
			return err
		}
	}
}

// TODO:: will be replaced using pipeline package
func (s *StreamServiceServer) HandleFlow(session *Session, requestMsg *any.Any) error {

	if ptypes.Is(requestMsg, &paustqproto.ConnectRequest{}) { // connect

		connectRequestMsg := &paustqproto.ConnectRequest{}
		if err := ptypes.UnmarshalAny(requestMsg, connectRequestMsg); err != nil {
			return err
		}

		session.Topic = connectRequestMsg.TopicName
		session.SessionType = connectRequestMsg.SessionType
		session.Connected = true

		respMsg := message.NewConnectResponseMsg()
		anyMsg, err := ptypes.MarshalAny(respMsg)
		if err != nil {
			if err = session.Send(anyMsg); err != nil {
				return err
			}
		}
	} else if ptypes.Is(requestMsg, &paustqproto.PutRequest{}) { // put

		if !session.Connected {
			return errors.New("initial connect request required")
		}

		if session.SessionType != paustqproto.SessionType_PUBLISHER {
			return errors.New("session type `publisher` required to operate `put`")
		}

		putRequestMsg := &paustqproto.PutRequest{}
		if err := ptypes.UnmarshalAny(requestMsg, putRequestMsg); err != nil {
			return err
		}

		if err := s.DB.PutRecord(session.Topic, session.Offset, putRequestMsg.Data); err != nil {
			return err
		}
		// TODO:: This works on single producer only. To support multiple producer, manage topic offset globally.
		session.Offset++

		respMsg := message.NewPutResponseMsg()
		anyMsg, err := ptypes.MarshalAny(respMsg)
		if err != nil {
			if err = session.Send(anyMsg); err != nil {
				return err
			}
		}

	} else if ptypes.Is(requestMsg, &paustqproto.FetchRequest{}) { //fetch

		if !session.Connected {
			return errors.New("initial connect request required")
		}

		if session.SessionType != paustqproto.SessionType_SUBSCRIBER {
			return errors.New("session type `subscriber` required to operate `fetch`")
		}

		fetchRequestMsg := &paustqproto.FetchRequest{}
		if err := ptypes.UnmarshalAny(requestMsg, fetchRequestMsg); err != nil {
			return err
		}

		session.Offset = fetchRequestMsg.StartOffset
		counter := 0

		for {
			result, err := s.DB.GetRecord(session.Topic, session.Offset)
			if err != nil {
				return err
			}

			// TODO:: Wait for topic with cv.wait and should check whether any producer publishing to topic exists or not
			if !result.Exists() {
				// for testing, wait 5 sec to new record
				if counter == 5 {
					return nil // end stream
				}
				counter++
				time.Sleep(1*time.Second)
				continue
			}

			counter = 0
			session.Offset++

			respMsg := message.NewFetchResponseMsg(result.Data(), session.Offset)
			anyMsg, err := ptypes.MarshalAny(respMsg)
			if err != nil {
				if err = session.Send(anyMsg); err != nil {
					return err
				}
			}
		}
	}
	return nil
}