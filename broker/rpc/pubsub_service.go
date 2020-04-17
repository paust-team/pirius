package rpc

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/paustq/broker/storage"
	paustq_proto "github.com/paust-team/paustq/proto"
	"io"
	"time"
)

type PubSubServiceServer struct {
	DB  		*storage.QRocksDB
	Topic 		string
	Offset 		uint64
	SessionType paustq_proto.SessionType
	Connected 	bool
}

func NewPubSubServiceServer(db *storage.QRocksDB) *PubSubServiceServer {
	return &PubSubServiceServer{DB: db, Offset: 0, Connected: false}
}

// TODO:: will be replaced using pipeline package
func (s *PubSubServiceServer) HandleFlow(stream paustq_proto.PubSubService_FlowStreamServer, request *any.Any) error {

	if ptypes.Is(request, &paustq_proto.PutRequest{}) { // put
		if s.SessionType != paustq_proto.SessionType_PUBLISHER {
			return errors.New("session type `publisher` required to operate `put`")
		}
		putRequestMsg := &paustq_proto.PutRequest{}
		if err := ptypes.UnmarshalAny(request, putRequestMsg); err != nil {
			return err
		}

		if err := s.DB.PutRecord(s.Topic, s.Offset, putRequestMsg.Data); err != nil {
			return err
		}
		// TODO:: This works on single producer only. To support multiple producer, manage topic offset globally.
		s.Offset++

		putResponseMsg := &paustq_proto.PutResponse{Magic: -1}
		anyMsg, err := ptypes.MarshalAny(putResponseMsg)
		if err != nil {
			if err = stream.Send(&paustq_proto.PubSubResponse{Response: anyMsg}); err != nil {
				return err
			}
		}

	} else if ptypes.Is(request, &paustq_proto.FetchRequest{}) { //fetch
		if s.SessionType != paustq_proto.SessionType_SUBSCRIBER {
			return errors.New("session type `subscriber` required to operate `fetch`")
		}

		fetchRequestMsg := &paustq_proto.FetchRequest{}
		if err := ptypes.UnmarshalAny(request, fetchRequestMsg); err != nil {
			return err
		}

		s.Offset = fetchRequestMsg.StartOffset
		counter := 0

		for {
			result, err := s.DB.GetRecord(s.Topic, s.Offset)
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
			s.Offset++

			fetchResponseMsg := &paustq_proto.FetchResponse{Magic: -1, Data: result.Data(), LastOffset: s.Offset}
			anyMsg, err := ptypes.MarshalAny(fetchResponseMsg)
			if err != nil {
				if err = stream.Send(&paustq_proto.PubSubResponse{Response: anyMsg}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *PubSubServiceServer) ConnectStream(_ context.Context, request *paustq_proto.ConnectRequest) (*paustq_proto.ConnectResponse, error) {

	s.Topic = request.TopicName
	s.SessionType = request.SessionType
	s.Connected = true
	return &paustq_proto.ConnectResponse{Magic: -1}, nil
}

func (s *PubSubServiceServer) FlowStream(stream paustq_proto.PubSubService_FlowStreamServer) error {
	if !s.Connected {
		return errors.New("initial connect request required")
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err = s.HandleFlow(stream, in.Request); err != nil {
			return err
		}
	}
}