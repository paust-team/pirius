package rpc

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/paust-team/paustq/broker/storage"
	paustq_proto "github.com/paust-team/paustq/proto"
	"io"
)

type PubSubServiceServer struct {
	DB  	*storage.QRocksDB
}

func NewPubSubServiceServer(db *storage.QRocksDB) *PubSubServiceServer {
	return &PubSubServiceServer{db}
}

// TODO:: will be replaced using pipeline package
func HandleFlow(stream paustq_proto.PubSubService_FlowStreamServer, request *any.Any) {
	if ptypes.Is(request, &paustq_proto.PutRequest{}) {

	} else if ptypes.Is(request, &paustq_proto.FetchRequest{}) {
		// stream.Send(FetchResponse)
	}
}

func (s *PubSubServiceServer) FlowStream(stream paustq_proto.PubSubService_FlowStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		HandleFlow(stream, in.Request)
	}
}