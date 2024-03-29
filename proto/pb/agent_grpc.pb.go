// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// PubSubClient is the client API for PubSub service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type PubSubClient interface {
	Subscribe(ctx context.Context, in *Subscription, opts ...grpc.CallOption) (PubSub_SubscribeClient, error)
}

type pubSubClient struct {
	cc grpc.ClientConnInterface
}

func NewPubSubClient(cc grpc.ClientConnInterface) PubSubClient {
	return &pubSubClient{cc}
}

func (c *pubSubClient) Subscribe(ctx context.Context, in *Subscription, opts ...grpc.CallOption) (PubSub_SubscribeClient, error) {
	stream, err := c.cc.NewStream(ctx, &PubSub_ServiceDesc.Streams[0], "/agent.proto.PubSub/Subscribe", opts...)
	if err != nil {
		return nil, err
	}
	x := &pubSubSubscribeClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type PubSub_SubscribeClient interface {
	Recv() (*SubscriptionResult, error)
	grpc.ClientStream
}

type pubSubSubscribeClient struct {
	grpc.ClientStream
}

func (x *pubSubSubscribeClient) Recv() (*SubscriptionResult, error) {
	m := new(SubscriptionResult)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// PubSubServer is the server API for PubSub service.
// All implementations must embed UnimplementedPubSubServer
// for forward compatibility
type PubSubServer interface {
	Subscribe(*Subscription, PubSub_SubscribeServer) error
	mustEmbedUnimplementedPubSubServer()
}

// UnimplementedPubSubServer must be embedded to have forward compatible implementations.
type UnimplementedPubSubServer struct {
}

func (UnimplementedPubSubServer) Subscribe(*Subscription, PubSub_SubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedPubSubServer) mustEmbedUnimplementedPubSubServer() {}

// UnsafePubSubServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PubSubServer will
// result in compilation errors.
type UnsafePubSubServer interface {
	mustEmbedUnimplementedPubSubServer()
}

func RegisterPubSubServer(s grpc.ServiceRegistrar, srv PubSubServer) {
	s.RegisterService(&PubSub_ServiceDesc, srv)
}

func _PubSub_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Subscription)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PubSubServer).Subscribe(m, &pubSubSubscribeServer{stream})
}

type PubSub_SubscribeServer interface {
	Send(*SubscriptionResult) error
	grpc.ServerStream
}

type pubSubSubscribeServer struct {
	grpc.ServerStream
}

func (x *pubSubSubscribeServer) Send(m *SubscriptionResult) error {
	return x.ServerStream.SendMsg(m)
}

// PubSub_ServiceDesc is the grpc.ServiceDesc for PubSub service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PubSub_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "agent.proto.PubSub",
	HandlerType: (*PubSubServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _PubSub_Subscribe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "agent.proto",
}

// RetrievablePubSubClient is the client API for RetrievablePubSub service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RetrievablePubSubClient interface {
	RetrievableSubscribe(ctx context.Context, opts ...grpc.CallOption) (RetrievablePubSub_RetrievableSubscribeClient, error)
}

type retrievablePubSubClient struct {
	cc grpc.ClientConnInterface
}

func NewRetrievablePubSubClient(cc grpc.ClientConnInterface) RetrievablePubSubClient {
	return &retrievablePubSubClient{cc}
}

func (c *retrievablePubSubClient) RetrievableSubscribe(ctx context.Context, opts ...grpc.CallOption) (RetrievablePubSub_RetrievableSubscribeClient, error) {
	stream, err := c.cc.NewStream(ctx, &RetrievablePubSub_ServiceDesc.Streams[0], "/agent.proto.RetrievablePubSub/RetrievableSubscribe", opts...)
	if err != nil {
		return nil, err
	}
	x := &retrievablePubSubRetrievableSubscribeClient{stream}
	return x, nil
}

type RetrievablePubSub_RetrievableSubscribeClient interface {
	Send(*RetrievableSubscription) error
	Recv() (*SubscriptionResult, error)
	grpc.ClientStream
}

type retrievablePubSubRetrievableSubscribeClient struct {
	grpc.ClientStream
}

func (x *retrievablePubSubRetrievableSubscribeClient) Send(m *RetrievableSubscription) error {
	return x.ClientStream.SendMsg(m)
}

func (x *retrievablePubSubRetrievableSubscribeClient) Recv() (*SubscriptionResult, error) {
	m := new(SubscriptionResult)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// RetrievablePubSubServer is the server API for RetrievablePubSub service.
// All implementations must embed UnimplementedRetrievablePubSubServer
// for forward compatibility
type RetrievablePubSubServer interface {
	RetrievableSubscribe(RetrievablePubSub_RetrievableSubscribeServer) error
	mustEmbedUnimplementedRetrievablePubSubServer()
}

// UnimplementedRetrievablePubSubServer must be embedded to have forward compatible implementations.
type UnimplementedRetrievablePubSubServer struct {
}

func (UnimplementedRetrievablePubSubServer) RetrievableSubscribe(RetrievablePubSub_RetrievableSubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method RetrievableSubscribe not implemented")
}
func (UnimplementedRetrievablePubSubServer) mustEmbedUnimplementedRetrievablePubSubServer() {}

// UnsafeRetrievablePubSubServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RetrievablePubSubServer will
// result in compilation errors.
type UnsafeRetrievablePubSubServer interface {
	mustEmbedUnimplementedRetrievablePubSubServer()
}

func RegisterRetrievablePubSubServer(s grpc.ServiceRegistrar, srv RetrievablePubSubServer) {
	s.RegisterService(&RetrievablePubSub_ServiceDesc, srv)
}

func _RetrievablePubSub_RetrievableSubscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RetrievablePubSubServer).RetrievableSubscribe(&retrievablePubSubRetrievableSubscribeServer{stream})
}

type RetrievablePubSub_RetrievableSubscribeServer interface {
	Send(*SubscriptionResult) error
	Recv() (*RetrievableSubscription, error)
	grpc.ServerStream
}

type retrievablePubSubRetrievableSubscribeServer struct {
	grpc.ServerStream
}

func (x *retrievablePubSubRetrievableSubscribeServer) Send(m *SubscriptionResult) error {
	return x.ServerStream.SendMsg(m)
}

func (x *retrievablePubSubRetrievableSubscribeServer) Recv() (*RetrievableSubscription, error) {
	m := new(RetrievableSubscription)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// RetrievablePubSub_ServiceDesc is the grpc.ServiceDesc for RetrievablePubSub service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RetrievablePubSub_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "agent.proto.RetrievablePubSub",
	HandlerType: (*RetrievablePubSubServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RetrievableSubscribe",
			Handler:       _RetrievablePubSub_RetrievableSubscribe_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "agent.proto",
}
