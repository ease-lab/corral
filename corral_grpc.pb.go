// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package corral

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion7

// CorralClient is the client API for Corral service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CorralClient interface {
	Invoke(ctx context.Context, in *CorralRequest, opts ...grpc.CallOption) (*CorralResponse, error)
}

type corralClient struct {
	cc grpc.ClientConnInterface
}

func NewCorralClient(cc grpc.ClientConnInterface) CorralClient {
	return &corralClient{cc}
}

func (c *corralClient) Invoke(ctx context.Context, in *CorralRequest, opts ...grpc.CallOption) (*CorralResponse, error) {
	out := new(CorralResponse)
	err := c.cc.Invoke(ctx, "/corral.Corral/Invoke", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CorralServer is the server API for Corral service.
// All implementations must embed UnimplementedCorralServer
// for forward compatibility
type CorralServer interface {
	Invoke(context.Context, *CorralRequest) (*CorralResponse, error)
	mustEmbedUnimplementedCorralServer()
}

// UnimplementedCorralServer must be embedded to have forward compatible implementations.
type UnimplementedCorralServer struct {
}

func (UnimplementedCorralServer) Invoke(context.Context, *CorralRequest) (*CorralResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Invoke not implemented")
}
func (UnimplementedCorralServer) mustEmbedUnimplementedCorralServer() {}

// UnsafeCorralServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CorralServer will
// result in compilation errors.
type UnsafeCorralServer interface {
	mustEmbedUnimplementedCorralServer()
}

func RegisterCorralServer(s grpc.ServiceRegistrar, srv CorralServer) {
	s.RegisterService(&_Corral_serviceDesc, srv)
}

func _Corral_Invoke_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CorralRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CorralServer).Invoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/corral.Corral/Invoke",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CorralServer).Invoke(ctx, req.(*CorralRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Corral_serviceDesc = grpc.ServiceDesc{
	ServiceName: "corral.Corral",
	HandlerType: (*CorralServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Invoke",
			Handler:    _Corral_Invoke_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "corral.proto",
}
