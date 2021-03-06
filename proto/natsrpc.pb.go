// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.12.3
// source: natsrpc.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
	_ "github.com/golang/protobuf/protoc-gen-go/descriptor"
	any "github.com/golang/protobuf/ptypes/any"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

// ErrorCode indicates to which class an Error belongs.
// Mostly copied from gRPC.
type ErrorCode int32

const (
	ErrorCode_OK ErrorCode = 0
	// Canceled indicates the operation was canceled (typically by the caller).
	//
	// The framework will generate this error code when cancellation
	// is requested.
	ErrorCode_Canceled ErrorCode = 1
	ErrorCode_Unknown  ErrorCode = 2
	// InvalidArgument indicates client specified an invalid argument.
	// Note that this differs from FailedPrecondition. It indicates arguments
	// that are problematic regardless of the state of the system
	// (e.g., a malformed file name).
	//
	// This error code will not be generated by the framework.
	ErrorCode_InvalidArgument ErrorCode = 3
	// DeadlineExceeded means operation expired before completion.
	// For operations that change the state of the system, this error may be
	// returned even if the operation has completed successfully. For
	// example, a successful response from a server could have been delayed
	// long enough for the deadline to expire.
	//
	// The framework will generate this error code when the deadline is
	// exceeded.
	ErrorCode_DeadlineExceeded ErrorCode = 4
	// NotFound means some requested entity (e.g., file or directory) was
	// not found.
	//
	// This error code will not be generated by the framework.
	ErrorCode_NotFound ErrorCode = 5
	// AlreadyExists means an attempt to create an entity failed because one
	// already exists.
	//
	// This error code will not be generated by the framework.
	ErrorCode_AlreadyExists ErrorCode = 6
	// PermissionDenied indicates the caller does not have permission to
	// execute the specified operation. It must not be used for rejections
	// caused by exhausting some resource (use ResourceExhausted
	// instead for those errors). It must not be
	// used if the caller cannot be identified (use Unauthenticated
	// instead for those errors).
	//
	// This error code will not be generated by the core framework,
	// but expect authentication middleware to use it.
	ErrorCode_PermissionDenied ErrorCode = 7
	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the entire file system is out of space.
	//
	// This error code will be generated by the framework in
	// out-of-memory and server overload situations, or when a message is
	// larger than the configured maximum size.
	ErrorCode_ResourceExhausted ErrorCode = 8
	// FailedPrecondition indicates operation was rejected because the
	// system is not in a state required for the operation's execution.
	// For example, directory to be deleted may be non-empty, an rmdir
	// operation is applied to a non-directory, etc.
	//
	// A litmus test that may help a service implementor in deciding
	// between FailedPrecondition, Aborted, and Unavailable:
	//  (a) Use Unavailable if the client can retry just the failing call.
	//  (b) Use Aborted if the client should retry at a higher-level
	//      (e.g., restarting a read-modify-write sequence).
	//  (c) Use FailedPrecondition if the client should not retry until
	//      the system state has been explicitly fixed. E.g., if an "rmdir"
	//      fails because the directory is non-empty, FailedPrecondition
	//      should be returned since the client should not retry unless
	//      they have first fixed up the directory by deleting files from it.
	//  (d) Use FailedPrecondition if the client performs conditional
	//      REST Get/Update/Delete on a resource and the resource on the
	//      server does not match the condition. E.g., conflicting
	//      read-modify-write on the same resource.
	//
	// This error code will not be generated by the framework.
	ErrorCode_FailedPrecondition ErrorCode = 9
	// Aborted indicates the operation was aborted, typically due to a
	// concurrency issue like sequencer check failures, transaction aborts,
	// etc.
	//
	// See litmus test above for deciding between FailedPrecondition,
	// Aborted, and Unavailable.
	//
	// This error code will not be generated by the framework.
	ErrorCode_Aborted ErrorCode = 10
	// OutOfRange means operation was attempted past the valid range.
	// E.g., seeking or reading past end of file.
	//
	// Unlike InvalidArgument, this error indicates a problem that may
	// be fixed if the system state changes. For example, a 32-bit file
	// system will generate InvalidArgument if asked to read at an
	// offset that is not in the range [0,2^32-1], but it will generate
	// OutOfRange if asked to read from an offset past the current
	// file size.
	//
	// There is a fair bit of overlap between FailedPrecondition and
	// OutOfRange. We recommend using OutOfRange (the more specific
	// error) when it applies so that callers who are iterating through
	// a space can easily look for an OutOfRange error to detect when
	// they are done.
	//
	// This error code will not be generated by the framework.
	ErrorCode_OutOfRange ErrorCode = 11
	// Unimplemented indicates operation is not implemented or not
	// supported/enabled in this service.
	//
	// This error code will be generated by the framework. Most
	// commonly, you will see this error code when a method implementation
	// is missing on the server.
	ErrorCode_Unimplemented ErrorCode = 12
	// Internal errors. Means some invariants expected by underlying
	// system has been broken. If you see one of these errors,
	// something is very broken.
	//
	// This error code will be generated by the framework in several
	// internal error conditions.
	ErrorCode_Internal ErrorCode = 13
	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff. Note that it is not always safe to retry
	// non-idempotent operations.
	//
	// See litmus test above for deciding between FailedPrecondition,
	// Aborted, and Unavailable.
	//
	// This error code will be generated by the framework during
	// abrupt shutdown of a server process or network connection.
	ErrorCode_Unavailable ErrorCode = 14
	// DataLoss indicates unrecoverable data loss or corruption.
	//
	// This error code will not be generated by the framework.
	ErrorCode_DataLoss ErrorCode = 15
	// Unauthenticated indicates the request does not have valid
	// authentication credentials for the operation.
	//
	// The framework will generate this error code when the
	// authentication metadata is invalid or a Credentials callback fails,
	// but also expect authentication middleware to generate it.
	ErrorCode_Unauthenticated ErrorCode = 16
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0:  "OK",
		1:  "Canceled",
		2:  "Unknown",
		3:  "InvalidArgument",
		4:  "DeadlineExceeded",
		5:  "NotFound",
		6:  "AlreadyExists",
		7:  "PermissionDenied",
		8:  "ResourceExhausted",
		9:  "FailedPrecondition",
		10: "Aborted",
		11: "OutOfRange",
		12: "Unimplemented",
		13: "Internal",
		14: "Unavailable",
		15: "DataLoss",
		16: "Unauthenticated",
	}
	ErrorCode_value = map[string]int32{
		"OK":                 0,
		"Canceled":           1,
		"Unknown":            2,
		"InvalidArgument":    3,
		"DeadlineExceeded":   4,
		"NotFound":           5,
		"AlreadyExists":      6,
		"PermissionDenied":   7,
		"ResourceExhausted":  8,
		"FailedPrecondition": 9,
		"Aborted":            10,
		"OutOfRange":         11,
		"Unimplemented":      12,
		"Internal":           13,
		"Unavailable":        14,
		"DataLoss":           15,
		"Unauthenticated":    16,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_natsrpc_proto_enumTypes[0].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_natsrpc_proto_enumTypes[0]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_natsrpc_proto_rawDescGZIP(), []int{0}
}

// Error is used by services to return error information
type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    ErrorCode `protobuf:"varint,1,opt,name=code,proto3,enum=natsrpc.ErrorCode" json:"code,omitempty"`
	Message string    `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	TraceId string    `protobuf:"bytes,3,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
}

func (x *Error) Reset() {
	*x = Error{}
	if protoimpl.UnsafeEnabled {
		mi := &file_natsrpc_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_natsrpc_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Error.ProtoReflect.Descriptor instead.
func (*Error) Descriptor() ([]byte, []int) {
	return file_natsrpc_proto_rawDescGZIP(), []int{0}
}

func (x *Error) GetCode() ErrorCode {
	if x != nil {
		return x.Code
	}
	return ErrorCode_OK
}

func (x *Error) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Error) GetTraceId() string {
	if x != nil {
		return x.TraceId
	}
	return ""
}

// Sample is used for testing
type Sample struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *Sample) Reset() {
	*x = Sample{}
	if protoimpl.UnsafeEnabled {
		mi := &file_natsrpc_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Sample) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Sample) ProtoMessage() {}

func (x *Sample) ProtoReflect() protoreflect.Message {
	mi := &file_natsrpc_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Sample.ProtoReflect.Descriptor instead.
func (*Sample) Descriptor() ([]byte, []int) {
	return file_natsrpc_proto_rawDescGZIP(), []int{1}
}

func (x *Sample) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

// Response is the message that services send back
// in response to requests.
type Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to ResultOrError:
	//	*Response_Error
	//	*Response_Result
	ResultOrError isResponse_ResultOrError `protobuf_oneof:"ResultOrError"`
}

func (x *Response) Reset() {
	*x = Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_natsrpc_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Response) ProtoMessage() {}

func (x *Response) ProtoReflect() protoreflect.Message {
	mi := &file_natsrpc_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Response.ProtoReflect.Descriptor instead.
func (*Response) Descriptor() ([]byte, []int) {
	return file_natsrpc_proto_rawDescGZIP(), []int{2}
}

func (m *Response) GetResultOrError() isResponse_ResultOrError {
	if m != nil {
		return m.ResultOrError
	}
	return nil
}

func (x *Response) GetError() *Error {
	if x, ok := x.GetResultOrError().(*Response_Error); ok {
		return x.Error
	}
	return nil
}

func (x *Response) GetResult() *any.Any {
	if x, ok := x.GetResultOrError().(*Response_Result); ok {
		return x.Result
	}
	return nil
}

type isResponse_ResultOrError interface {
	isResponse_ResultOrError()
}

type Response_Error struct {
	Error *Error `protobuf:"bytes,1,opt,name=error,proto3,oneof"`
}

type Response_Result struct {
	Result *any.Any `protobuf:"bytes,2,opt,name=result,proto3,oneof"`
}

func (*Response_Error) isResponse_ResultOrError() {}

func (*Response_Result) isResponse_ResultOrError() {}

var File_natsrpc_proto protoreflect.FileDescriptor

var file_natsrpc_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x6e, 0x61, 0x74, 0x73, 0x72, 0x70, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x07, 0x6e, 0x61, 0x74, 0x73, 0x72, 0x70, 0x63, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
	0x70, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x64, 0x0a, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x26,
	0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x6e,
	0x61, 0x74, 0x73, 0x72, 0x70, 0x63, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65,
	0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x12, 0x19, 0x0a, 0x08, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x74, 0x72, 0x61, 0x63, 0x65, 0x49, 0x64, 0x22, 0x22, 0x0a, 0x06, 0x53,
	0x61, 0x6d, 0x70, 0x6c, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22,
	0x73, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x26, 0x0a, 0x05, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6e, 0x61, 0x74,
	0x73, 0x72, 0x70, 0x63, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x48, 0x00, 0x52, 0x05, 0x65, 0x72,
	0x72, 0x6f, 0x72, 0x12, 0x2e, 0x0a, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x48, 0x00, 0x52, 0x06, 0x72, 0x65, 0x73,
	0x75, 0x6c, 0x74, 0x42, 0x0f, 0x0a, 0x0d, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x4f, 0x72, 0x45,
	0x72, 0x72, 0x6f, 0x72, 0x2a, 0xb1, 0x02, 0x0a, 0x09, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f,
	0x64, 0x65, 0x12, 0x06, 0x0a, 0x02, 0x4f, 0x4b, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x61,
	0x6e, 0x63, 0x65, 0x6c, 0x65, 0x64, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x6e, 0x6b, 0x6e,
	0x6f, 0x77, 0x6e, 0x10, 0x02, 0x12, 0x13, 0x0a, 0x0f, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64,
	0x41, 0x72, 0x67, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x10, 0x03, 0x12, 0x14, 0x0a, 0x10, 0x44, 0x65,
	0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x45, 0x78, 0x63, 0x65, 0x65, 0x64, 0x65, 0x64, 0x10, 0x04,
	0x12, 0x0c, 0x0a, 0x08, 0x4e, 0x6f, 0x74, 0x46, 0x6f, 0x75, 0x6e, 0x64, 0x10, 0x05, 0x12, 0x11,
	0x0a, 0x0d, 0x41, 0x6c, 0x72, 0x65, 0x61, 0x64, 0x79, 0x45, 0x78, 0x69, 0x73, 0x74, 0x73, 0x10,
	0x06, 0x12, 0x14, 0x0a, 0x10, 0x50, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x44,
	0x65, 0x6e, 0x69, 0x65, 0x64, 0x10, 0x07, 0x12, 0x15, 0x0a, 0x11, 0x52, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x45, 0x78, 0x68, 0x61, 0x75, 0x73, 0x74, 0x65, 0x64, 0x10, 0x08, 0x12, 0x16,
	0x0a, 0x12, 0x46, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x50, 0x72, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x10, 0x09, 0x12, 0x0b, 0x0a, 0x07, 0x41, 0x62, 0x6f, 0x72, 0x74, 0x65,
	0x64, 0x10, 0x0a, 0x12, 0x0e, 0x0a, 0x0a, 0x4f, 0x75, 0x74, 0x4f, 0x66, 0x52, 0x61, 0x6e, 0x67,
	0x65, 0x10, 0x0b, 0x12, 0x11, 0x0a, 0x0d, 0x55, 0x6e, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x65, 0x64, 0x10, 0x0c, 0x12, 0x0c, 0x0a, 0x08, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e,
	0x61, 0x6c, 0x10, 0x0d, 0x12, 0x0f, 0x0a, 0x0b, 0x55, 0x6e, 0x61, 0x76, 0x61, 0x69, 0x6c, 0x61,
	0x62, 0x6c, 0x65, 0x10, 0x0e, 0x12, 0x0c, 0x0a, 0x08, 0x44, 0x61, 0x74, 0x61, 0x4c, 0x6f, 0x73,
	0x73, 0x10, 0x0f, 0x12, 0x13, 0x0a, 0x0f, 0x55, 0x6e, 0x61, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74,
	0x69, 0x63, 0x61, 0x74, 0x65, 0x64, 0x10, 0x10, 0x42, 0x26, 0x5a, 0x24, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x68, 0x61, 0x74, 0x6f, 0x62, 0x69, 0x74, 0x6f, 0x2d,
	0x69, 0x6f, 0x2f, 0x6e, 0x61, 0x74, 0x73, 0x72, 0x70, 0x63, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_natsrpc_proto_rawDescOnce sync.Once
	file_natsrpc_proto_rawDescData = file_natsrpc_proto_rawDesc
)

func file_natsrpc_proto_rawDescGZIP() []byte {
	file_natsrpc_proto_rawDescOnce.Do(func() {
		file_natsrpc_proto_rawDescData = protoimpl.X.CompressGZIP(file_natsrpc_proto_rawDescData)
	})
	return file_natsrpc_proto_rawDescData
}

var file_natsrpc_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_natsrpc_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_natsrpc_proto_goTypes = []interface{}{
	(ErrorCode)(0),   // 0: natsrpc.ErrorCode
	(*Error)(nil),    // 1: natsrpc.Error
	(*Sample)(nil),   // 2: natsrpc.Sample
	(*Response)(nil), // 3: natsrpc.Response
	(*any.Any)(nil),  // 4: google.protobuf.Any
}
var file_natsrpc_proto_depIdxs = []int32{
	0, // 0: natsrpc.Error.code:type_name -> natsrpc.ErrorCode
	1, // 1: natsrpc.Response.error:type_name -> natsrpc.Error
	4, // 2: natsrpc.Response.result:type_name -> google.protobuf.Any
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_natsrpc_proto_init() }
func file_natsrpc_proto_init() {
	if File_natsrpc_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_natsrpc_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Error); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_natsrpc_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Sample); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_natsrpc_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Response); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_natsrpc_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*Response_Error)(nil),
		(*Response_Result)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_natsrpc_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_natsrpc_proto_goTypes,
		DependencyIndexes: file_natsrpc_proto_depIdxs,
		EnumInfos:         file_natsrpc_proto_enumTypes,
		MessageInfos:      file_natsrpc_proto_msgTypes,
	}.Build()
	File_natsrpc_proto = out.File
	file_natsrpc_proto_rawDesc = nil
	file_natsrpc_proto_goTypes = nil
	file_natsrpc_proto_depIdxs = nil
}
