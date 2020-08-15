package server

import (
	"github.com/hatobito-io/natsrpc/proto"
)

// Error is used by services to return error information
type Error = proto.Error

// ErrorTooBusy is sent by the server if it cannot
// serve a request
var ErrorTooBusy = &Error{
	Code:    proto.ErrorCode_ResourceExhausted,
	Message: "Server is too busy",
}

// ErrorShuttingDown is sent by the server
// if a request comes when the server in the
// process of shutting down
var ErrorShuttingDown = &Error{
	Code:    proto.ErrorCode_Unavailable,
	Message: "Service is shutting down",
}
