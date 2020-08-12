package server

import (
	"context"
	"time"

	rpcproto "github.com/hatobito-io/natsrpc/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RequestHandler if the function signature for request handlers
type RequestHandler func(*Request) (proto.Message, error)

// Request represents a single request sent to the server by a client
type Request struct {
	Method    string
	Message   *nats.Msg
	Context   context.Context
	Cancel    context.CancelFunc
	Deadline  time.Time
	Handler   RequestHandler
	CreatedAt time.Time
}

func (req *Request) execute() {
	reply, err := req.Handler(req)
	if err != nil {
		return
	}
	req.sendReply(reply)
}

func (req *Request) sendError(e *Error) {
	req.sendReply(e)
}

func (req *Request) sendReply(reply proto.Message) {
	var result *rpcproto.Response
	if e, ok := reply.(*Error); ok {
		result = &rpcproto.Response{
			ResultOrError: &rpcproto.Response_Error{
				Error: e,
			},
		}
	} else {
		any, err := anypb.New(reply)
		if err != nil {
			return
		}
		result = &rpcproto.Response{
			ResultOrError: &rpcproto.Response_Result{
				Result: any,
			},
		}
	}
	data, err := proto.Marshal(result)
	if err != nil {
		return
	}
	req.Message.Respond(data)
}
