package server

import (
	"context"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	defaultTiemout = time.Second * 1

	defaultPoolSize  = 10
	defaultQueueSize = 10
)

// Service is used to serve requests to the server
type Service interface {
	Handle(*Request) (proto.Message, error)
	Subject() string
}

// Server receives requests from NATS, passes the to
// Service and sends results back to NATS. Server also
// manages workers pool.
type Server interface {
	// GetPool returns the Pool used by the server. Returned value
	// can be used to manipulate pool size and queue size without
	// stopping the server.
	GetPool() Pool
	// Stop stops the server
	Stop()
	// SetTimeout sets request timeout
	SetTimeout(time.Duration)
}

var _ Server = &serverImpl{}

type serverImpl struct {
	s       Service
	conn    *nats.Conn
	subject string
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration
	pool    *pool
	subs    *nats.Subscription
}

func (s *serverImpl) GetPool() Pool {
	return s.pool
}

func (s *serverImpl) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

func (s *serverImpl) Stop() {
	s.subs.Unsubscribe()
	s.pool.stop(s.timeout)
	s.cancel()
}

// Start starts a server
func Start(conn *nats.Conn, service Service, opts ...Option) (Server, error) {
	s := &serverImpl{}
	s.s = service
	s.conn = conn
	var cancel context.CancelFunc
	s.ctx, cancel = context.WithCancel(context.Background())
	s.subject = service.Subject()
	s.timeout = defaultTiemout
	s.pool = newPool(s.ctx)
	for _, o := range opts {
		if err := o(s); err != nil {
			cancel()
			return nil, err
		}
	}
	subs, err := s.conn.QueueSubscribe(s.s.Subject()+".*", s.subject, s.handleMessage)
	if err != nil {
		cancel()
		return nil, err
	}
	s.cancel = cancel
	s.subs = subs
	return s, nil
}

func (s *serverImpl) handleMessage(msg *nats.Msg) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	method := strings.TrimPrefix(msg.Subject, s.s.Subject()+".")
	req := &Request{
		Method:    method,
		Message:   msg,
		Cancel:    cancel,
		Context:   ctx,
		Handler:   s.s.Handle,
		Deadline:  time.Now().Add(s.timeout),
		CreatedAt: time.Now(),
	}
	queue := s.pool.getQueue()
	if queue == nil {
		req.sendError(ErrorShuttingDown)
		return
	}
	select {
	case queue <- req:
	default:
		req.sendError(ErrorTooBusy)
	}
}
