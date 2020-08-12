package server

import (
	"context"
	"errors"
	"time"
)

// Option is used to set server options
type Option func(*serverImpl) error

// WithContext sets the root context used by the server
func WithContext(ctx context.Context) Option {
	return func(s *serverImpl) error {
		s.ctx = ctx
		return nil
	}
}

// WithNatsSubject sets the NATS subject prefix
// used by the server
func WithNatsSubject(queue string) Option {
	return func(s *serverImpl) error {
		s.subject = queue
		return nil
	}
}

// WithMaxWaiting sets the maximum number of
// requests waiting in the queue
func WithMaxWaiting(n int) Option {
	return func(s *serverImpl) error {
		if n < 1 {
			return errors.New("Maximum waiting should be no less than 1")
		}
		s.pool.SetMaxWaiting(n)
		return nil
	}
}

// WithPoolSize sets the size of server's workers pool
func WithPoolSize(n int) Option {
	return func(s *serverImpl) error {
		<-time.After(time.Millisecond * 200)
		if n < 0 {
			return errors.New("Pool size should be no less than 1")
		}
		s.pool.SetSize(n)
		return nil
	}
}
