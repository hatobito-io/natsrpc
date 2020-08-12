package server

import (
	"context"
	"sync"
	"time"
)

// Pool is used to manage the pool of requests.
// It contains a pool of workers doing actual requests
// processing and a queue of waiting requests.
type Pool interface {
	// SetSize sets the size of the workers pool.
	SetSize(int)
	// SetMaxWaiting sets the maximum number of
	// requests waiting in the queue.
	//
	// Reducing the queue capacity may cause
	// some waiting requests to be ended
	// prematurely with ErrorTooBusy.
	SetMaxWaiting(int)
}

var _ Pool = &pool{}

type pool struct {
	sync.Mutex
	wg            sync.WaitGroup
	queue         chan *Request
	schedule      chan *Request
	size          int
	cancelContext context.CancelFunc
}

func newPool(ctx context.Context) *pool {
	p := &pool{}
	_, p.cancelContext = context.WithCancel(ctx)
	p.queue = make(chan *Request, defaultQueueSize)
	p.schedule = make(chan *Request)
	p.wg.Add(1)
	go p.scheduler()
	p.setSize(defaultPoolSize)
	return p
}

func (p *pool) scheduler() {
	defer p.wg.Done()
queueIteration:
	for {
		p.Lock()
		queue := p.queue
		p.Unlock()
		if queue == nil {
			return
		}
		for req := range queue {
			now := time.Now()
			if req.Deadline.After(now) {
				select {
				case p.schedule <- req:
					continue queueIteration
				case <-time.After(req.Deadline.Sub(now)):
				}
			}
			req.sendError(ErrorTooBusy)
		}
	}
}

func (p *pool) setSize(size int) {
	p.Lock()
	defer p.Unlock()
	for p.size > size {
		p.schedule <- nil
		p.size--
	}
	for p.size < size {
		p.wg.Add(1)
		go p.worker()
		p.size++
	}
}

func (p *pool) SetSize(size int) {
	if size < 1 {
		return
	}
	p.setSize(size)
}

func (p *pool) SetMaxWaiting(size int) {
	p.Lock()
	defer p.Unlock()
	queue := p.queue
	p.queue = make(chan *Request, size)
	close(queue)
	// try to save as many requests from the current queue
	// as possible
	for req := range queue {
		select {
		case p.queue <- req:
			//
		default:
			req.sendError(ErrorTooBusy)
		}
	}
}

func (p *pool) worker() {
	defer p.wg.Done()
	for req := range p.schedule {
		if req == nil {
			return
		}
		req.execute()
	}
}

func (p *pool) getQueue() chan<- *Request {
	p.Lock()
	defer p.Unlock()
	return p.queue
}

func (p *pool) stop(timeout time.Duration) {
	p.setSize(0)
	p.Lock()
	queue := p.queue
	p.queue = nil
	p.Unlock()
	close(queue)
	for req := range queue {
		req.sendError(ErrorShuttingDown)
	}
	timer := time.AfterFunc(timeout, p.cancelContext)
	close(p.schedule)
	p.wg.Wait()
	timer.Stop()
}
