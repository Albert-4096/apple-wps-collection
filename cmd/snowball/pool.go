package main

import (
	"context"
	"sync"
	"sync/atomic"
)

// pool is a worker pool whose size can be changed at runtime. Each worker is a
// goroutine that pulls BSSIDs off the work channel and runs handle. setTarget
// reconciles the number of workers to the requested count: it spawns new ones
// or signals surplus ones to retire after their current item.
type pool struct {
	work   chan string
	handle func(ctx context.Context, bssid string)
	max    int

	ctx    context.Context
	mu     sync.Mutex
	quits  []chan struct{} // one per intended worker; close to retire it
	wg     sync.WaitGroup
	target int32
	active int32
}

// newPool creates a pool with a work channel buffered to workBuf. max is the
// hard ceiling enforced by setTarget. Call start before setTarget.
func newPool(handle func(context.Context, string), max, workBuf int) *pool {
	return &pool{
		work:   make(chan string, workBuf),
		handle: handle,
		max:    max,
	}
}

// start binds the lifecycle context and brings the pool to its initial size.
func (p *pool) start(ctx context.Context, initial int) {
	p.ctx = ctx
	p.setTarget(initial)
}

// setTarget reconciles the worker count to n, clamped to [0, max].
func (p *pool) setTarget(n int) {
	if n < 0 {
		n = 0
	}
	if n > p.max {
		n = p.max
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.target, int32(n))
	for len(p.quits) < n {
		q := make(chan struct{})
		p.quits = append(p.quits, q)
		p.wg.Add(1)
		go p.worker(q)
	}
	for len(p.quits) > n {
		last := p.quits[len(p.quits)-1]
		p.quits = p.quits[:len(p.quits)-1]
		close(last)
	}
}

func (p *pool) worker(quit <-chan struct{}) {
	atomic.AddInt32(&p.active, 1)
	defer atomic.AddInt32(&p.active, -1)
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-quit:
			return
		case b, ok := <-p.work:
			if !ok {
				return
			}
			p.handle(p.ctx, b)
		}
	}
}

func (p *pool) getTarget() int { return int(atomic.LoadInt32(&p.target)) }
func (p *pool) getActive() int { return int(atomic.LoadInt32(&p.active)) }
func (p *pool) maxTarget() int { return p.max }
func (p *pool) wait()          { p.wg.Wait() }
