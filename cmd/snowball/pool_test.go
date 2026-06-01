package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func waitActive(t *testing.T, p *pool, want int) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if p.getActive() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("active = %d, want %d", p.getActive(), want)
}

func TestPoolScalesToTarget(t *testing.T) {
	p := newPool(func(context.Context, string) {}, 5, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx, 0)
	waitActive(t, p, 0)

	p.setTarget(3)
	if p.getTarget() != 3 {
		t.Fatalf("target = %d, want 3", p.getTarget())
	}
	waitActive(t, p, 3)

	p.setTarget(1)
	waitActive(t, p, 1)

	cancel()
	p.wait()
}

func TestPoolClampsTarget(t *testing.T) {
	p := newPool(func(context.Context, string) {}, 5, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx, 0)

	p.setTarget(100)
	if p.getTarget() != 5 {
		t.Fatalf("target = %d, want clamped to 5", p.getTarget())
	}
	waitActive(t, p, 5)

	p.setTarget(-3)
	if p.getTarget() != 0 {
		t.Fatalf("target = %d, want clamped to 0", p.getTarget())
	}
	waitActive(t, p, 0)

	cancel()
	p.wait()
}

func TestPoolProcessesWork(t *testing.T) {
	var n int32
	done := make(chan struct{}, 3)
	p := newPool(func(ctx context.Context, b string) {
		atomic.AddInt32(&n, 1)
		done <- struct{}{}
	}, 5, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx, 2)

	p.work <- "a"
	p.work <- "b"
	p.work <- "c"
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for work to be handled")
		}
	}
	if atomic.LoadInt32(&n) != 3 {
		t.Fatalf("handled = %d, want 3", n)
	}

	cancel()
	p.wait()
}
