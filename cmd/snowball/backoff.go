package main

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// backoff is a shared, exponential backoff coordinator. When Apple rate-limits
// us (503) or requests fail, workers call fail() and then pause() so the whole
// pool slows together; a successful request calls reset().
type backoff struct {
	base, max time.Duration
	mu        sync.Mutex
	attempt   int
}

func newBackoff(base, max time.Duration) *backoff {
	return &backoff{base: base, max: max}
}

// fail records a failure and returns the (deterministic) delay to apply.
func (b *backoff) fail() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempt++
	d := b.base << (b.attempt - 1)
	// Detect shift overflow as well as exceeding the cap.
	if d <= 0 || d > b.max {
		d = b.max
	}
	return d
}

func (b *backoff) reset() {
	b.mu.Lock()
	b.attempt = 0
	b.mu.Unlock()
}

// snapshot reports the current throttle state without mutating it: whether the
// pool is currently backing off, and the delay that the next pause would use.
func (b *backoff) snapshot() (throttled bool, delay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.attempt == 0 {
		return false, 0
	}
	d := b.base << (b.attempt - 1)
	if d <= 0 || d > b.max {
		d = b.max
	}
	return true, d
}

// pause sleeps for d plus a little jitter, returning early if ctx is cancelled.
// Returns false if the context was cancelled during the wait.
func (b *backoff) pause(ctx context.Context, d time.Duration) bool {
	jitter := time.Duration(rand.Int63n(int64(b.base)))
	t := time.NewTimer(d + jitter)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
