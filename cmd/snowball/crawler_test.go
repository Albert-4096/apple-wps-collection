package main

import (
	"context"
	"errors"
	"testing"
	"time"
	"wloc/lib"
)

func newTestCrawler(t *testing.T, q queryFunc) *crawler {
	t.Helper()
	return &crawler{
		store:       newTestStore(t),
		backoff:     newBackoff(time.Millisecond, 5*time.Millisecond),
		query:       q,
		maxAttempts: 3,
	}
}

func TestHandleSuccessStoresAndExpands(t *testing.T) {
	q := func([]string, int32, ...lib.Modifier) ([]lib.AP, error) {
		return []lib.AP{
			{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}},
			{BSSID: "aa:bb:cc:dd:ee:02", Location: lib.Location{Lat: 3, Long: 4}},
		}, nil
	}
	c := newTestCrawler(t, q)
	if err := c.store.enqueue([]string{"aa:bb:cc:dd:ee:00"}); err != nil {
		t.Fatal(err)
	}

	c.handle(context.Background(), "aa:bb:cc:dd:ee:00")

	if n, _ := c.store.apCount(); n != 2 {
		t.Fatalf("apCount = %d, want 2", n)
	}
	// The two returned neighbours become new pending frontier entries; the
	// queried BSSID is now processed (not pending).
	if n, _ := c.store.pendingCount(); n != 2 {
		t.Fatalf("pendingCount = %d, want 2", n)
	}
}

func TestHandlePoisonMarkedFailed(t *testing.T) {
	calls := 0
	q := func([]string, int32, ...lib.Modifier) ([]lib.AP, error) {
		calls++
		return nil, errors.New("Bad Request")
	}
	c := newTestCrawler(t, q)
	if err := c.store.enqueue([]string{"aa:bb:cc:dd:ee:00"}); err != nil {
		t.Fatal(err)
	}

	c.handle(context.Background(), "aa:bb:cc:dd:ee:00")

	if calls != c.maxAttempts {
		t.Fatalf("query called %d times, want %d", calls, c.maxAttempts)
	}
	if n, _ := c.store.pendingCount(); n != 0 {
		t.Fatalf("pendingCount = %d, want 0 (should be failed)", n)
	}
}

func TestHandleRateLimitDoesNotCountAsPoison(t *testing.T) {
	calls := 0
	q := func([]string, int32, ...lib.Modifier) ([]lib.AP, error) {
		calls++
		if calls < 5 {
			return nil, errors.New("Service Unavailable") // 503
		}
		return []lib.AP{{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}}}, nil
	}
	c := newTestCrawler(t, q)
	if err := c.store.enqueue([]string{"aa:bb:cc:dd:ee:00"}); err != nil {
		t.Fatal(err)
	}

	c.handle(context.Background(), "aa:bb:cc:dd:ee:00")

	// 503s must not trip the poison limit: it keeps retrying past maxAttempts
	// until the server recovers.
	if calls != 5 {
		t.Fatalf("query called %d times, want 5", calls)
	}
	if n, _ := c.store.apCount(); n != 1 {
		t.Fatalf("apCount = %d, want 1 after recovery", n)
	}
}

func TestHandleStopsOnContextCancel(t *testing.T) {
	q := func([]string, int32, ...lib.Modifier) ([]lib.AP, error) {
		return nil, errors.New("Service Unavailable")
	}
	c := newTestCrawler(t, q)
	if err := c.store.enqueue([]string{"aa:bb:cc:dd:ee:00"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	done := make(chan struct{})
	go func() { c.handle(ctx, "aa:bb:cc:dd:ee:00"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handle did not return on cancelled context")
	}
}
