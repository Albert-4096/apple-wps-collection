package main

import (
	"testing"
	"time"
)

func TestHubBroadcastReachesSubscribers(t *testing.T) {
	h := newHub()
	a := h.subscribe(4)
	b := h.subscribe(4)
	h.broadcast([]byte("x"))
	for _, s := range []*subscriber{a, b} {
		select {
		case m := <-s.ch:
			if string(m) != "x" {
				t.Fatalf("got %q, want x", m)
			}
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive broadcast")
		}
	}
}

func TestHubDropsSlowSubscriber(t *testing.T) {
	h := newHub()
	h.subscribe(1) // never drained
	h.broadcast([]byte("1")) // fills the buffer
	h.broadcast([]byte("2")) // overflow -> subscriber dropped
	if h.count() != 0 {
		t.Fatalf("count = %d, want 0 (slow subscriber dropped)", h.count())
	}
}

func TestHubUnsubscribe(t *testing.T) {
	h := newHub()
	s := h.subscribe(1)
	if h.count() != 1 {
		t.Fatalf("count = %d, want 1", h.count())
	}
	h.unsubscribe(s)
	if h.count() != 0 {
		t.Fatalf("count = %d, want 0 after unsubscribe", h.count())
	}
}
