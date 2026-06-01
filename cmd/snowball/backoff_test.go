package main

import (
	"testing"
	"time"
)

func TestBackoffDoublesUntilCap(t *testing.T) {
	b := newBackoff(time.Second, 60*time.Second)
	want := []time.Duration{1, 2, 4, 8, 16, 32, 60, 60}
	for i, w := range want {
		got := b.fail()
		if got != w*time.Second {
			t.Fatalf("fail #%d = %v, want %v", i+1, got, w*time.Second)
		}
	}
}

func TestBackoffResetsOnSuccess(t *testing.T) {
	b := newBackoff(time.Second, 60*time.Second)
	b.fail()
	b.fail()
	b.fail()
	b.reset()
	if got := b.fail(); got != time.Second {
		t.Fatalf("after reset, fail() = %v, want 1s", got)
	}
}
