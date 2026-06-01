package main

import (
	"context"
	"testing"
	"time"
	"wloc/lib"
)

func TestCaptureBufferCapsAndFlushes(t *testing.T) {
	buf := newCaptureBuffer(2)
	buf.add([]lib.AP{{BSSID: "a", Location: lib.Location{Lat: 1, Long: 2}}})
	buf.add([]lib.AP{{BSSID: "b"}, {BSSID: "c"}}) // 3 added, cap 2
	pts := buf.flush()
	if len(pts) != 2 {
		t.Fatalf("flush = %d points, want 2 (capped)", len(pts))
	}
	if len(buf.flush()) != 0 {
		t.Fatal("second flush should be empty")
	}
}

func TestBuildStats(t *testing.T) {
	st := newTestStore(t)
	if err := st.enqueue([]string{"aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"}); err != nil {
		t.Fatal(err)
	}
	b := newBackoff(time.Second, time.Minute)
	p := newPool(func(context.Context, string) {}, 50, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx, 0)

	s, err := buildStats(st, p, b, 1.5)
	if err != nil {
		t.Fatal(err)
	}
	if s.Type != "stats" {
		t.Fatalf("Type = %q, want stats", s.Type)
	}
	if s.Pending != 2 {
		t.Fatalf("Pending = %d, want 2", s.Pending)
	}
	if s.Max != 50 {
		t.Fatalf("Max = %d, want 50", s.Max)
	}
	if s.Rate != 1.5 {
		t.Fatalf("Rate = %v, want 1.5", s.Rate)
	}
	if s.Throttled {
		t.Fatal("should not be throttled")
	}
}
