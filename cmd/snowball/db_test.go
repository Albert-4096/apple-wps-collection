package main

import (
	"path/filepath"
	"testing"
	"wloc/lib"
)

func newTestStore(t *testing.T) *store {
	t.Helper()
	s, err := openDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	t.Cleanup(func() { s.close() })
	return s
}

func TestEnqueueAndPending(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.pending(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("pending = %d, want 2", len(got))
	}
}

func TestEnqueueDedupes(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	// Re-enqueueing the same BSSID must not create a duplicate.
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	n, err := s.pendingCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pendingCount = %d, want 1", n)
	}
}

func TestEnqueueDoesNotResurrectProcessed(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	if err := s.markProcessed("aa:bb:cc:dd:ee:01"); err != nil {
		t.Fatal(err)
	}
	// Apple will return this BSSID again; it must not return to pending.
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	n, err := s.pendingCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pendingCount = %d, want 0 (already processed)", n)
	}
}

func TestMarkProcessedRemovesFromPending(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	if err := s.markProcessed("aa:bb:cc:dd:ee:01"); err != nil {
		t.Fatal(err)
	}
	n, err := s.pendingCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pendingCount = %d, want 0", n)
	}
}

func TestMarkFailedRemovesFromPending(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	if err := s.markFailed("aa:bb:cc:dd:ee:01"); err != nil {
		t.Fatal(err)
	}
	n, err := s.pendingCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pendingCount = %d, want 0 after fail", n)
	}
}

func TestIncAttempts(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01"}); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 3; want++ {
		got, err := s.incAttempts("aa:bb:cc:dd:ee:01")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("incAttempts call %d returned %d", want, got)
		}
	}
}

func TestClaimRemovesFromPending(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:03"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.claim(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claim returned %d, want 2", len(claimed))
	}
	// Claimed rows must no longer be pending, so a second claimer can't grab them.
	if n, _ := s.pendingCount(); n != 1 {
		t.Fatalf("pendingCount = %d, want 1 after claiming 2", n)
	}
	again, err := s.claim(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("second claim returned %d, want 1", len(again))
	}
}

func TestInflightCount(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.inflightCount(); n != 0 {
		t.Fatalf("inflightCount = %d, want 0 before claim", n)
	}
	if _, err := s.claim(2); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.inflightCount(); n != 2 {
		t.Fatalf("inflightCount = %d, want 2 after claiming 2", n)
	}
}

func TestResetInflightRequeuesClaimed(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.claim(2); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.pendingCount(); n != 0 {
		t.Fatalf("pendingCount = %d, want 0 while claimed", n)
	}
	// Simulating a restart: in-flight (claimed) work returns to pending.
	if err := s.resetInflight(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.pendingCount(); n != 2 {
		t.Fatalf("pendingCount = %d, want 2 after reset", n)
	}
}

func TestResetInflightLeavesDoneAlone(t *testing.T) {
	s := newTestStore(t)
	if err := s.enqueue([]string{"done", "failed", "claimed"}); err != nil {
		t.Fatal(err)
	}
	if err := s.markProcessed("done"); err != nil {
		t.Fatal(err)
	}
	if err := s.markFailed("failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.claim(10); err != nil { // claims "claimed" (only pending)
		t.Fatal(err)
	}
	if err := s.resetInflight(); err != nil {
		t.Fatal(err)
	}
	// Only the claimed one should come back; done/failed stay terminal.
	if n, _ := s.pendingCount(); n != 1 {
		t.Fatalf("pendingCount = %d, want 1 (only the claimed one)", n)
	}
}

func TestAddAPsDedupesAndCounts(t *testing.T) {
	s := newTestStore(t)
	aps := []lib.AP{
		{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}},
		{BSSID: "aa:bb:cc:dd:ee:02", Location: lib.Location{Lat: 3, Long: 4}},
		{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}}, // dup
	}
	if err := s.addAPs(aps); err != nil {
		t.Fatal(err)
	}
	// Adding again should still leave the count unchanged.
	if err := s.addAPs(aps); err != nil {
		t.Fatal(err)
	}
	n, err := s.apCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("apCount = %d, want 2 unique", n)
	}
}

func TestAddAPsSkipsInvalidBSSID(t *testing.T) {
	s := newTestStore(t)
	aps := []lib.AP{
		{BSSID: "not-a-mac", Location: lib.Location{Lat: 1, Long: 2}},
		{BSSID: "aa:bb:cc:dd:ee:02", Location: lib.Location{Lat: 3, Long: 4}},
	}
	if err := s.addAPs(aps); err != nil {
		t.Fatal(err)
	}
	n, err := s.apCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("apCount = %d, want 1 (invalid skipped)", n)
	}
}
