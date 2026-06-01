# Snowball Real-Time Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a live, map-led web dashboard to the `snowball` crawler that streams captured BSSIDs and stats over WebSocket and lets the operator change worker concurrency on a running instance.

**Architecture:** Single Go binary. The crawler publishes capture/stat events to an in-process pub/sub hub; a WebSocket handler in the same process fans them out to browsers. A dynamic worker pool reconciles its live goroutine count to a target the dashboard sets; the target persists in SQLite. The frontend is embedded static assets (`go:embed`) using Leaflet + uPlot from CDN — no Node build.

**Tech Stack:** Go 1.22, `modernc.org/sqlite`, `labstack/echo/v4` (already in `go.mod`), `gorilla/websocket` (new), Leaflet + leaflet.heat + uPlot via CDN.

**Conventions:** TDD throughout. Run `go test ./cmd/snowball/...` after each task. Match the existing test style in `cmd/snowball/*_test.go` (table-free, `t.Fatalf` with `got/want`). End every commit message with the trailer:
`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

---

## File Structure

All work is in `cmd/snowball/` plus repo-root config.

| File | Responsibility |
|------|----------------|
| `db.go` *(modify)* | Add `settings` table, `getSetting`/`setSetting`, `recentAPs` |
| `backoff.go` *(modify)* | Add `snapshot()` to read throttle state without mutating |
| `pool.go` *(new)* | Dynamic worker pool: reconcile active goroutines to a target |
| `hub.go` *(new)* | In-process pub/sub; drops slow subscribers, never blocks publishers |
| `events.go` *(new)* | WS message structs, `apsToPoints`, `captureBuffer`, `buildStats`, `buildSnapshot` |
| `crawler.go` *(modify)* | Emit a capture event after storing APs |
| `assets.go` *(new)* | `go:embed web` filesystem var |
| `web/index.html` *(new)* | Dashboard markup + CDN script tags |
| `web/style.css` *(new)* | Dark OLED ops-console styling |
| `web/app.js` *(new)* | WS client, Leaflet map/heat, uPlot sparkline, workers control |
| `server.go` *(new)* | echo server: static assets, `/ws`, `setWorkers`, token auth |
| `main.go` *(modify)* | Wire hub/pool/server/broadcast loop; new flags; load persisted target |
| `docker-compose.yml` *(modify)* | Resource caps + localhost-only port publish |
| `cmd/snowball/Dockerfile` *(modify)* | `EXPOSE 8080`, default `-listen :8080` |
| `cmd/snowball/README.md` *(modify)* | Document the dashboard + new flags |

---

## Task 1: DB settings + recentAPs

**Files:**
- Modify: `cmd/snowball/db.go`
- Test: `cmd/snowball/db_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/snowball/db_test.go`:

```go
func TestSettingsRoundTrip(t *testing.T) {
	s := newTestStore(t)
	if v, _ := s.getSetting("workers", "15"); v != "15" {
		t.Fatalf("default getSetting = %q, want 15", v)
	}
	if err := s.setSetting("workers", "42"); err != nil {
		t.Fatal(err)
	}
	v, err := s.getSetting("workers", "15")
	if err != nil {
		t.Fatal(err)
	}
	if v != "42" {
		t.Fatalf("getSetting = %q, want 42", v)
	}
}

func TestRecentAPs(t *testing.T) {
	s := newTestStore(t)
	aps := []lib.AP{
		{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}},
		{BSSID: "aa:bb:cc:dd:ee:02", Location: lib.Location{Lat: 3, Long: 4}},
	}
	if err := s.addAPs(aps); err != nil {
		t.Fatal(err)
	}
	got, err := s.recentAPs(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("recentAPs = %d, want 2", len(got))
	}
	if got[0].BSSID == "" {
		t.Fatal("recentAPs returned empty BSSID")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run 'TestSettingsRoundTrip|TestRecentAPs' -v`
Expected: FAIL — `s.getSetting undefined`, `s.recentAPs undefined`.

- [ ] **Step 3: Add the schema + methods**

In `cmd/snowball/db.go`, add a `settings` table to the `schema` slice in `openDB` (after the `idx_frontier_pending` statement):

```go
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
```

Then add these methods at the end of the file:

```go
// getSetting returns the stored value for key, or def if unset.
func (s *store) getSetting(key, def string) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var v string
	err := s.db.QueryRow("SELECT value FROM settings WHERE key = ?", key).Scan(&v)
	if err == sql.ErrNoRows {
		return def, nil
	}
	if err != nil {
		return def, err
	}
	return v, nil
}

// setSetting upserts a key/value pair.
func (s *store) setSetting(key, value string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO settings (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

// recentAPs returns the most recently discovered access points, newest first,
// with BSSIDs decoded back to colon-hex strings for the UI.
func (s *store) recentAPs(limit int) ([]lib.AP, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	rows, err := s.db.Query(
		"SELECT bssid, lat, lon FROM access_points ORDER BY discovered_at DESC, bssid DESC LIMIT ?",
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lib.AP
	for rows.Next() {
		var id int64
		var lat, lon float64
		if err := rows.Scan(&id, &lat, &lon); err != nil {
			return nil, err
		}
		out = append(out, lib.AP{BSSID: mac.Decode(id), Location: lib.Location{Lat: lat, Long: lon}})
	}
	return out, rows.Err()
}
```

`mac` is already imported in `db.go` (`wloc/lib/mac`). `sql` is already imported.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run 'TestSettingsRoundTrip|TestRecentAPs' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/db.go cmd/snowball/db_test.go
git commit -m "feat(snowball): add settings persistence and recentAPs query" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: backoff.snapshot()

**Files:**
- Modify: `cmd/snowball/backoff.go`
- Test: `cmd/snowball/backoff_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/snowball/backoff_test.go`:

```go
func TestBackoffSnapshot(t *testing.T) {
	b := newBackoff(time.Second, 4*time.Second)
	if th, _ := b.snapshot(); th {
		t.Fatal("fresh backoff should not report throttled")
	}
	b.fail()
	th, d := b.snapshot()
	if !th {
		t.Fatal("should report throttled after a failure")
	}
	if d != time.Second {
		t.Fatalf("delay = %v, want 1s", d)
	}
	b.reset()
	if th, _ := b.snapshot(); th {
		t.Fatal("reset should clear throttled state")
	}
}
```

If `backoff_test.go` does not already import `time`, add it.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run TestBackoffSnapshot -v`
Expected: FAIL — `b.snapshot undefined`.

- [ ] **Step 3: Add the method**

In `cmd/snowball/backoff.go`, add after `reset()`:

```go
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
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run TestBackoffSnapshot -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/backoff.go cmd/snowball/backoff_test.go
git commit -m "feat(snowball): expose backoff throttle state via snapshot()" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Dynamic worker pool

**Files:**
- Create: `cmd/snowball/pool.go`
- Test: `cmd/snowball/pool_test.go`

- [ ] **Step 1: Write the failing tests**

Create `cmd/snowball/pool_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run TestPool -v`
Expected: FAIL — `newPool undefined`.

- [ ] **Step 3: Implement the pool**

Create `cmd/snowball/pool.go`:

```go
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

func (p *pool) getTarget() int  { return int(atomic.LoadInt32(&p.target)) }
func (p *pool) getActive() int  { return int(atomic.LoadInt32(&p.active)) }
func (p *pool) maxTarget() int  { return p.max }
func (p *pool) wait()           { p.wg.Wait() }
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run TestPool -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/pool.go cmd/snowball/pool_test.go
git commit -m "feat(snowball): add runtime-resizable worker pool" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Event hub

**Files:**
- Create: `cmd/snowball/hub.go`
- Test: `cmd/snowball/hub_test.go`

- [ ] **Step 1: Write the failing tests**

Create `cmd/snowball/hub_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run TestHub -v`
Expected: FAIL — `newHub undefined`.

- [ ] **Step 3: Implement the hub**

Create `cmd/snowball/hub.go`:

```go
package main

import "sync"

// subscriber is one connected dashboard client. Messages are delivered on ch.
type subscriber struct {
	ch chan []byte
}

// hub is an in-process publish/subscribe fan-out. broadcast is non-blocking: a
// subscriber whose buffer is full is dropped, so a slow or dead browser can
// never block the crawler that publishes events.
type hub struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[*subscriber]struct{})}
}

func (h *hub) subscribe(buffer int) *subscriber {
	s := &subscriber{ch: make(chan []byte, buffer)}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *hub) unsubscribe(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
	}
}

func (h *hub) broadcast(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- msg:
		default:
			// Slow consumer: drop it rather than block the publisher.
			delete(h.subs, s)
			close(s.ch)
		}
	}
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run TestHub -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/hub.go cmd/snowball/hub_test.go
git commit -m "feat(snowball): add in-process pub/sub event hub" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Event messages, capture buffer, stats/snapshot builders

**Files:**
- Create: `cmd/snowball/events.go`
- Test: `cmd/snowball/events_test.go`

Depends on Task 1 (`recentAPs`), Task 2 (`snapshot`), Task 3 (`pool`).

- [ ] **Step 1: Write the failing tests**

Create `cmd/snowball/events_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run 'TestCaptureBuffer|TestBuildStats' -v`
Expected: FAIL — `newCaptureBuffer undefined`, `buildStats undefined`.

- [ ] **Step 3: Implement events.go**

Create `cmd/snowball/events.go`:

```go
package main

import (
	"encoding/json"
	"sync"
	"wloc/lib"
)

// point is one access point as sent to the browser.
type point struct {
	B   string  `json:"b"`
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// statsMsg is the periodic metrics push (type "stats").
type statsMsg struct {
	Type      string  `json:"type"`
	APs       int     `json:"aps"`
	Rate      float64 `json:"rate"`
	Pending   int     `json:"pending"`
	Inflight  int     `json:"inflight"`
	Active    int     `json:"workers"`
	Target    int     `json:"target"`
	Max       int     `json:"max"`
	Throttled bool    `json:"throttled"`
	BackoffMs int64   `json:"backoffMs"`
}

// captureMsg is a batch of freshly discovered points (type "capture").
type captureMsg struct {
	Type   string  `json:"type"`
	Points []point `json:"points"`
}

// snapshotMsg is the one-shot state sent when a client connects (type "snapshot").
type snapshotMsg struct {
	Type   string   `json:"type"`
	Stats  statsMsg `json:"stats"`
	Points []point  `json:"points"`
}

func apsToPoints(aps []lib.AP) []point {
	pts := make([]point, 0, len(aps))
	for _, ap := range aps {
		pts = append(pts, point{B: ap.BSSID, Lat: ap.Location.Lat, Lon: ap.Location.Long})
	}
	return pts
}

// captureBuffer accumulates points published by workers and hands them out in
// capped batches when the broadcast loop flushes it. Capping bounds the data
// pushed per tick so a burst of captures can't firehose the socket.
type captureBuffer struct {
	mu     sync.Mutex
	points []point
	max    int
}

func newCaptureBuffer(max int) *captureBuffer {
	return &captureBuffer{max: max}
}

func (c *captureBuffer) add(aps []lib.AP) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ap := range aps {
		if len(c.points) >= c.max {
			return
		}
		c.points = append(c.points, point{B: ap.BSSID, Lat: ap.Location.Lat, Lon: ap.Location.Long})
	}
}

func (c *captureBuffer) flush() []point {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.points
	c.points = nil
	return out
}

// buildStats assembles the current metrics. rate is captures/sec, computed by
// the caller from the apCount delta.
func buildStats(st *store, p *pool, b *backoff, rate float64) (statsMsg, error) {
	aps, err := st.apCount()
	if err != nil {
		return statsMsg{}, err
	}
	pending, err := st.pendingCount()
	if err != nil {
		return statsMsg{}, err
	}
	inflight, err := st.inflightCount()
	if err != nil {
		return statsMsg{}, err
	}
	throttled, delay := b.snapshot()
	return statsMsg{
		Type:      "stats",
		APs:       aps,
		Rate:      rate,
		Pending:   pending,
		Inflight:  inflight,
		Active:    p.getActive(),
		Target:    p.getTarget(),
		Max:       p.maxTarget(),
		Throttled: throttled,
		BackoffMs: delay.Milliseconds(),
	}, nil
}

// buildSnapshot marshals the initial state for a newly connected client.
func buildSnapshot(st *store, p *pool, b *backoff, recent int) ([]byte, error) {
	stats, err := buildStats(st, p, b, 0)
	if err != nil {
		return nil, err
	}
	aps, err := st.recentAPs(recent)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshotMsg{Type: "snapshot", Stats: stats, Points: apsToPoints(aps)})
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run 'TestCaptureBuffer|TestBuildStats' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/events.go cmd/snowball/events_test.go
git commit -m "feat(snowball): add WS message types, capture buffer, stat builders" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Crawler capture hook

**Files:**
- Modify: `cmd/snowball/crawler.go:13-19` (struct), `cmd/snowball/crawler.go:56-59` (handle success path)
- Test: `cmd/snowball/crawler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/snowball/crawler_test.go`:

```go
func TestHandleEmitsCaptureEvent(t *testing.T) {
	q := func([]string, int32, ...lib.Modifier) ([]lib.AP, error) {
		return []lib.AP{{BSSID: "aa:bb:cc:dd:ee:01", Location: lib.Location{Lat: 1, Long: 2}}}, nil
	}
	c := newTestCrawler(t, q)
	var got []lib.AP
	c.onCapture = func(aps []lib.AP) { got = aps }
	if err := c.store.enqueue([]string{"aa:bb:cc:dd:ee:00"}); err != nil {
		t.Fatal(err)
	}

	c.handle(context.Background(), "aa:bb:cc:dd:ee:00")

	if len(got) != 1 {
		t.Fatalf("onCapture received %d aps, want 1", len(got))
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/snowball/ -run TestHandleEmitsCaptureEvent -v`
Expected: FAIL — `c.onCapture undefined`.

- [ ] **Step 3: Add the field and the call**

In `cmd/snowball/crawler.go`, add a field to the `crawler` struct:

```go
type crawler struct {
	store       *store
	backoff     *backoff
	query       queryFunc
	opts        []lib.Modifier
	maxAttempts int
	onCapture   func([]lib.AP) // optional; called with each batch of discovered APs
}
```

In `handle`, in the success path right after `c.store.addAPs(aps)` (the block starting at `c.backoff.reset()`), add the hook call before the `next := make(...)` line:

```go
		c.backoff.reset()
		if err := c.store.addAPs(aps); err != nil {
			log.Printf("addAPs: %v", err)
		}
		if c.onCapture != nil {
			c.onCapture(aps)
		}
		next := make([]string, len(aps))
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./cmd/snowball/ -run TestHandle -v`
Expected: PASS (new test plus the existing `TestHandle*`).

- [ ] **Step 5: Commit**

```bash
git add cmd/snowball/crawler.go cmd/snowball/crawler_test.go
git commit -m "feat(snowball): emit capture events from crawler.handle" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Frontend assets + embed

**Files:**
- Create: `cmd/snowball/assets.go`
- Create: `cmd/snowball/web/index.html`
- Create: `cmd/snowball/web/style.css`
- Create: `cmd/snowball/web/app.js`

No unit test; verified by `go build` here and by the server test in Task 8.

- [ ] **Step 1: Create the embed file**

Create `cmd/snowball/assets.go`:

```go
package main

import "embed"

//go:embed web
var webFS embed.FS
```

- [ ] **Step 2: Create `cmd/snowball/web/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>snowball — live</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <link rel="stylesheet" href="https://unpkg.com/uplot@1.6.30/dist/uPlot.min.css" />
  <link rel="stylesheet" href="style.css" />
</head>
<body>
  <div id="map" aria-label="Map of captured access points"></div>

  <header id="topbar">
    <div class="brand">
      <span class="dot"></span> snowball
    </div>
    <div class="tiles">
      <div class="tile"><span class="label">Access points</span><span class="value" id="m-aps">0</span></div>
      <div class="tile">
        <span class="label">Captures/s</span>
        <span class="value" id="m-rate">0.0</span>
        <div id="spark"></div>
      </div>
      <div class="tile"><span class="label">Pending</span><span class="value" id="m-pending">0</span></div>
      <div class="tile"><span class="label">In-flight</span><span class="value" id="m-inflight">0</span></div>
    </div>
    <div class="status" id="status" role="status">
      <span class="status-dot"></span><span id="status-text">CONNECTING</span>
    </div>
  </header>

  <aside id="panel">
    <section class="control">
      <label for="workers">Workers <span id="workers-val">0</span> / <span id="workers-max">0</span></label>
      <input type="range" id="workers" min="0" max="0" value="0" step="1" />
      <p class="hint">Active: <span id="workers-active">0</span></p>
    </section>
    <section class="feed">
      <h2>Live captures</h2>
      <ul id="feed"></ul>
    </section>
  </aside>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
  <script src="https://unpkg.com/uplot@1.6.30/dist/uPlot.iife.min.js"></script>
  <script src="app.js"></script>
</body>
</html>
```

- [ ] **Step 3: Create `cmd/snowball/web/style.css`**

```css
:root {
  --bg: #0F172A;
  --surface: #1E293B;
  --muted: #272F42;
  --border: #475569;
  --fg: #F8FAFC;
  --fg-dim: #94A3B8;
  --accent: #22C55E;
  --warn: #F59E0B;
  --danger: #EF4444;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body {
  font-family: Inter, system-ui, sans-serif;
  background: var(--bg);
  color: var(--fg);
  overflow: hidden;
}
#map { position: absolute; inset: 0; background: var(--bg); }
.leaflet-container { background: var(--bg); }

#topbar {
  position: absolute; top: 0; left: 0; right: 0; z-index: 1000;
  display: flex; align-items: center; gap: 24px;
  padding: 12px 16px;
  background: linear-gradient(180deg, rgba(15,23,42,0.92), rgba(15,23,42,0.0));
  pointer-events: none;
}
#topbar .brand, #topbar .tiles, #topbar .status { pointer-events: auto; }
.brand { font-weight: 700; letter-spacing: 0.04em; display: flex; align-items: center; gap: 8px; }
.brand .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 10px var(--accent); }

.tiles { display: flex; gap: 12px; flex: 1; }
.tile {
  background: rgba(30,41,59,0.85); border: 1px solid var(--border); border-radius: 10px;
  padding: 8px 14px; min-width: 110px; position: relative;
}
.tile .label { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--fg-dim); }
.tile .value { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
#spark { position: absolute; right: 8px; bottom: 6px; width: 70px; height: 24px; }

.status {
  display: flex; align-items: center; gap: 8px;
  background: rgba(30,41,59,0.85); border: 1px solid var(--border); border-radius: 999px;
  padding: 8px 14px; font-size: 12px; font-weight: 600; letter-spacing: 0.05em;
}
.status-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--fg-dim); }
.status.running .status-dot { background: var(--accent); box-shadow: 0 0 8px var(--accent); }
.status.throttled .status-dot { background: var(--warn); box-shadow: 0 0 8px var(--warn); }
.status.down .status-dot { background: var(--danger); }

#panel {
  position: absolute; top: 80px; right: 16px; bottom: 16px; z-index: 1000;
  width: 320px; display: flex; flex-direction: column; gap: 12px;
}
.control, .feed {
  background: rgba(30,41,59,0.9); border: 1px solid var(--border); border-radius: 12px; padding: 14px;
}
.control label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 10px; }
.control input[type=range] { width: 100%; accent-color: var(--accent); cursor: pointer; }
.control .hint { margin: 8px 0 0; font-size: 12px; color: var(--fg-dim); }
.feed { flex: 1; display: flex; flex-direction: column; min-height: 0; }
.feed h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--fg-dim); margin: 0 0 8px; }
#feed { list-style: none; margin: 0; padding: 0; overflow-y: auto; flex: 1; font-size: 12px; }
#feed li {
  display: flex; justify-content: space-between; gap: 8px;
  padding: 4px 0; border-bottom: 1px solid var(--muted);
  font-variant-numeric: tabular-nums; animation: flashin 240ms ease-out;
}
#feed li .mac { color: var(--fg); }
#feed li .loc { color: var(--fg-dim); }
@keyframes flashin { from { background: rgba(34,197,94,0.25); } to { background: transparent; } }

input:focus-visible, a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

@media (prefers-reduced-motion: reduce) {
  #feed li { animation: none; }
  .brand .dot, .status-dot { box-shadow: none; }
}
@media (max-width: 768px) {
  #panel { position: absolute; left: 16px; width: auto; top: auto; height: 40%; }
  .tiles { overflow-x: auto; }
}
```

- [ ] **Step 4: Create `cmd/snowball/web/app.js`**

```javascript
(() => {
  "use strict";
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // --- Map ---
  const map = L.map("map", { zoomControl: true, worldCopyJump: true }).setView([20, 0], 2);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "© OpenStreetMap, © CARTO",
    subdomains: "abcd",
    maxZoom: 19,
  }).addTo(map);
  const heat = L.heatLayer([], { radius: 18, blur: 22, maxZoom: 12 }).addTo(map);
  const heatPoints = [];
  const HEAT_CAP = 20000;

  function addPoints(points) {
    for (const p of points) {
      if (typeof p.lat !== "number" || typeof p.lon !== "number") continue;
      heatPoints.push([p.lat, p.lon, 0.6]);
      if (!reduceMotion) flashPing(p.lat, p.lon);
    }
    if (heatPoints.length > HEAT_CAP) heatPoints.splice(0, heatPoints.length - HEAT_CAP);
    heat.setLatLngs(heatPoints);
  }

  function flashPing(lat, lon) {
    const m = L.circleMarker([lat, lon], {
      radius: 5, color: "#22C55E", weight: 1, fillColor: "#22C55E", fillOpacity: 0.9,
    }).addTo(map);
    let op = 0.9;
    const id = setInterval(() => {
      op -= 0.09;
      if (op <= 0) { clearInterval(id); map.removeLayer(m); return; }
      m.setStyle({ fillOpacity: op, opacity: op });
    }, 80);
  }

  // --- Sparkline (rate) ---
  const sparkData = [[], []];
  const SPARK_LEN = 120;
  let t = 0;
  const spark = new uPlot({
    width: 70, height: 24, cursor: { show: false }, legend: { show: false },
    axes: [{ show: false }, { show: false }],
    scales: { x: { time: false } },
    series: [{}, { stroke: "#22C55E", width: 1.5, fill: "rgba(34,197,94,0.15)" }],
  }, sparkData, document.getElementById("spark"));

  function pushRate(rate) {
    sparkData[0].push(t++); sparkData[1].push(rate);
    if (sparkData[0].length > SPARK_LEN) { sparkData[0].shift(); sparkData[1].shift(); }
    spark.setData(sparkData);
  }

  // --- Feed ---
  const feed = document.getElementById("feed");
  const FEED_CAP = 60;
  function pushFeed(points) {
    for (const p of points.slice(-12)) {
      const li = document.createElement("li");
      const mac = document.createElement("span"); mac.className = "mac"; mac.textContent = p.b;
      const loc = document.createElement("span"); loc.className = "loc";
      loc.textContent = p.lat.toFixed(3) + ", " + p.lon.toFixed(3);
      li.append(mac, loc);
      feed.prepend(li);
    }
    while (feed.children.length > FEED_CAP) feed.removeChild(feed.lastChild);
  }

  // --- Stats / status ---
  const el = (id) => document.getElementById(id);
  let sliderActive = false;
  function applyStats(s) {
    el("m-aps").textContent = s.aps.toLocaleString();
    el("m-rate").textContent = s.rate.toFixed(1);
    el("m-pending").textContent = s.pending.toLocaleString();
    el("m-inflight").textContent = s.inflight.toLocaleString();
    el("workers-active").textContent = s.workers;
    pushRate(s.rate);

    const slider = el("workers");
    el("workers-max").textContent = s.max;
    slider.max = String(s.max);
    if (!sliderActive) { slider.value = String(s.target); el("workers-val").textContent = s.target; }

    const status = el("status");
    const text = el("status-text");
    status.classList.remove("running", "throttled", "down");
    if (s.throttled) { status.classList.add("throttled"); text.textContent = "THROTTLED (" + Math.round(s.backoffMs / 1000) + "s)"; }
    else { status.classList.add("running"); text.textContent = "RUNNING"; }
  }

  // --- Workers control ---
  const slider = el("workers");
  slider.addEventListener("input", () => { sliderActive = true; el("workers-val").textContent = slider.value; });
  let sendTimer = null;
  slider.addEventListener("change", () => {
    sliderActive = false;
    clearTimeout(sendTimer);
    sendTimer = setTimeout(() => send({ type: "setWorkers", n: Number(slider.value) }), 50);
  });

  // --- WebSocket ---
  let ws = null;
  function send(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
  function setDown() {
    const status = el("status");
    status.classList.remove("running", "throttled"); status.classList.add("down");
    el("status-text").textContent = "DISCONNECTED";
  }
  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(proto + "://" + location.host + "/ws" + location.search);
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "snapshot") { applyStats(msg.stats); addPoints(msg.points); }
      else if (msg.type === "stats") { applyStats(msg); }
      else if (msg.type === "capture") { addPoints(msg.points); pushFeed(msg.points); }
    };
    ws.onclose = () => { setDown(); setTimeout(connect, 2000); };
    ws.onerror = () => ws.close();
  }
  connect();
})();
```

- [ ] **Step 5: Verify it compiles (embed needs the files present)**

Run: `go build ./cmd/snowball/`
Expected: builds without error (the `webFS` embed resolves).

- [ ] **Step 6: Commit**

```bash
git add cmd/snowball/assets.go cmd/snowball/web/
git commit -m "feat(snowball): add embedded dashboard frontend assets" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: WebSocket + HTTP server

**Files:**
- Modify: `go.mod`, `go.sum` (add `gorilla/websocket`)
- Create: `cmd/snowball/server.go`
- Test: `cmd/snowball/server_test.go`

Depends on Tasks 3, 4, 5, 7.

- [ ] **Step 1: Add the WebSocket dependency**

Run: `go get github.com/gorilla/websocket@v1.5.3`
Expected: `go.mod`/`go.sum` updated.

- [ ] **Step 2: Write the failing tests**

Create `cmd/snowball/server_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURL(httpURL string) string { return "ws" + strings.TrimPrefix(httpURL, "http") }

func testServer(t *testing.T, token string) *server {
	t.Helper()
	st := newTestStore(t)
	p := newPool(func(context.Context, string) {}, 50, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p.start(ctx, 0)
	return newServer(serverDeps{
		hub:     newHub(),
		store:   st,
		pool:    p,
		backoff: newBackoff(time.Second, time.Minute),
		token:   token,
		assets:  webFS,
		persist: func(int) {},
	})
}

func TestServerHealthz(t *testing.T) {
	ts := httptest.NewServer(testServer(t, "").handler())
	defer ts.Close()
	res, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
}

func TestServerServesIndex(t *testing.T) {
	ts := httptest.NewServer(testServer(t, "").handler())
	defer ts.Close()
	res, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
}

func TestServerWSSnapshot(t *testing.T) {
	ts := httptest.NewServer(testServer(t, "").handler())
	defer ts.Close()
	c, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "snapshot" {
		t.Fatalf("first message type = %v, want snapshot", m["type"])
	}
}

func TestServerSetWorkers(t *testing.T) {
	s := testServer(t, "")
	ts := httptest.NewServer(s.handler())
	defer ts.Close()
	c, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.ReadMessage(); err != nil { // consume snapshot
		t.Fatal(err)
	}
	if err := c.WriteJSON(map[string]any{"type": "setWorkers", "n": 3}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.pool.getTarget() == 3 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pool target = %d, want 3", s.pool.getTarget())
}

func TestServerTokenRequired(t *testing.T) {
	ts := httptest.NewServer(testServer(t, "secret").handler())
	defer ts.Close()
	if _, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws", nil); err == nil {
		t.Fatal("expected handshake failure without token")
	}
	c, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws?token=secret", nil)
	if err != nil {
		t.Fatalf("dial with token: %v", err)
	}
	c.Close()
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./cmd/snowball/ -run TestServer -v`
Expected: FAIL — `newServer undefined`, `serverDeps undefined`.

- [ ] **Step 4: Implement the server**

Create `cmd/snowball/server.go`:

```go
package main

import (
	"encoding/json"
	"io/fs"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

type serverDeps struct {
	hub     *hub
	store   *store
	pool    *pool
	backoff *backoff
	token   string
	assets  fs.FS
	persist func(int)
}

type server struct {
	serverDeps
	upgrader websocket.Upgrader
}

func newServer(d serverDeps) *server {
	return &server{
		serverDeps: d,
		upgrader:   websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
}

// handler builds the HTTP router: static dashboard, health check, and WS.
func (s *server) handler() http.Handler {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.GET("/healthz", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })
	e.GET("/ws", s.handleWS)
	sub, err := fs.Sub(s.assets, "web")
	if err != nil {
		panic(err) // embed path is a compile-time constant; this cannot happen at runtime
	}
	e.GET("/*", echo.WrapHandler(http.FileServer(http.FS(sub))))
	return e
}

// setWorkers applies a new target to the live pool and persists the clamped value.
func (s *server) setWorkers(n int) {
	s.pool.setTarget(n)
	if s.persist != nil {
		s.persist(s.pool.getTarget())
	}
}

func (s *server) handleWS(c echo.Context) error {
	if s.token != "" && c.QueryParam("token") != s.token {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	conn, err := s.upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return nil // upgrade already wrote the response
	}
	defer conn.Close()

	sub := s.hub.subscribe(64)
	defer s.hub.unsubscribe(sub)

	if snap, err := buildSnapshot(s.store, s.pool, s.backoff, 500); err == nil {
		_ = conn.WriteMessage(websocket.TextMessage, snap)
	}

	// Reader: handle control messages; closing conn unblocks the writer.
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				return
			}
			var ctl struct {
				Type string `json:"type"`
				N    int    `json:"n"`
			}
			if json.Unmarshal(data, &ctl) == nil && ctl.Type == "setWorkers" {
				s.setWorkers(ctl.N)
			}
		}
	}()

	// Writer: pump hub messages to the client until the channel closes (slow
	// client dropped) or the write fails (client gone).
	for msg := range sub.ch {
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return nil
		}
	}
	return nil
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./cmd/snowball/ -run TestServer -v`
Expected: PASS (all five).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum cmd/snowball/server.go cmd/snowball/server_test.go
git commit -m "feat(snowball): add embedded dashboard HTTP/WebSocket server" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: Wire it all together in main.go

**Files:**
- Modify: `cmd/snowball/main.go`

Depends on all prior tasks. This replaces the fixed worker loop with the pool, replaces `stats()` with `broadcastLoop`, starts the server, and loads the persisted worker target.

- [ ] **Step 1: Replace the imports**

Set the import block in `cmd/snowball/main.go` to:

```go
import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"wloc/lib"
)
```

- [ ] **Step 2: Replace `main()`**

Replace the entire `func main()` with:

```go
func main() {
	dbPath := flag.String("db", "snowball.db", "path to the SQLite database")
	workers := flag.Int("workers", 15, "initial worker count when none is persisted")
	maxWorkers := flag.Int("max-workers", 200, "hard ceiling for the worker count")
	maxAttempts := flag.Int("max-attempts", 5, "give up on a BSSID after this many non-rate-limit failures")
	statsEvery := flag.Duration("stats-interval", 30*time.Second, "how often to log/broadcast stats")
	listen := flag.String("listen", "127.0.0.1:8080", "dashboard listen address")
	token := flag.String("token", "", "shared secret required to connect/control the dashboard when set")
	flag.Parse()

	store, err := openDB(*dbPath)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer store.close()

	if err := store.resetInflight(); err != nil {
		log.Fatalf("reset in-flight work: %v", err)
	}

	ouis, err := loadOUIs()
	if err != nil {
		log.Fatalf("load OUIs: %v", err)
	}
	log.Printf("loaded %d OUI prefixes for random seeding", len(ouis))

	bo := newBackoff(time.Second, 5*time.Minute)
	buf := newCaptureBuffer(2000)
	c := &crawler{
		store:       store,
		backoff:     bo,
		query:       lib.QueryBssid,
		maxAttempts: *maxAttempts,
		onCapture:   buf.add,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Dynamic worker pool. The initial target comes from the persisted setting,
	// falling back to the -workers flag.
	p := newPool(c.handle, *maxWorkers, *maxWorkers)
	initial := *workers
	if v, err := store.getSetting("workers", ""); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			initial = n
		}
	}
	p.start(ctx, initial)

	hub := newHub()
	persist := func(n int) {
		if err := store.setSetting("workers", strconv.Itoa(n)); err != nil {
			log.Printf("persist workers: %v", err)
		}
	}
	persist(p.getTarget())

	srv := newServer(serverDeps{
		hub: hub, store: store, pool: p, backoff: bo,
		token: *token, assets: webFS, persist: persist,
	})

	go broadcastLoop(ctx, hub, store, p, bo, buf, *statsEvery)
	go func() {
		log.Printf("dashboard listening on http://%s", *listen)
		if err := http.ListenAndServe(*listen, srv.handler()); err != nil && err != http.ErrServerClosed {
			log.Printf("dashboard server: %v", err)
		}
	}()

	dispatch(ctx, c, ouis, p)

	log.Println("shutting down, waiting for workers...")
	close(p.work)
	p.wait()
	log.Println("stopped")
}
```

- [ ] **Step 3: Replace `dispatch()` to feed the pool**

Replace the existing `func dispatch(...)` with:

```go
// dispatch claims pending BSSIDs and feeds them to the worker pool. When the
// frontier is fully drained and no work is in flight, it injects one random
// seed to keep the snowball alive.
func dispatch(ctx context.Context, c *crawler, ouis []string, p *pool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := p.getTarget()
		if batch < 1 {
			batch = 1
		}

		claimed, err := c.store.claim(batch)
		if err != nil {
			log.Printf("claim: %v", err)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}

		if len(claimed) == 0 {
			inflight, err := c.store.inflightCount()
			if err != nil {
				log.Printf("inflightCount: %v", err)
			}
			if inflight == 0 {
				seed := randomBSSID(ouis)
				if err := c.store.enqueue([]string{seed}); err != nil {
					log.Printf("enqueue seed: %v", err)
				} else {
					log.Printf("frontier empty, seeded random BSSID %s", seed)
				}
			} else if !sleepCtx(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}

		for _, b := range claimed {
			select {
			case p.work <- b:
			case <-ctx.Done():
				return
			}
		}
	}
}
```

- [ ] **Step 4: Replace `stats()` with `broadcastLoop()`**

Delete the existing `func stats(...)` and add:

```go
// broadcastLoop pushes periodic stats and batched captures to dashboard clients
// and logs the same stats line as before. It owns the capture-flush cadence so
// a burst of discoveries is coalesced into one message per tick.
func broadcastLoop(ctx context.Context, h *hub, st *store, p *pool, b *backoff, buf *captureBuffer, every time.Duration) {
	statsT := time.NewTicker(every)
	capT := time.NewTicker(500 * time.Millisecond)
	defer statsT.Stop()
	defer capT.Stop()
	var last int
	for {
		select {
		case <-ctx.Done():
			return
		case <-capT.C:
			pts := buf.flush()
			if len(pts) == 0 {
				continue
			}
			if msg, err := json.Marshal(captureMsg{Type: "capture", Points: pts}); err == nil {
				h.broadcast(msg)
			}
		case <-statsT.C:
			aps, err := st.apCount()
			if err != nil {
				log.Printf("stats apCount: %v", err)
				continue
			}
			rate := float64(aps-last) / every.Seconds()
			last = aps
			pending, _ := st.pendingCount()
			log.Printf("stats: %d access points (+%.1f/s), %d pending", aps, rate, pending)
			s, err := buildStats(st, p, b, rate)
			if err != nil {
				continue
			}
			if msg, err := json.Marshal(s); err == nil {
				h.broadcast(msg)
			}
		}
	}
}
```

Keep the existing `sleepCtx` helper as-is.

- [ ] **Step 5: Build, vet, and run the full test suite**

Run: `go build ./cmd/snowball/ && go vet ./cmd/snowball/ && go test ./cmd/snowball/...`
Expected: build succeeds, vet is clean, all tests PASS (Tasks 1–8 plus the pre-existing suite).

- [ ] **Step 6: Manual smoke test**

Run:
```bash
go run ./cmd/snowball -db /tmp/snowball-smoke.db -listen 127.0.0.1:8080
```
Then open `http://127.0.0.1:8080` in a browser. Expected within ~30s: the status pill reads RUNNING (or THROTTLED), the Access points tile climbs, green pings flash onto the map, the feed scrolls, and dragging the Workers slider changes the Active count. `Ctrl-C` shuts down cleanly ("stopped"). Remove the smoke DB afterward: `rm -f /tmp/snowball-smoke.db*`.

- [ ] **Step 7: Commit**

```bash
git add cmd/snowball/main.go
git commit -m "feat(snowball): wire dashboard, dynamic pool, and event broadcast into main" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: Docker resource caps + docs

**Files:**
- Modify: `docker-compose.yml`
- Modify: `cmd/snowball/Dockerfile`
- Modify: `cmd/snowball/README.md`

- [ ] **Step 1: Add resource caps and port to docker-compose.yml**

Replace the `services:` block in `docker-compose.yml` with:

```yaml
services:
  snowball:
    build:
      context: .
      dockerfile: cmd/snowball/Dockerfile
    image: snowball
    container_name: snowball
    restart: unless-stopped
    # Run as the host user so the bind-mounted ./data is writable and the DB
    # files stay owned by you. Change if your uid:gid differs from 1000:1000.
    user: "1000:1000"
    # Inside the container bind the dashboard on all interfaces; on the host it
    # is published to localhost only (see ports below). Set SNOWBALL_TOKEN and
    # add `-token ${SNOWBALL_TOKEN}` to expose it more widely.
    command: ["-db", "/data/snowball.db", "-listen", ":8080"]
    ports:
      - "127.0.0.1:8080:8080"
    volumes:
      - ./data:/data
    # Host resource caps so the crawler is a good neighbour. The workload is
    # network-I/O-bound, so CPU is mostly a guardrail; tune to taste.
    deploy:
      resources:
        limits:
          cpus: "1.0"
          memory: 256M
        reservations:
          memory: 64M
```

- [ ] **Step 2: Expose the port in the Dockerfile**

In `cmd/snowball/Dockerfile`, add `EXPOSE 8080` immediately before the `USER snowball` line:

```dockerfile
EXPOSE 8080
USER snowball
```

- [ ] **Step 3: Document the dashboard in the README**

In `cmd/snowball/README.md`, add this section after the "Build & run" flags table:

```markdown
## Live dashboard

The service serves a real-time web dashboard from the same binary. By default it
binds `127.0.0.1:8080` (localhost only). Open `http://127.0.0.1:8080` to watch
captured access points light up a dark world map, see live stats (total APs,
captures/s, pending, in-flight, throttle state), and adjust the worker count on
the fly with the Workers slider — the chosen value persists across restarts.

Extra flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `-listen` | `127.0.0.1:8080` | dashboard listen address (use `:8080` to bind all interfaces) |
| `-max-workers` | `200` | hard ceiling the Workers slider can reach |
| `-token` | _(empty)_ | shared secret required on the dashboard/WebSocket when set |

Because the dashboard can change crawler behaviour, it binds localhost by
default. To reach it from another machine, set `-listen :8080` and a `-token`,
then connect to `http://host:8080/?token=YOURTOKEN`.
```

Also update the Docker run example note to mention the published port (the
`docker run` example should add `-p 127.0.0.1:8080:8080`).

- [ ] **Step 4: Validate compose config**

Run: `docker compose config`
Expected: prints the merged config with no errors (validates the `deploy.resources` and `ports` syntax). If Docker is unavailable in the environment, skip with a note.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml cmd/snowball/Dockerfile cmd/snowball/README.md
git commit -m "feat(snowball): publish dashboard port and add host resource caps" \
  -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Real-time stats + map + feed over WebSocket → Tasks 4, 5, 7, 8, 9 (hub, events, frontend, server, broadcast loop). ✅
- Runtime-adjustable worker concurrency, no restart → Task 3 (pool), Task 8 (`setWorkers`), Task 9 (slider wiring). ✅
- Worker target persists across restarts → Task 1 (`settings`), Task 8 (`persist`), Task 9 (load on boot). ✅
- Snapshot seeds the map on connect → Task 1 (`recentAPs`), Task 5 (`buildSnapshot`), Task 8 (sent on upgrade). ✅
- Capture firehose bounded → Task 5 (`captureBuffer` cap), Task 9 (500ms flush). ✅
- Backoff/throttle visibility → Task 2 (`snapshot`), Task 5 (`buildStats`), Task 7 (status pill). ✅
- Slow-client isolation (never blocks crawler) → Task 4 (`hub.broadcast` drops slow subs) + test. ✅
- Bind localhost by default, optional token → Task 8 (token check), Task 9 (`-listen`/`-token` defaults). ✅
- Host resource caps → Task 10. ✅
- Single binary, no Node build → Task 7 (`go:embed`, CDN libs). ✅
- Existing tests stay green → Task 9 Step 5 runs the full suite. ✅

**Placeholder scan:** No TBD/TODO; every code step contains complete code and exact commands.

**Type consistency:**
- `pool`: `newPool(handle, max, workBuf)`, `start`, `setTarget`, `getTarget`, `getActive`, `maxTarget`, `wait`, exported field `work` — used identically in Tasks 5, 8, 9.
- `hub`: `newHub`, `subscribe(buffer)`, `unsubscribe`, `broadcast`, `count`; `subscriber.ch` — consistent across Tasks 4, 8.
- `statsMsg` JSON: `workers` = active, `target`, `max` — `app.js` reads `s.workers`/`s.target`/`s.max` matching the struct tags.
- `serverDeps` fields (`hub, store, pool, backoff, token, assets, persist`) match `newServer` usage in Tasks 8 and 9.
- `crawler.onCapture func([]lib.AP)` matches `buf.add` signature in Task 9.

No gaps found.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-01-snowball-realtime-dashboard.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
