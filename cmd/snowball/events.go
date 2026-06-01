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
