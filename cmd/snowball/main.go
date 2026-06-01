// Command snowball is a 24/7 service that continuously discovers Wi-Fi access
// points via Apple's WLOC API using breadth-first "snowball" expansion: query a
// BSSID, store the nearby BSSIDs Apple returns, feed the new ones back into the
// queue, and repeat forever. All state lives in SQLite, so it resumes cleanly
// after a crash or reboot. It needs no launch arguments.
package main

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

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
