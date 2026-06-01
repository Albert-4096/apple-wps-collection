// Command snowball is a 24/7 service that continuously discovers Wi-Fi access
// points via Apple's WLOC API using breadth-first "snowball" expansion: query a
// BSSID, store the nearby BSSIDs Apple returns, feed the new ones back into the
// queue, and repeat forever. All state lives in SQLite, so it resumes cleanly
// after a crash or reboot. It needs no launch arguments.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"
	"wloc/lib"
)

func main() {
	dbPath := flag.String("db", "snowball.db", "path to the SQLite database")
	workers := flag.Int("workers", 15, "number of concurrent query workers")
	maxAttempts := flag.Int("max-attempts", 5, "give up on a BSSID after this many non-rate-limit failures")
	statsEvery := flag.Duration("stats-interval", 30*time.Second, "how often to log stats")
	flag.Parse()

	store, err := openDB(*dbPath)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer store.close()

	// Requeue anything left in-flight by a previous crash/shutdown.
	if err := store.resetInflight(); err != nil {
		log.Fatalf("reset in-flight work: %v", err)
	}

	ouis, err := loadOUIs()
	if err != nil {
		log.Fatalf("load OUIs: %v", err)
	}
	log.Printf("loaded %d OUI prefixes for random seeding", len(ouis))

	c := &crawler{
		store:       store,
		backoff:     newBackoff(time.Second, 5*time.Minute),
		query:       lib.QueryBssid,
		maxAttempts: *maxAttempts,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	work := make(chan string, *workers)
	var done = make(chan struct{}, *workers)
	for i := 0; i < *workers; i++ {
		go func() {
			for b := range work {
				c.handle(ctx, b)
			}
			done <- struct{}{}
		}()
	}

	go stats(ctx, store, *statsEvery)

	dispatch(ctx, c, ouis, work, *workers)

	// dispatch returned because ctx was cancelled and it closed work; wait for
	// workers to finish their current item and drain.
	log.Println("shutting down, waiting for workers...")
	for i := 0; i < *workers; i++ {
		<-done
	}
	log.Println("stopped")
}

// dispatch claims pending BSSIDs and feeds them to the worker pool. When the
// frontier is fully drained and no work is in flight, it injects one random
// seed to keep the snowball alive.
func dispatch(ctx context.Context, c *crawler, ouis []string, work chan<- string, batch int) {
	defer close(work)
	for {
		select {
		case <-ctx.Done():
			return
		default:
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
				// Dead end: nothing pending and nothing running. Re-seed.
				seed := randomBSSID(ouis)
				if err := c.store.enqueue([]string{seed}); err != nil {
					log.Printf("enqueue seed: %v", err)
				} else {
					log.Printf("frontier empty, seeded random BSSID %s", seed)
				}
			} else if !sleepCtx(ctx, 200*time.Millisecond) {
				// Workers are busy; give them a moment to enqueue neighbours.
				return
			}
			continue
		}

		for _, b := range claimed {
			select {
			case work <- b:
			case <-ctx.Done():
				return
			}
		}
	}
}

func stats(ctx context.Context, s *store, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var last int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			aps, err := s.apCount()
			if err != nil {
				log.Printf("stats apCount: %v", err)
				continue
			}
			pending, _ := s.pendingCount()
			rate := float64(aps-last) / every.Seconds()
			last = aps
			log.Printf("stats: %d access points (+%.1f/s), %d pending", aps, rate, pending)
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
