package main

import (
	"context"
	"log"
	"strings"
	"wloc/lib"
)

// queryFunc matches lib.QueryBssid; injectable for testing.
type queryFunc func(bssids []string, maxResults int32, options ...lib.Modifier) ([]lib.AP, error)

type crawler struct {
	store       *store
	backoff     *backoff
	query       queryFunc
	opts        []lib.Modifier
	maxAttempts int
}

// handle queries one BSSID and folds the results back into the store. On
// success it records the access points, enqueues the newly-returned BSSIDs as
// fresh frontier work, and marks this BSSID processed. Errors are retried with
// shared exponential backoff; rate limits (503) are retried indefinitely while
// other errors trip the poison limit after maxAttempts.
func (c *crawler) handle(ctx context.Context, bssid string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		aps, err := c.query([]string{bssid}, 0, c.opts...)
		if err != nil {
			d := c.backoff.fail()
			if !isRateLimit(err) {
				n, aerr := c.store.incAttempts(bssid)
				if aerr != nil {
					log.Printf("incAttempts(%s): %v", bssid, aerr)
				}
				if n >= c.maxAttempts {
					log.Printf("giving up on %s after %d attempts: %v", bssid, n, err)
					if err := c.store.markFailed(bssid); err != nil {
						log.Printf("markFailed(%s): %v", bssid, err)
					}
					return
				}
			}
			if !c.backoff.pause(ctx, d) {
				return // context cancelled during wait
			}
			continue
		}

		c.backoff.reset()
		if err := c.store.addAPs(aps); err != nil {
			log.Printf("addAPs: %v", err)
		}
		next := make([]string, len(aps))
		for i, ap := range aps {
			next[i] = ap.BSSID
		}
		if err := c.store.enqueue(next); err != nil {
			log.Printf("enqueue: %v", err)
		}
		if err := c.store.markProcessed(bssid); err != nil {
			log.Printf("markProcessed(%s): %v", bssid, err)
		}
		return
	}
}

// isRateLimit reports whether the error is Apple throttling us (503), which is
// transient and server-side rather than a problem with the queried BSSID.
func isRateLimit(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Service Unavailable") || strings.Contains(msg, "503")
}
