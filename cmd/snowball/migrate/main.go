// Command migrate converts a legacy Python-wloc bssids.db into the Go snowball's
// SQLite schema, so the snowball can resume from previously collected data.
//
//	go run ./cmd/snowball/migrate -src cmd/wloc/bssids.db -dst snowball-migrated.db
//
// Mapping (see ../crawler.go for the live model this mirrors):
//
//	bssids(bssid, latitude, longitude) -> access_points(bssid=mac.Encode(...), lat, lon)
//	processed_macs(mac)                -> frontier(bssid=mac, processed=1)  // already queried
//	bssids.bssid not yet queried       -> frontier(bssid,    processed=0)  // pending work
//
// The destination must not already exist; this never mutates the source.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"wloc/lib/mac"

	_ "modernc.org/sqlite"
)

// access_points needs mac.Encode (Go), so it is streamed row-by-row from a
// dedicated read connection. frontier needs no encoding, so it is done in bulk
// SQL via ATTACH on the write connection.
const schema = `
CREATE TABLE access_points (
	bssid INTEGER PRIMARY KEY,
	lat REAL NOT NULL,
	lon REAL NOT NULL,
	discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE frontier (
	bssid TEXT PRIMARY KEY,
	processed INTEGER NOT NULL DEFAULT 0,
	attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_frontier_pending ON frontier(processed) WHERE processed = 0;`

func main() {
	src := flag.String("src", "cmd/wloc/bssids.db", "legacy Python-wloc bssids.db to read")
	dst := flag.String("dst", "snowball-migrated.db", "snowball.db to create (must not exist)")
	flag.Parse()

	if _, err := os.Stat(*dst); err == nil {
		log.Fatalf("destination %q already exists; refusing to overwrite", *dst)
	}

	out, err := sql.Open("sqlite", *dst)
	if err != nil {
		log.Fatalf("open dst: %v", err)
	}
	defer out.Close()
	out.SetMaxOpenConns(1) // ATTACH and pragmas must stick to one connection.

	for _, p := range []string{"PRAGMA journal_mode=WAL;", "PRAGMA synchronous=NORMAL;", schema} {
		if _, err := out.Exec(p); err != nil {
			log.Fatalf("init dst: %v", err)
		}
	}

	nAP, nSkip := copyAccessPoints(*src, out)
	log.Printf("access_points: %d inserted, %d skipped (unparseable BSSID)", nAP, nSkip)

	done, pending := copyFrontier(*src, out)
	log.Printf("frontier: %d done (already queried), %d pending (to expand)", done, pending)
	log.Printf("migration complete -> %s", *dst)
}

// copyAccessPoints streams every legacy bssids row through mac.Encode into the
// access_points table. A separate read-only handle keeps the SELECT cursor off
// the write connection (SQLite dislikes writing under an open cursor).
func copyAccessPoints(src string, out *sql.DB) (inserted, skipped int) {
	in, err := sql.Open("sqlite", "file:"+src+"?mode=ro")
	if err != nil {
		log.Fatalf("open src: %v", err)
	}
	defer in.Close()

	rows, err := in.Query("SELECT bssid, latitude, longitude FROM bssids")
	if err != nil {
		log.Fatalf("read bssids: %v", err)
	}
	defer rows.Close()

	tx, err := out.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO access_points (bssid, lat, lon) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}
	for rows.Next() {
		var b string
		var lat, lon float64
		if err := rows.Scan(&b, &lat, &lon); err != nil {
			log.Fatalf("scan: %v", err)
		}
		id, err := mac.Encode(b)
		if err != nil {
			skipped++
			continue
		}
		if _, err := stmt.Exec(id, lat, lon); err != nil {
			log.Fatalf("insert ap: %v", err)
		}
		inserted++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate bssids: %v", err)
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		log.Fatalf("commit aps: %v", err)
	}
	return inserted, skipped
}

// copyFrontier bulk-loads the frontier via ATTACH. processed_macs become "done"
// first so the OR IGNORE on the bssids pass leaves them done and only enqueues
// the never-queried BSSIDs as pending. length(mac)=17 filters a few malformed
// legacy rows.
func copyFrontier(src string, out *sql.DB) (done, pending int) {
	ctx := context.Background()
	conn, err := out.Conn(ctx)
	if err != nil {
		log.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "ATTACH DATABASE ? AS legacy", src); err != nil {
		log.Fatalf("attach: %v", err)
	}

	r, err := conn.ExecContext(ctx,
		"INSERT OR IGNORE INTO frontier (bssid, processed) SELECT mac, 1 FROM legacy.processed_macs WHERE length(mac) = 17")
	if err != nil {
		log.Fatalf("load processed_macs: %v", err)
	}
	n, _ := r.RowsAffected()
	done = int(n)

	r, err = conn.ExecContext(ctx,
		"INSERT OR IGNORE INTO frontier (bssid, processed) SELECT bssid, 0 FROM legacy.bssids")
	if err != nil {
		log.Fatalf("load bssids into frontier: %v", err)
	}
	n, _ = r.RowsAffected()
	pending = int(n)
	return done, pending
}
