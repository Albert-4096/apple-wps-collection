package main

import (
	"database/sql"
	"fmt"
	"sync"
	"wloc/lib"
	"wloc/lib/mac"

	_ "modernc.org/sqlite"
)

// store is the persistent state for the snowball crawler. All writes are
// serialized behind a mutex (the seedcrawl pattern); SQLite WAL mode keeps
// concurrent reads cheap.
type store struct {
	db   *sql.DB
	lock sync.Mutex
}

func openDB(path string) (*store, error) {
	d, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// A single connection keeps the write mutex and SQLite's locking in sync
	// and avoids "database is locked" churn under concurrency.
	d.SetMaxOpenConns(1)
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA synchronous=NORMAL;",
	}
	for _, p := range pragmas {
		if _, err := d.Exec(p); err != nil {
			return nil, fmt.Errorf("pragma %q: %w", p, err)
		}
	}
	schema := []string{
		`CREATE TABLE IF NOT EXISTS access_points (
			bssid INTEGER PRIMARY KEY,
			lat REAL NOT NULL,
			lon REAL NOT NULL,
			discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS frontier (
			bssid TEXT PRIMARY KEY,
			processed INTEGER NOT NULL DEFAULT 0,
			attempts INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_frontier_pending ON frontier(processed) WHERE processed = 0;`,
	}
	for _, stmt := range schema {
		if _, err := d.Exec(stmt); err != nil {
			return nil, fmt.Errorf("schema: %w", err)
		}
	}
	return &store{db: d}, nil
}

func (s *store) close() error {
	return s.db.Close()
}

// enqueue adds BSSIDs to the frontier as pending. INSERT OR IGNORE means a
// BSSID is queued at most once ever, so the frontier doubles as the "seen"
// set: already-processed BSSIDs are never resurrected.
func (s *store) enqueue(bssids []string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO frontier (bssid) VALUES (?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, b := range bssids {
		if _, err := stmt.Exec(b); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// pending returns up to limit BSSIDs that still need to be queried.
func (s *store) pending(limit int) ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	rows, err := s.db.Query("SELECT bssid FROM frontier WHERE processed = 0 LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// frontier.processed states.
const (
	statePending  = 0
	stateDone     = 1
	stateFailed   = 2
	stateInflight = 3
)

// claim atomically moves up to limit pending BSSIDs to the in-flight state and
// returns them, so concurrent dispatchers/workers never grab the same row.
func (s *store) claim(limit int) ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	rows, err := s.db.Query(
		`UPDATE frontier SET processed = ? WHERE bssid IN (
			SELECT bssid FROM frontier WHERE processed = ? LIMIT ?
		) RETURNING bssid`,
		stateInflight, statePending, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// resetInflight returns claimed-but-unfinished work to pending. Called at
// startup so BSSIDs in flight during a crash/shutdown are retried.
func (s *store) resetInflight() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec("UPDATE frontier SET processed = ? WHERE processed = ?", statePending, stateInflight)
	return err
}

func (s *store) pendingCount() (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM frontier WHERE processed = 0").Scan(&n)
	return n, err
}

func (s *store) inflightCount() (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM frontier WHERE processed = ?", stateInflight).Scan(&n)
	return n, err
}

func (s *store) markProcessed(bssid string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec("UPDATE frontier SET processed = ? WHERE bssid = ?", stateDone, bssid)
	return err
}

func (s *store) markFailed(bssid string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, err := s.db.Exec("UPDATE frontier SET processed = ? WHERE bssid = ?", stateFailed, bssid)
	return err
}

// incAttempts increments and returns the failure count for a frontier entry.
func (s *store) incAttempts(bssid string) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var n int
	err := s.db.QueryRow(
		"UPDATE frontier SET attempts = attempts + 1 WHERE bssid = ? RETURNING attempts",
		bssid,
	).Scan(&n)
	return n, err
}

// addAPs stores discovered access points, deduping on BSSID. Entries with an
// unparseable BSSID are skipped rather than aborting the batch.
func (s *store) addAPs(aps []lib.AP) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO access_points (bssid, lat, lon) VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, ap := range aps {
		id, err := mac.Encode(ap.BSSID)
		if err != nil {
			continue
		}
		if _, err := stmt.Exec(id, ap.Location.Lat, ap.Location.Long); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *store) apCount() (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM access_points").Scan(&n)
	return n, err
}
