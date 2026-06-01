# Snowball Real-Time Dashboard — Design

**Date:** 2026-06-01
**Status:** Approved (pending spec review)
**Component:** `cmd/snowball`

## Summary

Add a live, map-led operations dashboard to the existing `snowball` BSSID
crawler. The dashboard streams captured access points and crawler stats to the
browser in real time over WebSockets, and lets the operator change the active
worker count (compute allocation) on a running instance without a restart. The
dashboard is served by the existing single Go binary; no separate process and no
Node build step.

This work also adds host-level resource caps to `docker-compose.yml` so the
crawler is a good neighbor on a shared server.

## Background

`cmd/snowball` already continuously discovers Wi-Fi APs via Apple's WLOC API
using breadth-first "snowball" expansion, runs 24/7, persists all state in
SQLite, and is dockerized with a fixed-size worker pool (`-workers`, default 15).
What is missing: (1) any visibility into what it is doing in real time, and
(2) the ability to change how hard it works while it runs.

Key facts that shape the design:

- The workload is **network-I/O-bound, not CPU-bound**. Each worker waits on an
  HTTPS call to Apple, which rate-limits with HTTP 503. The crawler already
  applies *shared* exponential backoff across the whole pool. Past a point, more
  workers do not add throughput — they just back off together. "Allocate
  compute" therefore means "set concurrency / request pressure," bounded by
  Apple's throttling, not by host CPU.
- Every captured AP has a `BSSID` and a `lat`/`lon` (`lib.AP` in
  `lib/types.go`), stored in the `access_points` table. Apple returns ~100
  geographically-clustered APs per successful query, so a map will visibly
  "light up" regions as the snowball spreads.
- It is a single Go binary / single container. `labstack/echo` and `a-h/templ`
  are already in `go.mod` (currently unused).

## Goals

1. Real-time browser dashboard: live stats + a map of captured APs + a feed of
   freshly discovered BSSIDs, pushed over WebSocket.
2. Runtime-adjustable worker concurrency from the dashboard, with no restart;
   the chosen value persists across restarts.
3. Host resource caps via `docker-compose.yml`.
4. Stay a single binary / single container. No Node build step.

## Non-goals

- Authentication beyond an optional shared-secret token.
- Historical analytics, time-travel, or replay of past captures.
- Multi-crawler aggregation or a remote fan-out broker.
- Exposing frontier/queue internals in the UI.

## Approach

**Single binary, in-process event hub** (chosen). The crawler and web server run
in the same process. The crawler publishes events to an in-memory pub/sub hub; a
WebSocket handler fans them out to connected browsers. Worker-count changes from
the UI mutate the live pool directly through shared state.

Rejected alternatives:

- *Separate dashboard process polling SQLite* — not truly real-time; control
  becomes indirect (crawler polling a settings row); more moving parts.
- *Redis pub/sub sidecar* — only pays off with multiple crawlers or remote
  fan-out; adds infrastructure for no benefit on one server.

## Architecture

```
            ┌──────────────────────── snowball process ────────────────────────┐
 Apple WLOC │  worker pool (dynamic)  ──capture/processed events──▶  event hub  │
   ▲   │    │      ▲  reconciles to                                    │ fan-out│
   │   ▼    │      │  target count                                     ▼        │
 lib.QueryBssid    │                                          WebSocket clients │
            │   pool supervisor ◀── setWorkers ── echo HTTP server ◀──┐         │
            │   stats goroutine ──stats tick──▶ hub                   │ /ws /    │
            │   SQLite store (APs, frontier, settings)                │ browser  │
            └─────────────────────────────────────────────────────────┘
```

### New / changed files (all under `cmd/snowball/`)

- **`hub.go`** *(new)* — pub/sub event hub. Each subscriber gets a buffered
  channel; a slow or dead browser is dropped (or has messages dropped) and
  **never blocks the crawler**. This isolation is the central correctness
  property of the design.
- **`pool.go`** *(new)* — dynamic worker pool extracted from `main.go`. Holds an
  atomic `target` count; a supervisor goroutine spawns new workers or signals
  surplus workers to retire after their current item, reconciling active count
  to target. Target is clamped to `[0, maxWorkers]`.
- **`server.go`** *(new)* — echo HTTP server. Serves the dashboard, upgrades
  `/ws`, handles inbound control messages. Binds `127.0.0.1:8080` by default.
- **`web/`** *(new, embedded via `go:embed`)* — `index.html`, `app.js`,
  `style.css`. Ships inside the binary and image; no build pipeline.
- **`crawler.go`** *(changed)* — after `addAPs`, publish a `capture` event with
  the new points; feed processed/failed counters.
- **`db.go`** *(changed)* — add `recentAPs(limit)` (to seed the map on page
  load) and a `settings` key/value table so the chosen worker count survives a
  restart.
- **`main.go`** *(changed)* — wire hub + pool + server together; add flags
  `-listen`, `-max-workers`, optional `-token`. On boot, load the persisted
  worker target (falling back to `-workers` / default 15).

### Dependencies

- Added (server): `github.com/gorilla/websocket` for the WebSocket endpoint.
- Client (CDN, no build): **Leaflet** + dark CARTO tile layer + a heat layer for
  the map; **uPlot** (tiny) for streaming sparklines. (MapLibre vector tiles are
  a possible later swap; Leaflet chosen for reliability and zero build.)

## Events & data flow

All WebSocket messages are JSON with a `type` discriminator.

Server → client:

- `snapshot` (on connect) — current stats plus the last ~500 APs from the DB, so
  the map is populated immediately.
- `stats` (every 1s) — total APs, capture rate/s, pending, in-flight, failed,
  worker target, worker active count, and backoff state (whether throttled and
  the current backoff delay).
- `capture` — batched and rate-capped (≤ ~200 points/s, sampled) lat/lon points,
  so a 100-AP burst across many workers cannot firehose the socket.

Client → server:

- `{type:"setWorkers", n}` — `n` is clamped to `[0, maxWorkers]`, written to the
  atomic target, persisted to `settings`, and reconciled by the supervisor. The
  next `stats` message confirms the new active/target counts.

## UI / layout

Dark OLED operations console (background `#0F172A`, "running" accent `#22C55E`,
Inter font, status colors green/amber/red, subtle glow). Map-led full-bleed
layout with an overlaid top stat bar and a right-hand panel.

- **Top bar:** large tabular-figure metric tiles — Total APs, Rate/s (with a
  live uPlot sparkline), Pending, In-flight — and a status pill: green
  "RUNNING" / amber "THROTTLED (backing off Xs)". Status is never conveyed by
  color alone; the text label always states it.
- **Map (hero):** an all-time density heatmap plus a "recent pings" layer where
  new captures flash in the accent green and fade out — regions light up as the
  snowball spreads.
- **Right panel:** a fast scrolling **feed** of freshly captured BSSIDs (MAC +
  coordinates), and the **Workers control** — a slider/stepper from `0` to
  `maxWorkers` showing current value and active-vs-target.

Accessibility: respects `prefers-reduced-motion` (no flashing/fade when set),
WCAG-AA contrast, keyboard-operable worker control, focus states visible,
tabular figures for numeric columns to prevent layout shift.

## Controls, safety & resource caps

- **Runtime concurrency:** the Workers control sets the live target; persisted in
  the `settings` table so it survives restarts. `-max-workers` (default 200) is
  the hard safety ceiling.
- **Bind localhost by default:** this is a control plane that changes crawler
  behavior. `-listen` exposes it on another address; optional `-token` is a
  shared secret required on `/ws` and on `setWorkers` when exposed.
- **Host resource caps:** `docker-compose.yml` gains
  `deploy.resources.limits` (`cpus`, `memory`) and reservations; the dashboard
  port is published to `127.0.0.1` only by default. CPU caps are a guardrail —
  the real throughput throttle is worker count plus Apple's 503 backoff.

## Error handling

- Slow/dead WebSocket clients must never block the crawler: the hub uses
  per-client buffered channels and drops the client (or drops messages) on
  overflow rather than blocking publishers.
- If map tiles or a CDN library fail to load, the dashboard still renders stats
  and the feed (graceful degradation).
- `setWorkers` values out of range are clamped, not rejected with an error.
- Stats queries that error are logged and skipped for that tick, as today.

## Testing

- **hub:** fan-out delivers to all subscribers; a slow subscriber is dropped and
  does not block other subscribers or the publisher.
- **pool:** scaling target up spawns workers; scaling down retires workers;
  active count converges to target; target never leaves `[0, maxWorkers]`.
- **server:** WS handshake succeeds; `setWorkers` clamps and the resulting state
  is echoed in `stats`; token is enforced on `/ws` and `setWorkers` when set,
  ignored when unset.
- **db:** `recentAPs(limit)` returns most-recent APs; `settings` get/set round
  trips, including the worker-target default when unset.
- **crawler:** `handle` publishes a `capture` event with the returned points
  (verified via an injected fake hub).
- Existing `snowball` tests (`backoff`, `crawler`, `db`, `seed`) stay green.

## Open questions

None outstanding. Possible future swaps noted inline (MapLibre vector tiles;
plain points instead of heatmap; different `-max-workers` ceiling).
