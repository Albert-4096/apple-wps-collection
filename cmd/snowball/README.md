# snowball

A long-running service that continuously discovers Wi-Fi access points through
Apple's WLOC API. It performs breadth-first "snowball" expansion: query a BSSID,
store the ~100 nearby BSSIDs Apple returns, feed the newly-seen ones back into
the queue, and repeat forever.

Unlike [`cmd/wloc/snowball_script.py`](../wloc/snowball_script.py) (which shells
out to the `wloc` CLI once per BSSID), this is a native Go service that calls
`lib.QueryBssid` directly with a concurrent worker pool, and is built to run 24/7
on a server.

## How it works

```
cold start → frontier empty → seed a random (real-OUI) BSSID
   → worker queries it via WLOC → Apple returns nearby BSSIDs
   → store them + enqueue the new ones → loop
   (frontier drains to empty → inject another random seed)
```

All state lives in a SQLite database (`snowball.db`), so the process resumes
cleanly after a crash or reboot — no arguments required.

- `access_points` — every discovered AP (`bssid` as int64, `lat`, `lon`).
- `frontier` — the BFS work queue, which doubles as the de-duplication "seen"
  set. `processed`: `0` pending, `1` done, `2` failed, `3` in-flight.

Random seeds use real vendor OUI prefixes (embedded from `top_ouis.txt`) so they
have a realistic chance of matching an AP in Apple's database; most still miss,
and the snowball grows from the neighbours of the occasional hit.

## Build & run

```sh
go build -o snowball ./cmd/snowball
./snowball                 # zero-config; writes ./snowball.db
```

Optional flags (all have sensible defaults):

| Flag | Default | Meaning |
|------|---------|---------|
| `-db` | `snowball.db` | SQLite database path |
| `-workers` | `15` | concurrent query workers |
| `-max-attempts` | `5` | give up on a BSSID after this many non-rate-limit failures |
| `-stats-interval` | `30s` | how often to log progress |

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

Apple rate-limits with HTTP 503; the service applies shared exponential backoff
(1s → 5m, with jitter) so the whole pool slows together and recovers
automatically. `Ctrl-C` / `SIGTERM` shuts down gracefully, finishing in-flight
queries before exiting.

## Running with Docker

The [`Dockerfile`](./Dockerfile) builds a static, CGO-free binary into a small
Alpine image (~57 MB) that runs as a non-root user. Build from the repository
root so the whole Go module is in the build context:

```sh
docker build -f cmd/snowball/Dockerfile -t snowball .
```

Run it with a named volume so the SQLite database survives restarts:

```sh
docker run -d --name snowball --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  -v snowball-data:/data snowball
docker logs -f snowball        # follow the stats output
```

The container's default command is `snowball -db /data/snowball.db`. Append
flags to override the defaults, e.g. `docker run ... snowball -workers 30`.

## Running as a systemd service

`/etc/systemd/system/snowball.service`:

```ini
[Unit]
Description=Apple WLOC snowball collector
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/snowball/snowball -db /var/lib/snowball/snowball.db
WorkingDirectory=/var/lib/snowball
Restart=always
RestartSec=10
User=snowball
Group=snowball

[Install]
WantedBy=multi-user.target
```

```sh
sudo useradd --system --home /var/lib/snowball --create-home snowball
sudo install -D snowball /opt/snowball/snowball
sudo systemctl enable --now snowball
journalctl -u snowball -f      # follow the stats output
```
