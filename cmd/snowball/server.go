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

	// Reader: handle control messages. It closes done on exit so the writer
	// tears down promptly when the client disconnects (closing the conn does
	// not by itself unblock the writer's channel receive).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
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

	// Writer: pump hub messages until the client disconnects (done closed by
	// the reader), the hub drops a slow client (sub.ch closed), or a write
	// fails. The deferred conn.Close above then unblocks the reader.
	for {
		select {
		case <-done:
			return nil
		case msg, ok := <-sub.ch:
			if !ok {
				return nil
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return nil
			}
		}
	}
}
