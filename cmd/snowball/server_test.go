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

func TestServerDisconnectUnsubscribes(t *testing.T) {
	s := testServer(t, "")
	ts := httptest.NewServer(s.handler())
	defer ts.Close()
	c, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReadMessage(); err != nil { // snapshot
		t.Fatal(err)
	}
	// No broadcasts happen on this hub. Closing the client must still tear down
	// the subscription (writer must not stay blocked on sub.ch forever).
	c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.hub.count() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("hub.count() = %d after disconnect, want 0", s.hub.count())
}
