package main

import "sync"

// subscriber is one connected dashboard client. Messages are delivered on ch.
type subscriber struct {
	ch chan []byte
}

// hub is an in-process publish/subscribe fan-out. broadcast is non-blocking: a
// subscriber whose buffer is full is dropped, so a slow or dead browser can
// never block the crawler that publishes events.
type hub struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[*subscriber]struct{})}
}

func (h *hub) subscribe(buffer int) *subscriber {
	s := &subscriber{ch: make(chan []byte, buffer)}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *hub) unsubscribe(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
	}
}

func (h *hub) broadcast(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- msg:
		default:
			// Slow consumer: drop it rather than block the publisher.
			delete(h.subs, s)
			close(s.ch)
		}
	}
}

func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
