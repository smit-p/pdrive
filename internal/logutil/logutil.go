// Package logutil provides a structured logging system for pdrive with
// multi-level log support (DEBUG, INFO, WARN, ERROR) and live log streaming
// via WebSocket. It wraps Go's standard log/slog with a ring-buffer handler
// that stores recent log entries in memory for the web UI.
package logutil

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Entry is a single log record exposed to the web UI.
type Entry struct {
	Time    string         `json:"time"`
	Level   string         `json:"level"`
	Message string         `json:"msg"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// RingHandler is an slog.Handler that:
//  1. Delegates to an inner handler (file/stderr output).
//  2. Stores the last N log entries in a ring buffer.
//  3. Broadcasts new entries to connected WebSocket clients.
type RingHandler struct {
	inner   slog.Handler
	mu      sync.Mutex
	entries []Entry
	head    int // next write position
	count   int
	cap     int
	subsMu  sync.RWMutex
	subs    map[chan Entry]struct{}
	attrs   []slog.Attr
	groups  []string
}

// NewRingHandler wraps an existing slog.Handler with a ring buffer of size n.
func NewRingHandler(inner slog.Handler, n int) *RingHandler {
	return &RingHandler{
		inner:   inner,
		entries: make([]Entry, n),
		cap:     n,
		subs:    make(map[chan Entry]struct{}),
	}
}

// Enabled reports whether the handler handles records at the given level.
func (h *RingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle processes a log record: stores it, broadcasts it, and delegates to inner.
func (h *RingHandler) Handle(ctx context.Context, r slog.Record) error {
	// Build entry.
	attrs := make(map[string]any)
	// Include pre-configured attrs (from WithAttrs).
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})

	e := Entry{
		Time:    r.Time.Format(time.RFC3339Nano),
		Level:   r.Level.String(),
		Message: r.Message,
	}
	if len(attrs) > 0 {
		e.Attrs = attrs
	}

	// Store in ring buffer.
	h.mu.Lock()
	h.entries[h.head] = e
	h.head = (h.head + 1) % h.cap
	if h.count < h.cap {
		h.count++
	}
	h.mu.Unlock()

	// Broadcast to WebSocket subscribers (non-blocking).
	h.subsMu.RLock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default: // drop if subscriber is slow
		}
	}
	h.subsMu.RUnlock()

	// Delegate to inner handler (file/stderr output).
	return h.inner.Handle(ctx, r)
}

// WithAttrs returns a new handler with the given attributes pre-set.
func (h *RingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &RingHandler{
		inner:   h.inner.WithAttrs(attrs),
		entries: h.entries,
		cap:     h.cap,
		subs:    h.subs,
		attrs:   append(h.attrs, attrs...),
		groups:  h.groups,
	}
}

// WithGroup returns a new handler with the given group name.
func (h *RingHandler) WithGroup(name string) slog.Handler {
	return &RingHandler{
		inner:   h.inner.WithGroup(name),
		entries: h.entries,
		cap:     h.cap,
		subs:    h.subs,
		attrs:   h.attrs,
		groups:  append(h.groups, name),
	}
}

// Recent returns the last n log entries (oldest first).
func (h *RingHandler) Recent(n int) []Entry {
	h.mu.Lock()
	defer h.mu.Unlock()

	if n > h.count {
		n = h.count
	}
	result := make([]Entry, n)
	start := (h.head - n + h.cap) % h.cap
	for i := 0; i < n; i++ {
		result[i] = h.entries[(start+i)%h.cap]
	}
	return result
}

// Subscribe returns a channel that receives new log entries.
// Call Unsubscribe to clean up.
func (h *RingHandler) Subscribe() chan Entry {
	ch := make(chan Entry, 64)
	h.subsMu.Lock()
	h.subs[ch] = struct{}{}
	h.subsMu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel and closes it.
func (h *RingHandler) Unsubscribe(ch chan Entry) {
	h.subsMu.Lock()
	delete(h.subs, ch)
	h.subsMu.Unlock()
	close(ch)
}

// ServeRecentLogs handles GET /api/logs — returns the last N log entries as JSON.
func (h *RingHandler) ServeRecentLogs(w http.ResponseWriter, r *http.Request) {
	entries := h.Recent(h.cap)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries) //nolint:errcheck
}

// ServeLogStream handles GET /api/logs/stream — upgrades to WebSocket for
// live log streaming. Uses a minimal WebSocket implementation over raw HTTP
// hijacking so we don't need an external dependency (gorilla/websocket).
func (h *RingHandler) ServeLogStream(w http.ResponseWriter, r *http.Request) {
	// Upgrade to WebSocket using raw hijack.
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		http.Error(w, "WebSocket upgrade failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	ch := h.Subscribe()
	defer h.Unsubscribe(ch)

	// Read goroutine to detect client disconnect.
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 256)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(entry)
			if err != nil {
				continue
			}
			if err := writeWebSocketText(conn, data); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
