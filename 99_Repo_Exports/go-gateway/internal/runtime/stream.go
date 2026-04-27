package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

type StreamHub struct {
	sb       *SnapshotBuilder
	rdb      *redis.Client
	interval time.Duration
	limitDOM int

	sseMu   sync.RWMutex
	sseSubs map[chan []byte]struct{}

	upgrader websocket.Upgrader
	wsMu     sync.RWMutex
	wsSubs   map[*websocket.Conn]struct{}
}

func NewStreamHub(rdb *redis.Client, sb *SnapshotBuilder, interval time.Duration, limitDOM int) *StreamHub {
	return &StreamHub{
		sb:       sb,
		rdb:      rdb,
		interval: interval,
		limitDOM: limitDOM,
		sseSubs:  make(map[chan []byte]struct{}),
		wsSubs:   make(map[*websocket.Conn]struct{}),
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
}

func (h *StreamHub) Start(ctx context.Context) {
	t := time.NewTicker(h.interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				h.tick(ctx)
			}
		}
	}()
}

func (h *StreamHub) tick(ctx context.Context) {
	snap, err := h.sb.Build(ctx, h.limitDOM)
	if err != nil {
		return
	}
	payload, _ := json.Marshal(struct {
		Type string   `json:"type"`
		Data Snapshot `json:"data"`
	}{"snapshot", snap})

	h.sseMu.RLock()
	for ch := range h.sseSubs {
		select {
		case ch <- payload:
		default:
		}
	}
	h.sseMu.RUnlock()

	h.wsMu.RLock()
	for c := range h.wsSubs {
		_ = c.WriteMessage(websocket.TextMessage, payload)
	}
	h.wsMu.RUnlock()
}

func (h *StreamHub) SSEHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 8)
	h.sseMu.Lock()
	h.sseSubs[ch] = struct{}{}
	h.sseMu.Unlock()
	defer func() { h.sseMu.Lock(); delete(h.sseSubs, ch); h.sseMu.Unlock(); close(ch) }()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	ctx := r.Context()
	go func() { h.tick(ctx) }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if _, err := w.Write([]byte("event: ping\ndata: {}\n\n")); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case msg := <-ch:
			_, _ = w.Write([]byte("event: snapshot\n"))
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(msg)
			_, _ = w.Write([]byte("\n\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}

func (h *StreamHub) WSHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.wsMu.Lock()
	h.wsSubs[conn] = struct{}{}
	h.wsMu.Unlock()

	ctx := r.Context()
	go func() { h.tick(ctx) }()

	go func() {
		defer func() { h.wsMu.Lock(); delete(h.wsSubs, conn); h.wsMu.Unlock(); _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				zap.S().Errorf("WS closed: %v", err)
				return
			}
		}
	}()
}
