package paper

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Side string

const (
	SideLong  Side = "LONG"
	SideShort Side = "SHORT"
)

type Position struct {
	ID       string    `json:"id"`
	Symbol   string    `json:"symbol"`
	Side     Side      `json:"side"`
	Entry    float64   `json:"entry"`
	Lots     float64   `json:"lots"`
	Remain   float64   `json:"remain"`
	TPLevels []float64 `json:"tp_levels,omitempty"`
	TPSplits []float64 `json:"tp_splits,omitempty"`
	OpenedAt int64     `json:"opened_at"`
	ClosedAt *int64    `json:"closed_at,omitempty"`

	Realized   float64 `json:"realized"`
	Unrealized float64 `json:"unrealized"`
	MarkPrice  float64 `json:"mark_price"`

	History []Exec `json:"history"`
}

type Exec struct {
	At        int64   `json:"at"`
	Price     float64 `json:"price"`
	LotsDelta float64 `json:"lots_delta"`
	Note      string  `json:"note"`
}

type Specs interface {
	Point() float64
	TickValuePerLot() float64
}

type simpleSpecs struct{ point, tvpl float64 }

func (s simpleSpecs) Point() float64           { return s.point }
func (s simpleSpecs) TickValuePerLot() float64 { return s.tvpl }

type Engine struct {
	mu      sync.RWMutex
	byID    map[string]*Position
	specs   Specs
	enabled bool
}

func NewEngine(sp Specs, enabled bool) *Engine {
	return &Engine{byID: make(map[string]*Position), specs: sp, enabled: enabled}
}

type OpenReq struct {
	Symbol   string    `json:"symbol"`
	Side     Side      `json:"side"`
	Lot      float64   `json:"lot"`
	Price    *float64  `json:"price,omitempty"`
	TPLevels []float64 `json:"tp_levels,omitempty"`
	TPSplits []float64 `json:"tp_splits,omitempty"`
}
type CloseReq struct {
	ID    string   `json:"id"`
	Price *float64 `json:"price,omitempty"`
}

func (e *Engine) OpenNow(mark float64, r OpenReq) (*Position, error) {
	if !e.enabled {
		return nil, errors.New("paper mode disabled")
	}
	if r.Lot <= 0 {
		return nil, errors.New("lot <= 0")
	}
	entry := mark
	if r.Price != nil {
		entry = *r.Price
	}
	p := &Position{
		ID:       uuid.New().String(),
		Symbol:   r.Symbol,
		Side:     r.Side,
		Entry:    entry,
		Lots:     r.Lot,
		Remain:   r.Lot,
		TPLevels: r.TPLevels,
		TPSplits: normSplits(r.TPSplits, len(r.TPLevels)),
		OpenedAt: time.Now().UnixMilli(),
	}
	p.History = append(p.History, Exec{At: p.OpenedAt, Price: entry, LotsDelta: +r.Lot, Note: "open"})
	e.mu.Lock()
	e.byID[p.ID] = p
	e.mu.Unlock()
	return p, nil
}

func (e *Engine) CloseNow(mark float64, r CloseReq) (*Position, error) {
	if !e.enabled {
		return nil, errors.New("paper mode disabled")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.byID[r.ID]
	if !ok {
		return nil, errors.New("not found")
	}
	exit := mark
	if r.Price != nil {
		exit = *r.Price
	}
	e.realize(p, p.Remain, exit, "close")
	ts := time.Now().UnixMilli()
	p.ClosedAt = &ts
	return p, nil
}

func (e *Engine) TickMark(mid float64) {
	if !e.enabled {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range e.byID {
		if p.ClosedAt != nil {
			continue
		}
		p.MarkPrice = mid
		e.maybeRealizeTP(p, mid)
		p.Unrealized = e.mtm(p, mid)
	}
}

func (e *Engine) List() []*Position {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Position, 0, len(e.byID))
	for _, p := range e.byID {
		out = append(out, p)
	}
	return out
}

func (e *Engine) Summary() (realized, unrealized float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, p := range e.byID {
		realized += p.Realized
		if p.ClosedAt == nil {
			unrealized += p.Unrealized
		}
	}
	return
}

func (e *Engine) maybeRealizeTP(p *Position, mid float64) {
	if len(p.TPLevels) == 0 || p.Remain <= 0 {
		return
	}
	for i, tp := range p.TPLevels {
		if p.TPSplits[i] <= 0 {
			continue
		}
		switch p.Side {
		case SideLong:
			if mid >= tp {
				q := p.Lots * p.TPSplits[i]
				e.realize(p, q, tp, "tp")
				p.TPSplits[i] = 0
			}
		case SideShort:
			if mid <= tp {
				q := p.Lots * p.TPSplits[i]
				e.realize(p, q, tp, "tp")
				p.TPSplits[i] = 0
			}
		}
	}
}

func (e *Engine) realize(p *Position, lots float64, price float64, note string) {
	if lots <= 0 {
		return
	}
	if lots > p.Remain {
		lots = p.Remain
	}
	p.Remain -= lots
	pnl := e.pnlLeg(p.Side, p.Entry, price, lots)
	p.Realized += pnl
	p.History = append(p.History, Exec{At: time.Now().UnixMilli(), Price: price, LotsDelta: -lots, Note: note})
}

func (e *Engine) mtm(p *Position, mark float64) float64 {
	return e.pnlLeg(p.Side, p.Entry, mark, p.Remain)
}

func (e *Engine) pnlLeg(side Side, entry, exit, lots float64) float64 {
	diff := exit - entry
	if side == SideShort {
		diff = entry - exit
	}
	ticks := diff / e.specs.Point()
	return ticks * e.specs.TickValuePerLot() * lots
}

func normSplits(splits []float64, n int) []float64 {
	if n <= 0 {
		return nil
	}
	if len(splits) != n {
		x := make([]float64, n)
		for i := range x {
			x[i] = 1.0 / float64(n)
		}
		return x
	}
	sum := 0.0
	for _, v := range splits {
		sum += v
	}
	if math.Abs(sum-1.0) < 1e-9 {
		return splits
	}
	out := make([]float64, n)
	for i, v := range splits {
		out[i] = v / sum
	}
	return out
}

func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// PaperEngine wraps Engine with Redis integration and HTTP handlers
type PaperEngine struct {
	engine  *Engine
	rdb     *redis.Client
	balance float64
	specsFn func(context.Context, string) (float64, float64)
}

// NewPaperEngine creates a new paper trading engine
func NewPaperEngine(rdb *redis.Client, initialBalance float64, specsFn func(context.Context, string) (float64, float64)) *PaperEngine {
	// Use default specs for now
	specs := simpleSpecs{point: 0.1, tvpl: 1.0}
	return &PaperEngine{
		engine:  NewEngine(specs, true),
		rdb:     rdb,
		balance: initialBalance,
		specsFn: specsFn,
	}
}

// RegisterHTTPHandlers registers paper trading HTTP endpoints
func (pe *PaperEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/paper/positions", pe.handlePositions)
	mux.HandleFunc("/paper/summary", pe.handleSummary)
	mux.HandleFunc("/paper/open", pe.handleOpen)
	mux.HandleFunc("/paper/close", pe.handleClose)
}

func (pe *PaperEngine) handlePositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	positions := pe.engine.List()
	JSON(w, http.StatusOK, map[string]any{"positions": positions})
}

func (pe *PaperEngine) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	realized, unrealized := pe.engine.Summary()
	JSON(w, http.StatusOK, map[string]any{
		"balance":    pe.balance,
		"realized":   realized,
		"unrealized": unrealized,
		"total":      pe.balance + realized + unrealized,
	})
}

func (pe *PaperEngine) handleOpen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OpenReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get current market price from Redis (or use provided price)
	mark := 0.0
	if req.Price != nil {
		mark = *req.Price
	} else {
		// Try to get from Redis
		ctx := context.Background()
		if raw, err := pe.rdb.Get(ctx, "last:tick:"+req.Symbol).Bytes(); err == nil {
			var obj struct{ Bid, Ask float64 }
			if json.Unmarshal(raw, &obj) == nil && obj.Bid > 0 && obj.Ask > 0 {
				mark = (obj.Bid + obj.Ask) / 2
			}
		}
	}

	if mark <= 0 {
		http.Error(w, "unable to determine market price", http.StatusBadRequest)
		return
	}

	position, err := pe.engine.OpenNow(mark, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	JSON(w, http.StatusOK, position)
}

func (pe *PaperEngine) handleClose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CloseReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get current market price if not provided
	mark := 0.0
	if req.Price != nil {
		mark = *req.Price
	} else {
		// Use a default or try to fetch from Redis
		mark = 2000.0 // fallback
	}

	position, err := pe.engine.CloseNow(mark, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	JSON(w, http.StatusOK, position)
}
