package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

type SnapshotProvider struct {
	rdb     *redis.Client
	ctx     context.Context
	Symbol  string
	BookKey string
	PivKey  string
	AtrKey  string
	// balance берём снаружи (из твоего состояния/EA), но оставим хук
	BalanceFn func() float64
}

func NewSnapshotProvider() *SnapshotProvider {
	rc := os.Getenv("REDIS_URL")
	if rc == "" {
		rc = "redis://localhost:6379/0"
	}
	opt, _ := parseRedisURL(rc)
	rdb := redis.NewClient(opt)
	symbol := getenv("SYMBOL", "XAUUSD")
	return &SnapshotProvider{
		rdb:       rdb,
		ctx:       context.Background(),
		Symbol:    symbol,
		BookKey:   getenv("BOOK_LAST_KEY", fmt.Sprintf("book:levels:%s", symbol)),
		PivKey:    getenv("PIVOTS_LAST_KEY", "pivots:latest"),
		AtrKey:    getenv("ATR_LAST_KEY", fmt.Sprintf("ta:last:atr:%s", symbol)),
		BalanceFn: func() float64 { return 10000.0 }, // заменится реальной функцией в main
	}
}

func (sp *SnapshotProvider) readJSON(key string, out any) error {
	s, err := sp.rdb.Get(sp.ctx, key).Result()
	if err != nil {
		return err
	}
	if s == "" {
		return fmt.Errorf("empty redis key %s", key)
	}
	return json.Unmarshal([]byte(s), out)
}

type DOMSnapshot struct {
	TS       int64        `json:"ts"`
	Symbol   string       `json:"symbol"`
	Provider string       `json:"provider"`
	Mid      float64      `json:"mid"`
	Bids     [][2]float64 `json:"bids"`
	Asks     [][2]float64 `json:"asks"`
}

type Pivots struct {
	H   float64 `json:"H"`
	L   float64 `json:"L"`
	C   float64 `json:"C"`
	Day string  `json:"day,omitempty"`
}

type RuntimeSnapshot struct {
	Symbol  string       `json:"symbol"`
	Balance float64      `json:"balance"`
	ATR     float64      `json:"atr"`
	Pivots  *Pivots      `json:"pivots,omitempty"`
	DOM     *DOMSnapshot `json:"dom,omitempty"`
}

func (sp *SnapshotProvider) BuildSnapshot() (*RuntimeSnapshot, error) {
	resp := &RuntimeSnapshot{Symbol: sp.Symbol, Balance: sp.BalanceFn()}
	// ATR
	if s, err := sp.rdb.Get(sp.ctx, sp.AtrKey).Result(); err == nil && s != "" {
		// допускаем как plain "3.20", так и {"atr":3.2}
		var holder any
		if json.Unmarshal([]byte(s), &holder) == nil {
			switch v := holder.(type) {
			case map[string]any:
				if a, ok := v["atr"]; ok {
					switch vv := a.(type) {
					case float64:
						resp.ATR = vv
					case json.Number:
						if f, err := vv.Float64(); err == nil {
							resp.ATR = f
						}
					}
				}
			case float64:
				resp.ATR = v
			}
		}
	}
	// pivots
	var piv Pivots
	if err := sp.readJSON(sp.PivKey, &piv); err == nil {
		resp.Pivots = &piv
	}
	// DOM
	var dom DOMSnapshot
	if err := sp.readJSON(sp.BookKey, &dom); err == nil {
		resp.DOM = &dom
	}
	return resp, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Small helper to ensure REDIS_URL with database works for go-redis
func parseRedisURL(raw string) (*redis.Options, error) {
	_, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	return redis.ParseURL(strings.TrimSpace(raw))
}
