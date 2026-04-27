package runtime

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type DOMLevel struct {
	Price float64 `json:"price"`
	Bid   float64 `json:"bid"`
	Ask   float64 `json:"ask"`
}

type Snapshot struct {
	TS      int64                  `json:"ts"`
	Symbol  string                 `json:"symbol"`
	Mid     float64                `json:"mid"`
	BestBid float64                `json:"best_bid"`
	BestAsk float64                `json:"best_ask"`
	ATR     float64                `json:"atr"`
	ATRMeta ATRRecord              `json:"atr_meta"`
	Pivots  map[string]float64     `json:"pivots,omitempty"`
	Balance *float64               `json:"balance,omitempty"`
	DOM     []DOMLevel             `json:"dom,omitempty"`
	Extra   map[string]interface{} `json:"extra,omitempty"`
}

type SnapshotBuilder struct {
	Rdb    *redis.Client
	ATR    *ATRProvider
	Symbol string
	DOMKey string
}

func NewSnapshotBuilder(rdb *redis.Client, symbol string) *SnapshotBuilder {
	return &SnapshotBuilder{
		Rdb:    rdb,
		ATR:    NewATRProvider(rdb, symbol),
		Symbol: symbol,
		DOMKey: "book:levels:" + symbol,
	}
}

func (b *SnapshotBuilder) Build(ctx context.Context, limitLevels int) (Snapshot, error) {
	now := time.Now().UnixMilli()
	atrRec, _ := b.ATR.GetATR(ctx)
	dom := b.loadDOM(ctx, limitLevels)
	bestBid, bestAsk := bestFromDOM(dom)
	mid := b.loadMid(ctx, bestBid, bestAsk)

	s := Snapshot{
		TS:      now,
		Symbol:  b.Symbol,
		Mid:     mid,
		BestBid: bestBid,
		BestAsk: bestAsk,
		ATR:     atrRec.ATR,
		ATRMeta: atrRec,
		Pivots:  b.loadPivots(ctx),
		Balance: b.loadBalance(ctx),
		DOM:     dom,
		Extra:   map[string]interface{}{"source": "gateway"},
	}
	return s, nil
}

func (b *SnapshotBuilder) loadDOM(ctx context.Context, n int) []DOMLevel {
	raw, err := b.Rdb.Get(ctx, b.DOMKey).Bytes()
	if err != nil || len(raw) == 0 {
		return nil
	}
	var arr []DOMLevel
	_ = json.Unmarshal(raw, &arr)
	if n > 0 && len(arr) > n {
		return arr[:n]
	}
	return arr
}

func bestFromDOM(dom []DOMLevel) (bid, ask float64) {
	if len(dom) == 0 {
		return 0, 0
	}
	bid, ask = 0, math.MaxFloat64
	for _, lv := range dom {
		if lv.Bid > 0 && lv.Bid > bid {
			bid = lv.Bid
		}
		if lv.Ask > 0 && lv.Ask < ask {
			ask = lv.Ask
		}
	}
	if ask == math.MaxFloat64 {
		ask = 0
	}
	return
}

func (b *SnapshotBuilder) loadMid(ctx context.Context, bestBid, bestAsk float64) float64 {
	if raw, err := b.Rdb.Get(ctx, "last:tick:"+b.Symbol).Bytes(); err == nil {
		var obj struct{ Bid, Ask float64 }
		if json.Unmarshal(raw, &obj) == nil && obj.Bid > 0 && obj.Ask > 0 {
			return (obj.Bid + obj.Ask) / 2
		}
	}
	if bestBid > 0 && bestAsk > 0 {
		return (bestBid + bestAsk) / 2
	}
	return 0
}

func (b *SnapshotBuilder) loadPivots(ctx context.Context) map[string]float64 {
	raw, err := b.Rdb.Get(ctx, "pivots:latest").Bytes()
	if err != nil || len(raw) == 0 {
		return nil
	}
	var obj map[string]float64
	if json.Unmarshal(raw, &obj) != nil {
		return nil
	}
	return obj
}

func (b *SnapshotBuilder) loadBalance(ctx context.Context) *float64 {
	key := os.Getenv("BALANCE_KEY")
	if key == "" {
		key = "account:last:balance"
	}
	raw, err := b.Rdb.Get(ctx, key).Float64()
	if err != nil {
		return nil
	}
	return &raw
}
