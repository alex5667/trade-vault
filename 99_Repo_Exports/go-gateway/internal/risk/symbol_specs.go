package risk

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type SymbolSpecs struct {
	Symbol          string    `json:"symbol"`
	Point           float64   `json:"point"`
	TickValuePerLot float64   `json:"tick_value_per_lot"`
	ContractSize    float64   `json:"contract_size"`
	LotStep         float64   `json:"lot_step"`
	MinLot          float64   `json:"min_lot"`
	MaxLot          float64   `json:"max_lot"`
	ATRPeriod       int       `json:"atr_period"`
	ATRSLMult       float64   `json:"atr_sl_mult"`
	ATRTPMults      []float64 `json:"atr_tp_mults"`
}

func LoadSymbolSpecs(ctx context.Context, rdb *redis.Client, symbol string) SymbolSpecs {
	key := "symbol_specs:" + symbol
	raw, err := rdb.Get(ctx, key).Bytes()
	if err == nil {
		var sp SymbolSpecs
		if json.Unmarshal(raw, &sp) == nil && sp.Point > 0 && sp.TickValuePerLot > 0 {
			return sp
		}
	}
	sp := SymbolSpecs{
		Point:           floatFromEnv("SPEC_POINT", 0.1),
		TickValuePerLot: floatFromEnv("SPEC_TICK_VALUE_PER_LOT", 1.0),
		ContractSize:    floatFromEnv("SPEC_CONTRACT_SIZE", 0),
	}
	_ = rdb.Set(ctx, key, mustJSON(sp), 24*time.Hour).Err()
	return sp
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func floatFromEnv(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// SymbolSpecsLoader provides cached loading of symbol specifications
type SymbolSpecsLoader struct {
	rdb      *redis.Client
	cacheTTL time.Duration
	defaults SymbolSpecs
}

// NewSymbolSpecsLoader creates a new symbol specs loader
func NewSymbolSpecsLoader(rdb *redis.Client, cacheTTL time.Duration, defaults SymbolSpecs) *SymbolSpecsLoader {
	return &SymbolSpecsLoader{
		rdb:      rdb,
		cacheTTL: cacheTTL,
		defaults: defaults,
	}
}

// Get retrieves symbol specs for a given symbol
func (l *SymbolSpecsLoader) Get(ctx context.Context, symbol string) (SymbolSpecs, error) {
	key := "symbol_specs:" + symbol
	raw, err := l.rdb.Get(ctx, key).Bytes()
	if err == nil {
		var sp SymbolSpecs
		if json.Unmarshal(raw, &sp) == nil && sp.Point > 0 && sp.TickValuePerLot > 0 {
			return sp, nil
		}
	}

	// Return defaults if not found
	result := l.defaults
	result.Symbol = symbol
	return result, nil
}
