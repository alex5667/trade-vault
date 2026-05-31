package models

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"go-worker/pkg/timeutil"
)

// NormalizedTick represents a unified trade tick for any exchange.
type NormalizedTick struct {
	Symbol     string   `json:"symbol"`
	Ts         int64    `json:"ts"`
	Price      string   `json:"price"`
	Qty        string   `json:"qty"`
	Quantity   string   `json:"quantity"`
	Side       string   `json:"side"` // BUY/SELL
	Source     string   `json:"source"`
	Market     string   `json:"market"`
	TradeID    int64    `json:"trade_id"`
	TradeIDRaw string   `json:"trade_id_raw,omitempty"` // For Bybit UUIDs
	Seq        int64    `json:"seq,omitempty"`          // For Bybit cross sequence
	Coin       string   `json:"coin,omitempty"`         // For Hyperliquid raw coin
	TxHash     string   `json:"tx_hash,omitempty"`      // For Hyperliquid L1 tx hash
	TradeUID   string   `json:"trade_uid,omitempty"`    // For Hyperliquid uid
	CVDUSD     *float64 `json:"cvd_usd,omitempty"`      // For Binance

	// QualityFlags is a comma-separated list of data-quality markers for this event.
	// Well-known values: "ok", "ts_fallback" (exchange ts was 0; local time substituted).
	// Empty string is treated as "ok" by PopulateRedisValues.
	QualityFlags string `json:"-"` // in-process only; serialised explicitly via PopulateRedisValues
}

// PopulateRedisValues prepares tick to be published to a Redis Stream format mapping by filling a provided map.
func (t NormalizedTick) PopulateRedisValues(vals map[string]any) {
	sym := strings.ToUpper(t.Symbol)
	ingestMs := timeutil.GetCurrentTimestampMs()

	vals["symbol"] = sym
	vals["ts"] = t.Ts
	vals["price"] = t.Price
	vals["qty"] = t.Qty
	vals["quantity"] = t.Qty // Standardized parity
	vals["v"] = 1            // Contract version
	vals["side"] = t.Side
	vals["source"] = t.Source
	vals["market"] = t.Market
	vals["trade_id"] = t.TradeID
	vals["written_at"] = ingestMs
	vals["schema_version"] = "1"

	// ── CLAUDE.md contract fields ─────────────────────────────────────────
	// event_time_ms / ingest_time_ms: canonical aliases for ts / written_at.
	// Old fields kept for backward compat with existing Python consumers.
	vals["event_time_ms"] = t.Ts
	vals["ingest_time_ms"] = ingestMs

	// event_id: deterministic idempotency key — "<source>:<SYMBOL>:<ts_ms>"
	vals["event_id"] = t.Source + ":" + sym + ":" + strconv.FormatInt(t.Ts, 10)

	// trace_id: generate a random correlation ID for downstream tracing (Go↔Python).
	traceBytes := make([]byte, 16)
	_, _ = rand.Read(traceBytes)
	vals["trace_id"] = hex.EncodeToString(traceBytes)

	// quality_flags: comma-separated DQ markers; "ok" when clean.
	qf := t.QualityFlags
	if qf == "" {
		qf = "ok"
	}
	vals["quality_flags"] = qf

	if t.TradeIDRaw != "" {
		vals["trade_id_raw"] = t.TradeIDRaw
	}
	if t.Seq != 0 {
		vals["seq"] = t.Seq
	}
	if t.Coin != "" {
		vals["coin"] = strings.ToUpper(t.Coin)
	}
	if t.TxHash != "" {
		vals["tx_hash"] = t.TxHash
	}
	if t.TradeUID != "" {
		vals["trade_uid"] = t.TradeUID
	}
	if t.CVDUSD != nil {
		vals["cvd_usd"] = *t.CVDUSD
	}
}

// NormalizedDepth represents a unified top-N orderbook snapshot.
type NormalizedDepth struct {
	Symbol    string     `json:"symbol"`
	Ts        int64      `json:"ts"`
	FirstID   int64      `json:"first_id"`
	FinalID   int64      `json:"final_id"`
	PrevFinal int64      `json:"prev_final"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
	Source    string     `json:"source"`
	Market    string     `json:"market"`
	Seq       int64      `json:"seq,omitempty"`  // Bybit
	Coin      string     `json:"coin,omitempty"` // Hyperliquid

	// GapDetected is set by the exchange normaliser when a sequence gap is found
	// (delta.FirstID != prevFinalID+1).  The controller must NOT publish this
	// book to the main stream; it should write a DLQ event and request a
	// re-snapshot instead.  This field is intentionally excluded from Redis /
	// JSON serialisation — it is an in-process signal only.
	GapDetected bool  `json:"-"`
	GapExpected int64 `json:"-"` // what FirstID we expected
	GapActual   int64 `json:"-"` // what FirstID we received

	// QualityFlags: same semantics as NormalizedTick.QualityFlags.
	QualityFlags string `json:"-"`
}

// PopulateRedisValues prepares the book to be published to a Redis Stream by filling a provided map.
func (d NormalizedDepth) PopulateRedisValues(vals map[string]any) {
	bidsJSON, err := json.Marshal(d.Bids)
	if err != nil {
		bidsJSON = []byte("[]")
	}
	asksJSON, err := json.Marshal(d.Asks)
	if err != nil {
		asksJSON = []byte("[]")
	}

	sym := strings.ToUpper(d.Symbol)
	ingestMs := timeutil.GetCurrentTimestampMs()

	vals["symbol"] = sym
	vals["ts"] = d.Ts
	vals["first_id"] = d.FirstID
	vals["final_id"] = d.FinalID
	vals["prev_final"] = d.PrevFinal
	vals["bids"] = string(bidsJSON)
	vals["asks"] = string(asksJSON)
	vals["source"] = d.Source
	vals["market"] = d.Market
	vals["v"] = 1 // Contract version
	vals["written_at"] = ingestMs
	vals["schema_version"] = "1"

	// ── CLAUDE.md contract fields ─────────────────────────────────────────
	vals["event_time_ms"] = d.Ts
	vals["ingest_time_ms"] = ingestMs
	vals["event_id"] = d.Source + ":" + sym + ":" + strconv.FormatInt(d.Ts, 10)
	
	traceBytes := make([]byte, 16)
	_, _ = rand.Read(traceBytes)
	vals["trace_id"] = hex.EncodeToString(traceBytes)

	qf := d.QualityFlags
	if qf == "" {
		qf = "ok"
	}
	vals["quality_flags"] = qf

	if d.Seq != 0 {
		vals["seq"] = d.Seq
	}
	if d.Coin != "" {
		vals["coin"] = strings.ToUpper(d.Coin)
	}
}
