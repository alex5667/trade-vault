package hyperliquid

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go-worker/internal/models"
)

// --- Hyperliquid WS message formats (official docs) ---
// See: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
//
// Key specificity vs Binance/Bybit:
// - trades feed sends WsTrade[] with fields: coin, side, px, sz, hash, time, tid
// - l2Book feed is SNAPSHOT feed (levels full aggregated), pushed each block (not diff)
// - keepalive requires client to send {"method":"ping"} if no messages in 60s

type hyperliquidEnvelope struct {
	Channel    string          `json:"channel"`
	Data       json.RawMessage `json:"data"`
	IsSnapshot *bool           `json:"isSnapshot,omitempty"`
}

// WsTrade — формат из docs.
type WsTrade struct {
	Coin  string   `json:"coin"`
	Side  string   `json:"side"`
	Px    string   `json:"px"`
	Sz    string   `json:"sz"`
	Hash  string   `json:"hash"`
	Time  int64    `json:"time"`
	TID   int64    `json:"tid"`
	Users []string `json:"users"`
}

type WsLevel struct {
	Px string `json:"px"`
	Sz string `json:"sz"`
	N  int    `json:"n"`
}

// WsBook — snapshot feed.
type WsBook struct {
	Coin   string      `json:"coin"`
	Levels [][]WsLevel `json:"levels"` // [bids, asks]
	Time   int64       `json:"time"`
}

// models.NormalizedTick — совместимый с Python/Redis tick-схемой.
// Ключевые поля соответствуют internal/binance.models.NormalizedTick.
// Доп.поля добавлены без нарушения обратной совместимости (Python может игнорировать).

// models.NormalizedDepth — совместимый с Python/Redis book-схемой.
// Для Hyperliquid: это snapshot aggregated L2 book (levels full state).

var errEmptyPayload = errors.New("empty hyperliquid payload")

// normalizeEpochMs tries to detect seconds vs millis.
// Hyperliquid docs use "time: number" and examples in ecosystem are ms.
// To be robust, if value looks like seconds, convert to ms.
func normalizeEpochMs(v int64) int64 {
	// 1e12 ms is ~2001-09-09. Anything below that is suspicious for modern data.
	if v > 0 && v < 1_000_000_000_000 {
		return v * 1000
	}
	return v
}

func normalizeSide(raw string) string {
	s := strings.ToUpper(strings.TrimSpace(raw))
	// Common HL encodings observed in SDKs / examples: "B" (buy) and "A" (sell).
	switch s {
	case "B", "BUY":
		return "BUY"
	case "A", "SELL":
		return "SELL"
	default:
		// keep as-is but never empty
		if s == "" {
			return "UNKNOWN"
		}
		return s
	}
}

// normalizeSymbol maps HL coin -> internal symbol.
//
// Policy:
// - If explicit map has coin, use it.
// - Else append suffix (default "USDT") to match existing Binance-style symbol keys.
// - Can be disabled by setting suffix="" (keep coin as symbol).
func normalizeSymbol(coin string, symbolMap map[string]string, suffix string) string {
	c := strings.ToUpper(strings.TrimSpace(coin))
	if c == "" {
		return ""
	}
	if v, ok := symbolMap[c]; ok {
		return strings.ToUpper(v)
	}
	if suffix == "" {
		return c
	}
	return c + strings.ToUpper(suffix)
}

// NormalizeFuturesMessage converts Hyperliquid WS message into normalized ticks/books.
// It is designed to be drop-in compatible with internal/binance.NormalizeFuturesMessage
// consumer loop (publish tick -> stream:tick_SYMBOL, publish book -> stream:book_SYMBOL).
func NormalizeFuturesMessage(raw []byte, symbolMap map[string]string, suffix string, maxBookLevels int) (ticks []models.NormalizedTick, books []models.NormalizedDepth, err error) {
	if len(raw) == 0 {
		return nil, nil, errEmptyPayload
	}

	// Hyperliquid sends either {channel,data} or other objects (e.g. pong).
	// Use Decoder.UseNumber? Here fields are mostly strings and int64.
	var env hyperliquidEnvelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return nil, nil, fmt.Errorf("decode envelope: %w", err)
	}

	switch env.Channel {
	case "trades":
		var wsTrades []WsTrade
		if err := json.Unmarshal(env.Data, &wsTrades); err != nil {
			return nil, nil, fmt.Errorf("decode trades: %w", err)
		}
		for _, tr := range wsTrades {
			sym := normalizeSymbol(tr.Coin, symbolMap, suffix)
			if sym == "" {
				continue
			}
			ts := normalizeEpochMs(tr.Time)
			tid := tr.TID
			// Per docs: for globally unique id use (block_time, coin, tid)
			tradeUID := fmt.Sprintf("%d:%s:%d", ts, strings.ToUpper(strings.TrimSpace(tr.Coin)), tid)

			ticks = append(ticks, models.NormalizedTick{
				Symbol:   sym,
				Ts:       ts,
				Price:    tr.Px,
				Qty:      tr.Sz,
				Side:     normalizeSide(tr.Side),
				Source:   "hyperliquid",
				Market:   "USDC-PERP",
				TradeID:  tid,
				Coin:     tr.Coin,
				TxHash:   tr.Hash,
				TradeUID: tradeUID,
			})
		}
		return ticks, nil, nil

	case "l2Book":
		var book WsBook
		if err := json.Unmarshal(env.Data, &book); err != nil {
			return nil, nil, fmt.Errorf("decode l2Book: %w", err)
		}
		sym := normalizeSymbol(book.Coin, symbolMap, suffix)
		if sym == "" {
			return nil, nil, nil
		}

		ts := normalizeEpochMs(book.Time)
		bids, asks := levelsToString2(book.Levels, maxBookLevels)

		// Hyperliquid book is snapshot per block.
		// We don't have update IDs like Binance. Keep 0 for ids for compatibility.
		books = append(books, models.NormalizedDepth{
			Symbol:    sym,
			Ts:        ts,
			FirstID:   0,
			FinalID:   0,
			PrevFinal: 0,
			Bids:      bids,
			Asks:      asks,
			Source:    "hyperliquid",
			Market:    "USDC-PERP",
			Coin:      book.Coin,
		})
		return nil, books, nil

	case "pong", "subscriptionResponse":
		// keepalive and ack — no market data.
		return nil, nil, nil

	default:
		// Ignore other channels (notification, user feeds, errors) in this module.
		return nil, nil, nil
	}
}

func levelsToString2(levels [][]WsLevel, maxN int) (bids [][]string, asks [][]string) {
	if len(levels) >= 1 {
		bids = make([][]string, 0, len(levels[0]))
		for i, lv := range levels[0] {
			if maxN > 0 && i >= maxN {
				break
			}
			bids = append(bids, []string{lv.Px, lv.Sz})
		}
	} else {
		bids = [][]string{}
	}

	if len(levels) >= 2 {
		asks = make([][]string, 0, len(levels[1]))
		for i, lv := range levels[1] {
			if maxN > 0 && i >= maxN {
				break
			}
			asks = append(asks, []string{lv.Px, lv.Sz})
		}
	} else {
		asks = [][]string{}
	}
	return bids, asks
}
