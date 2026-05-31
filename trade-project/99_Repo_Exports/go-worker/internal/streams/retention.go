package streams

import (
	"os"
	"strconv"
	"strings"
)

// Unified Retention Policy for Redis Streams (MaxLen)
// See issue: Inconsistent MaxLen across streams
const (
	// MaxLenGlobal is the default max length for global aggregate streams
	// containing data from all symbols (e.g. liq_evt, ticker-24h).
	// ~4h at 15 signals/sec at ~1KB/msg = 60MB
	MaxLenGlobal int64 = 200000

	// maxLenCandlesDefault is the compile-time default for MaxLenCandles.
	// Override at runtime via ENV: CANDLE_STREAM_MAXLEN=<positive int64>.
	maxLenCandlesDefault int64 = 50000

	// MaxLenDLQ is the default max length for Dead Letter Queue streams.
	MaxLenDLQ int64 = 50000

	// MaxLenPerSymbol is the default max length for per-symbol streams (e.g. tick_<symbol>).
	MaxLenPerSymbol int64 = 10000
)

// MaxLenCandles returns the effective MAXLEN for candle data streams.
//
// Priority:
//  1. ENV CANDLE_STREAM_MAXLEN (positive int64) — allows runtime override without recompile.
//  2. maxLenCandlesDefault (50000) — compile-time safe default.
//
// ENV format: CANDLE_STREAM_MAXLEN=100000
// Invalid or non-positive values fall back to the default silently.
func MaxLenCandles() int64 {
	v := strings.TrimSpace(os.Getenv("CANDLE_STREAM_MAXLEN"))
	if v == "" {
		return maxLenCandlesDefault
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return maxLenCandlesDefault
	}
	return n
}
