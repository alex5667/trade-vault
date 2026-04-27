package liquidation

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go-worker/internal/streams"

	"go.uber.org/zap"
)

// Config — конфигурация ingestion сервиса ликвидаций.
//
// ENV ключи:
//
//	LIQ_WS_ENABLED=true|false
//	LIQ_SYMBOLS=BTCUSDT,ETHUSDT
//
//	LIQ_BINANCE_ENABLED=true|false
//	LIQ_BINANCE_WS_URL=wss://fstream.binance.com/ws/!forceOrder@arr
//
//	LIQ_BYBIT_ENABLED=true|false
//	LIQ_BYBIT_WS_URL=wss://stream.bybit.com/v5/public/linear
//	LIQ_BYBIT_PING_MS=20000    — application-level op:ping к Bybit V5 WS (default: 20000 ms)
//	LIQ_BINANCE_PING_MS=20000  — клиентский ping к Binance WS (default: 20000 ms)
//
//	LIQ_STREAM=stream:liq_evt
//	LIQ_QUARANTINE_STREAM=stream:liq_evt_quarantine
//
//	LIQ_BATCH_SIZE=200
//	LIQ_FLUSH_MS=20
//	LIQ_STREAM_MAXLEN=200000
//
//	LIQ_MAX_EVENT_AGE_MS=10000
//	LIQ_MAX_FUTURE_SKEW_MS=2000
//	LIQ_MAX_OOO_MS=2000
//	LIQ_ENABLE_QUARANTINE=true|false
type Config struct {
	Enabled bool

	Symbols []string

	BinanceEnabled bool
	BinanceWSURL   string

	BybitEnabled bool
	BybitWSURL   string

	Stream           string
	QuarantineStream string

	BatchSize     int
	FlushInterval time.Duration
	StreamMaxLen  int64

	DQ *DQPolicy

	BybitPingPeriod   time.Duration
	BinancePingPeriod time.Duration // LIQ_BINANCE_PING_MS; default 20s
}

var validSymbolRegex = regexp.MustCompile("^[A-Z0-9_\\-]+$")

func parseBoolEnv(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "0", "false", "off", "no":
		return false
	case "1", "true", "on", "yes":
		return true
	default:
		return def
	}
}

func parseIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func parseInt64Env(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func parseDurationMsEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func parseSymbolsEnv(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	var invalidSymbols []string
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s != "" {
			if validSymbolRegex.MatchString(s) {
				out = append(out, s)
			} else {
				invalidSymbols = append(invalidSymbols, s)
			}
		}
	}

	if len(invalidSymbols) > 0 {
		zap.S().Fatalf("❌ Обнаружены невалидные символы в %s: %v. Разрешены только [A-Z0-9_\\-].", key, invalidSymbols)
	}

	if len(out) == 0 {
		return fallback
	}
	return out
}

// LoadConfigFromEnv выставляет безопасные дефолты (SAFE режим).
func LoadConfigFromEnv(fallbackSymbols []string) Config {
	symbols := parseSymbolsEnv("LIQ_SYMBOLS", fallbackSymbols)

	cfg := Config{
		Enabled:          parseBoolEnv("LIQ_WS_ENABLED", false),
		Symbols:          symbols,
		BinanceEnabled:   parseBoolEnv("LIQ_BINANCE_ENABLED", true),
		BinanceWSURL:     strings.TrimSpace(os.Getenv("LIQ_BINANCE_WS_URL")),
		BybitEnabled:     parseBoolEnv("LIQ_BYBIT_ENABLED", false),
		BybitWSURL:       strings.TrimSpace(os.Getenv("LIQ_BYBIT_WS_URL")),
		Stream:           strings.TrimSpace(os.Getenv("LIQ_STREAM")),
		QuarantineStream: strings.TrimSpace(os.Getenv("LIQ_QUARANTINE_STREAM")),
		BatchSize:        parseIntEnv("LIQ_BATCH_SIZE", 200),
		FlushInterval:    parseDurationMsEnv("LIQ_FLUSH_MS", 20*time.Millisecond),
		StreamMaxLen:     parseInt64Env("LIQ_STREAM_MAXLEN", streams.MaxLenGlobal),
		BybitPingPeriod:   parseDurationMsEnv("LIQ_BYBIT_PING_MS", 20*time.Second),
		BinancePingPeriod: parseDurationMsEnv("LIQ_BINANCE_PING_MS", 20*time.Second),
	}

	if cfg.BinanceWSURL == "" {
		cfg.BinanceWSURL = "wss://fstream.binance.com/ws/!forceOrder@arr"
	}
	if cfg.BybitWSURL == "" {
		cfg.BybitWSURL = "wss://stream.bybit.com/v5/public/linear"
	}
	if cfg.Stream == "" {
		cfg.Stream = streams.LiqEvt
	}
	if cfg.QuarantineStream == "" {
		cfg.QuarantineStream = streams.LiqEvtQuarantine
	}

	dq := DefaultDQPolicy(symbols)
	dq.MaxEventAge = parseDurationMsEnv("LIQ_MAX_EVENT_AGE_MS", 10*time.Second)
	dq.MaxFutureSkew = parseDurationMsEnv("LIQ_MAX_FUTURE_SKEW_MS", 2*time.Second)
	dq.MaxOutOfOrder = parseDurationMsEnv("LIQ_MAX_OOO_MS", 2*time.Second)
	dq.EnableQuarantine = parseBoolEnv("LIQ_ENABLE_QUARANTINE", true)

	// A4: optional dedup (noise reduction)
	dq.DedupEnabled = parseBoolEnv("LIQ_DEDUP_ENABLED", true)
	dq.DedupTTL = parseDurationMsEnv("LIQ_DEDUP_TTL_MS", 60*time.Second)
	dq.DedupMaxKeys = parseIntEnv("LIQ_DEDUP_MAX_KEYS", 20_000)
	if dq.DedupEnabled {
		dq.dedup = newDedupCache(dq.DedupTTL, dq.DedupMaxKeys)
	} else {
		dq.dedup = nil
	}

	cfg.DQ = &dq

	return cfg
}
