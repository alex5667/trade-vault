package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Bar struct {
	T int64   `json:"t"`
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
}

type ATRRecord struct {
	ATR    float64 `json:"atr"`
	Period int     `json:"period"`
	Method string  `json:"method"`
	TF     string  `json:"tf"`
	Source string  `json:"source"`
	TS     int64   `json:"ts"`
}

type ATRProvider struct {
	Rdb          *redis.Client
	Symbol       string
	BarsKey      string
	ATRTTL       time.Duration
	Period       int
	StaleSec     int
	PrioritizePy bool
}

func NewATRProvider(rdb *redis.Client, symbol string) *ATRProvider {
	period := intFromEnv("ATR_PERIOD", 14)
	stale := intFromEnv("ATR_STALE_SEC", 90)
	return &ATRProvider{
		Rdb:          rdb,
		Symbol:       symbol,
		BarsKey:      "bars:1m:" + symbol,
		ATRTTL:       2 * time.Minute,
		Period:       period,
		StaleSec:     stale,
		PrioritizePy: true,
	}
}

func (p *ATRProvider) keyATR() string { return "ta:last:atr:" + p.Symbol }

func (p *ATRProvider) GetATR(ctx context.Context) (ATRRecord, error) {
	rec, ok := p.readLastATR(ctx)
	nowMs := time.Now().UnixMilli()
	if ok && (nowMs-rec.TS) <= int64(p.StaleSec*1000) {
		return rec, nil
	}
	if p.PrioritizePy && ok && rec.Source == "py" {
		return rec, nil
	}
	atr, err := p.computeATRFromBars(ctx, p.Period)
	if err != nil || atr <= 0 {
		if ok {
			return rec, nil
		}
		return ATRRecord{}, errors.New("no ATR available")
	}
	out := ATRRecord{ATR: atr, Period: p.Period, Method: "wilder", TF: "M1", Source: "gw", TS: nowMs}
	_ = p.writeATR(ctx, out)
	return out, nil
}

func (p *ATRProvider) readLastATR(ctx context.Context) (ATRRecord, bool) {
	raw, err := p.Rdb.Get(ctx, p.keyATR()).Bytes()
	if err != nil {
		return ATRRecord{}, false
	}
	var rec ATRRecord
	if json.Unmarshal(raw, &rec) != nil {
		return ATRRecord{}, false
	}
	return rec, rec.ATR > 0
}

func (p *ATRProvider) writeATR(ctx context.Context, rec ATRRecord) error {
	raw, _ := json.Marshal(rec)
	return p.Rdb.Set(ctx, p.keyATR(), raw, p.ATRTTL).Err()
}

func (p *ATRProvider) computeATRFromBars(ctx context.Context, n int) (float64, error) {
	raw, err := p.Rdb.LRange(ctx, p.BarsKey, -300, -1).Result()
	if err != nil || len(raw) < n+1 {
		return 0, errors.New("not enough bars")
	}
	bars := make([]Bar, 0, len(raw))
	for _, s := range raw {
		var b Bar
		if json.Unmarshal([]byte(s), &b) == nil && b.H > 0 && b.L > 0 && b.C > 0 {
			bars = append(bars, b)
		}
	}
	if len(bars) < n+1 {
		return 0, errors.New("not enough clean bars")
	}
	return wilderATR(bars, n), nil
}

func wilderATR(bars []Bar, n int) float64 {
	var trSum float64
	for i := 1; i <= n; i++ {
		prev := bars[i-1].C
		hi, lo := bars[i].H, bars[i].L
		tr := math.Max(hi-lo, math.Max(math.Abs(hi-prev), math.Abs(lo-prev)))
		trSum += tr
	}
	atr := trSum / float64(n)
	for i := n + 1; i < len(bars); i++ {
		prev := bars[i-1].C
		hi, lo := bars[i].H, bars[i].L
		tr := math.Max(hi-lo, math.Max(math.Abs(hi-prev), math.Abs(lo-prev)))
		atr = (atr*float64(n-1) + tr) / float64(n)
	}
	return atr
}

func intFromEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// RedisListCandleRepo - placeholder for ATR service compatibility
type RedisListCandleRepo struct {
	RDB *redis.Client
}

// ATRService wraps ATRProvider with HTTP handlers
type ATRService struct {
	rdb      *redis.Client
	repo     *RedisListCandleRepo
	provider *ATRProvider
	period   int
	cacheTTL time.Duration
}

// NewATRService creates a new ATR service
func NewATRService(rdb *redis.Client, repo *RedisListCandleRepo, period int, cacheTTL time.Duration) *ATRService {
	symbol := os.Getenv("SYMBOL")
	if symbol == "" {
		symbol = "XAUUSD"
	}
	return &ATRService{
		rdb:      rdb,
		repo:     repo,
		provider: NewATRProvider(rdb, symbol),
		period:   period,
		cacheTTL: cacheTTL,
	}
}

// RegisterHTTPHandlers registers ATR endpoints
func (s *ATRService) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/runtime/atr", s.handleATR)
}

func (s *ATRService) handleATR(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	atrRec, err := s.provider.GetATR(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(atrRec)
}
