package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go-worker/internal/monitoring"
	"go-worker/internal/streams"

	"go.uber.org/zap"
)

// Bybit V5 Market Data REST.
// Документация:
//   - Get Tickers: /v5/market/tickers (category=linear|inverse|spot|option)
//   - Get Funding Rate History: /v5/market/funding/history (category=linear|inverse, symbol required)
//
// ВАЖНО:
// - В отличие от Binance, funding history на Bybit требует symbol → поэтому
//   мы запрашиваем funding rate только по списку BYBIT_FUNDING_SYMBOLS.
// - Для «все тикеры за 24ч» используем /v5/market/tickers?category=linear.

const (
	DefaultBaseURL = "https://api.bybit.com"
)

// APIClient — клиент Bybit REST API для публичных market endpoints.
// Не использует ключи, только публичные вызовы.
type APIClient struct {
	baseURL    string
	httpClient *http.Client
	publisher  *StreamPublisher
}

// NewAPIClient создаёт Bybit API client.
// ENV:
//
//	BYBIT_BASE_URL           (default https://api.bybit.com)
//	BYBIT_HTTP_TIMEOUT       (default 10s)
//	BYBIT_HTTP_KEEPALIVE     (default 30s)
//	BYBIT_HTTP_IDLE_CONN     (default 90s)
//	BYBIT_HTTP_MAX_IDLE_CONS (default 50)
func NewAPIClient() *APIClient {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("BYBIT_BASE_URL")), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	timeout := getEnvDuration("BYBIT_HTTP_TIMEOUT", 10*time.Second)
	keepAlive := getEnvDuration("BYBIT_HTTP_KEEPALIVE", 30*time.Second)
	idleConnTimeout := getEnvDuration("BYBIT_HTTP_IDLE_CONN", 90*time.Second)
	maxIdleConns := getEnvInt("BYBIT_HTTP_MAX_IDLE_CONS", 50)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: keepAlive,
		}).DialContext,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConns,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	return &APIClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		publisher: NewStreamPublisher(),
	}
}

// Ticker24h — часть payload'а /v5/market/tickers (Linear/Inverse).
// Храним строки как есть (без float), чтобы не терять точность и не ломать контракт.
type Ticker24h struct {
	Symbol            string `json:"symbol"`
	LastPrice         string `json:"lastPrice"`
	PrevPrice24h      string `json:"prevPrice24h"`
	Price24hPcnt      string `json:"price24hPcnt"`
	HighPrice24h      string `json:"highPrice24h"`
	LowPrice24h       string `json:"lowPrice24h"`
	Volume24h         string `json:"volume24h"`
	Turnover24h       string `json:"turnover24h"`
	OpenInterest      string `json:"openInterest"`
	OpenInterestValue string `json:"openInterestValue"`
	FundingRate       string `json:"fundingRate"`
	NextFundingTime   string `json:"nextFundingTime"`
	Bid1Price         string `json:"bid1Price"`
	Bid1Size          string `json:"bid1Size"`
	Ask1Price         string `json:"ask1Price"`
	Ask1Size          string `json:"ask1Size"`
}

type tickersResp struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string      `json:"category"`
		List     []Ticker24h `json:"list"`
	} `json:"result"`
	Time int64 `json:"time"` // server time in ms
}

// FundingRatePoint — элемент ответа /v5/market/funding/history.
type FundingRatePoint struct {
	Symbol               string `json:"symbol"`
	FundingRate          string `json:"fundingRate"`
	FundingRateTimestamp string `json:"fundingRateTimestamp"` // ms as string per docs
}

type fundingHistoryResp struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string             `json:"category"`
		List     []FundingRatePoint `json:"list"`
	} `json:"result"`
	Time int64 `json:"time"`
}

// Fetch24hTickers возвращает 24h тикеры (snapshot) для linear.
// Возвращаем также tsMs (server time), чтобы downstream мог иметь детерминизм.
func (c *APIClient) Fetch24hTickers(ctx context.Context) ([]Ticker24h, int64, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("nil bybit client")
	}

	u, err := url.Parse(c.baseURL + "/v5/market/tickers")
	if err != nil {
		return nil, 0, fmt.Errorf("parse base url: %w", err)
	}
	q := u.Query()
	q.Set("category", "linear")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		monitoring.RecordBybitRest("/v5/market/tickers", false)
		return nil, 0, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		monitoring.RecordBybitRest("/v5/market/tickers", false)
		return nil, 0, fmt.Errorf("bybit tickers bad status: %d body=%s", resp.StatusCode, string(body))
	}

	var decoded tickersResp
	if err := json.Unmarshal(body, &decoded); err != nil {
		monitoring.RecordBybitRest("/v5/market/tickers", false)
		return nil, 0, fmt.Errorf("decode tickers: %w", err)
	}
	if decoded.RetCode != 0 {
		monitoring.RecordBybitRest("/v5/market/tickers", false)
		return nil, decoded.Time, fmt.Errorf("bybit tickers retCode=%d retMsg=%s", decoded.RetCode, decoded.RetMsg)
	}

	monitoring.RecordBybitRest("/v5/market/tickers", true)

	ts := decoded.Time
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	return decoded.Result.List, ts, nil
}

// FetchFundingRateLatest запрашивает последнюю запись funding rate для одного символа.
func (c *APIClient) FetchFundingRateLatest(ctx context.Context, symbol string) (*FundingRatePoint, int64, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("nil bybit client")
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, 0, fmt.Errorf("empty symbol")
	}

	u, err := url.Parse(c.baseURL + "/v5/market/funding/history")
	if err != nil {
		return nil, 0, fmt.Errorf("parse base url: %w", err)
	}
	q := u.Query()
	q.Set("category", "linear")
	q.Set("symbol", symbol)
	q.Set("limit", "1")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		monitoring.RecordBybitRest("/v5/market/funding/history", false)
		return nil, 0, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		monitoring.RecordBybitRest("/v5/market/funding/history", false)
		return nil, 0, fmt.Errorf("bybit funding bad status: %d body=%s", resp.StatusCode, string(body))
	}

	var decoded fundingHistoryResp
	if err := json.Unmarshal(body, &decoded); err != nil {
		monitoring.RecordBybitRest("/v5/market/funding/history", false)
		return nil, 0, fmt.Errorf("decode funding: %w", err)
	}
	if decoded.RetCode != 0 {
		monitoring.RecordBybitRest("/v5/market/funding/history", false)
		return nil, decoded.Time, fmt.Errorf("bybit funding retCode=%d retMsg=%s", decoded.RetCode, decoded.RetMsg)
	}
	monitoring.RecordBybitRest("/v5/market/funding/history", true)

	ts := decoded.Time
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	if len(decoded.Result.List) == 0 {
		return nil, ts, nil
	}
	pt := decoded.Result.List[0]
	if pt.Symbol == "" {
		pt.Symbol = symbol
	}
	return &pt, ts, nil
}

// FetchAndPublishMarketData — Bybit аналог binance.FetchAndPublishMarketData.
// Публикует:
//   - stream:bybit:ticker-24h  (все linear тикеры за 24ч)
//   - stream:bybit:funding-rate (последние funding rates по allowlist symbols)
//
// ENV:
//
//	BYBIT_TICKERS_ENABLED           (default true)
//	BYBIT_FUNDING_ENABLED           (default true)
//	BYBIT_FUNDING_SYMBOLS           (default BTCUSDT,ETHUSDT)
//	BYBIT_FUNDING_MAX_CONCURRENCY   (default 5)
func FetchAndPublishMarketData(ctx context.Context) error {
	client := NewAPIClient()

	// По умолчанию включаем оба типа данных, но позволяем отключить точечно.
	tickersEnabled := os.Getenv("BYBIT_TICKERS_ENABLED")
	if tickersEnabled == "" {
		tickersEnabled = "true"
	}
	fundingEnabled := os.Getenv("BYBIT_FUNDING_ENABLED")
	if fundingEnabled == "" {
		fundingEnabled = "true"
	}

	if tickersEnabled == "true" {
		tickers, tsMs, err := client.Fetch24hTickers(ctx)
		if err != nil {
			return err
		}
		if err := client.publisher.PublishTicker24h(ctx, tickers, tsMs, streams.BybitTicker24h); err != nil {
			return err
		}
	}

	if fundingEnabled == "true" {
		symbols := parseSymbols(getEnv("BYBIT_FUNDING_SYMBOLS", "BTCUSDT,ETHUSDT"))
		maxConc := getEnvInt("BYBIT_FUNDING_MAX_CONCURRENCY", 5)
		if maxConc <= 0 {
			maxConc = 5
		}
		points, tsMs, err := fetchFundingMulti(ctx, client, symbols, maxConc)
		if err != nil {
			// Funding — вспомогательная телеметрия: не роняем общий сбор, если
			// тикеры отработали. Логируем и продолжаем.
			zap.S().Errorf("⚠️ Bybit funding fetch failed: %v", err)
		} else if len(points) > 0 {
			_ = client.publisher.PublishFundingRates(ctx, points, tsMs, streams.BybitFundingRate)
		}
	}

	return nil
}

func fetchFundingMulti(ctx context.Context, client *APIClient, symbols []string, maxConc int) ([]FundingRatePoint, int64, error) {
	if len(symbols) == 0 {
		return nil, 0, nil
	}

	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	mu := sync.Mutex{}
	var out []FundingRatePoint
	var lastTs int64
	var firstErr error

	for _, s := range symbols {
		sym := s
		if sym == "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			pt, ts, err := client.FetchFundingRateLatest(ctx, sym)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if pt == nil {
				return
			}
			mu.Lock()
			out = append(out, *pt)
			if ts > lastTs {
				lastTs = ts
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	if lastTs <= 0 {
		lastTs = time.Now().UnixMilli()
	}
	return out, lastTs, firstErr
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func parseSymbols(symbolsStr string) []string {
	parts := strings.Split(symbolsStr, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
