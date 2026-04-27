// StableCoinPoller fetches the combined USDT+USDC market-cap dominance from
// the CoinGecko public API (no key required for /api/v3/global) and forwards
// it to a crossasset.StableCoinUpdater (implemented by crossasset.Tracker).
//
// Poll interval: 60 s (well within CoinGecko free tier rate limits ~30 req/min).
//
// CoinGecko /api/v3/global returns dominance percentages under
//
//	data.market_cap_percentage.usdt  (float)
//	data.market_cap_percentage.usdc  (float)
//
// We sum both and pass to UpdateStableCoin.
package marketdata

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	defaultCGBaseURL  = "https://api.coingecko.com/api/v3"
	defaultSCInterval = 60 * time.Second
	defaultSCHTTPTO   = 30 * time.Second
	maxBodySizeBytes  = 32 * 1024
)

// StableCoinUpdater is the interface satisfied by crossasset.Tracker.
// We define it here to avoid an import cycle.
type StableCoinUpdater interface {
	UpdateStableCoin(ctx context.Context, combinedDominance float64)
}

// StableCoinPollerConfig configures StableCoinPoller.
type StableCoinPollerConfig struct {
	// BaseURL for CoinGecko API (default: https://api.coingecko.com/api/v3).
	// Override with COINGECKO_BASE_URL env var or for testing.
	BaseURL string

	// Interval between polls (default: 60s).
	Interval time.Duration

	// APIKey is optional — passed as x-cg-demo-api-key header if provided.
	// Free tier works without a key.
	APIKey string

	Logger *zap.SugaredLogger
}

// StableCoinPoller polls CoinGecko for USDT+USDC dominance and forwards
// the combined percentage to a StableCoinUpdater.
type StableCoinPoller struct {
	cfg     StableCoinPollerConfig
	updater StableCoinUpdater
	client  *http.Client
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewStableCoinPoller creates a new StableCoinPoller.
func NewStableCoinPoller(updater StableCoinUpdater, cfg StableCoinPollerConfig) *StableCoinPoller {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultCGBaseURL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultSCInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.S()
	}
	return &StableCoinPoller{
		cfg:     cfg,
		updater: updater,
		client:  &http.Client{Timeout: defaultSCHTTPTO},
		stopCh:  make(chan struct{}),
	}
}

// Start begins polling in a background goroutine.
func (p *StableCoinPoller) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// Initial fetch with small delay to let the exchange connection settle.
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			p.poll(ctx)
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		}

		ticker := time.NewTicker(p.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.poll(ctx)
			case <-p.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully shuts down the poller.
func (p *StableCoinPoller) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// cgGlobalResponse is the relevant subset of CoinGecko's /api/v3/global response.
type cgGlobalResponse struct {
	Data struct {
		MarketCapPercentage map[string]float64 `json:"market_cap_percentage"`
	} `json:"data"`
}

// poll fetches CoinGecko global data and forwards combined USDT+USDC dominance.
func (p *StableCoinPoller) poll(ctx context.Context) {
	url := p.cfg.BaseURL + "/global"

	reqCtx, cancel := context.WithTimeout(ctx, defaultSCHTTPTO)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		p.cfg.Logger.Errorf("⚠️ stablecoin-poller: build request error: %v", err)
		return
	}
	req.Header.Set("Accept", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("x-cg-demo-api-key", p.cfg.APIKey)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.cfg.Logger.Errorf("⚠️ stablecoin-poller: HTTP error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		p.cfg.Logger.Warnf("⚠️ stablecoin-poller: rate limited by CoinGecko (HTTP 429)")
		return
	}
	if resp.StatusCode != http.StatusOK {
		p.cfg.Logger.Warnf("⚠️ stablecoin-poller: HTTP %d from CoinGecko", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySizeBytes))
	if err != nil {
		p.cfg.Logger.Errorf("⚠️ stablecoin-poller: read body error: %v", err)
		return
	}

	var gr cgGlobalResponse
	if err := json.Unmarshal(body, &gr); err != nil {
		p.cfg.Logger.Errorf("⚠️ stablecoin-poller: JSON parse error: %v", err)
		return
	}

	usdtDom := gr.Data.MarketCapPercentage["usdt"]
	usdcDom := gr.Data.MarketCapPercentage["usdc"]
	combined := usdtDom + usdcDom

	if combined <= 0 {
		p.cfg.Logger.Warnf("⚠️ stablecoin-poller: zero dominance received (USDT=%.2f USDC=%.2f), skipping", usdtDom, usdcDom)
		return
	}

	p.cfg.Logger.Infof("📊 stablecoin-poller: USDT=%.2f%% USDC=%.2f%% combined=%.2f%%", usdtDom, usdcDom, combined)
	p.updater.UpdateStableCoin(ctx, combined)
}
