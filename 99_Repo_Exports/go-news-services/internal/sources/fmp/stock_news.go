package fmp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"trade-news-ingestor/internal/ingestor"
	"trade-news-ingestor/internal/sources/common"
)

type StockNewsConfig struct {
	Name          string
	Tickers       []string
	Limit         int
	DedupeTTL     time.Duration
	NewsUIDBucket time.Duration
	BaseURL       string // для тестов; если пусто => https://financialmodelingprep.com
	HTTPTimeout   time.Duration
	UserAgent     string
}

type StockNewsSource struct {
	cfg StockNewsConfig
	hc  *http.Client
}

func NewStockNews(cfg StockNewsConfig) *StockNewsSource {
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 8 * time.Second
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 50
	}
	if cfg.DedupeTTL <= 0 {
		cfg.DedupeTTL = 7 * 24 * time.Hour
	}
	if cfg.NewsUIDBucket <= 0 {
		cfg.NewsUIDBucket = 6 * time.Hour
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://financialmodelingprep.com"
	}
	return &StockNewsSource{cfg: cfg, hc: &http.Client{Timeout: cfg.HTTPTimeout}}
}

type stockNewsRow struct {
	Symbol        string `json:"symbol"`
	PublishedDate string `json:"publishedDate"`
	Title         string `json:"title"`
	URL           string `json:"url"`
	Text          string `json:"text"`
	Site          string `json:"site"`
}

func (s *StockNewsSource) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
	key := os.Getenv("FMP_API_KEY")
	if key == "" {
		return nil, nil
	}

	u, _ := url.Parse(s.cfg.BaseURL + "/api/v3/stock_news")
	q := u.Query()
	if len(s.cfg.Tickers) > 0 {
		q.Set("tickers", strings.Join(s.cfg.Tickers, ","))
	}
	q.Set("limit", fmt.Sprintf("%d", s.cfg.Limit))
	q.Set("apikey", key)
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if s.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", s.cfg.UserAgent)
	}

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fmp stock_news http status=%d", resp.StatusCode)
	}

	var rows []stockNewsRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}

	nowMs := time.Now().UnixMilli()
	out := make([]ingestor.NewsRawItem, 0, len(rows))

	for _, r := range rows {
		pubMs := parseFMPTimeMs(r.PublishedDate)
		publishedForStream := pubMs
		if publishedForStream <= 0 {
			publishedForStream = nowMs // downstream-friendly
		}

		bucketMs := ingestor.BucketStartMsOrZero(pubMs, s.cfg.NewsUIDBucket)

		// providerID усиливаем: symbol|site|publishedDate
		providerID := r.Symbol + "|" + r.Site + "|" + r.PublishedDate
		// если PublishedDate пустая/кривая — всё равно оставляем стабильный providerID (symbol|site)
		if r.PublishedDate == "" {
			providerID = r.Symbol + "|" + r.Site
		}

		uid := ingestor.StableUID("fmp", r.URL, r.Title, providerID, strconv.FormatInt(bucketMs, 10))

		syms := []string{}
		if r.Symbol != "" {
			syms = []string{r.Symbol}
		}

		item := ingestor.NewsRawItem{
			UID:           uid,
			PublishedTSms: publishedForStream,
			IngestedTSms:  nowMs,
			Source:        "fmp",
			Title:         r.Title,
			URL:           r.URL,
			Summary:       r.Text,
			SymbolsJSON:   common.JSONArrayStrings(syms),
			Importance:    0.0,
			PayloadJSON:   common.JSONObject(r),
		}
		out = append(out, item)
	}

	return out, nil
}

func parseFMPTimeMs(s string) int64 {
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

