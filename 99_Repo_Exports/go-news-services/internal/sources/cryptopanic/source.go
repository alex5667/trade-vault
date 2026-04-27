package cryptopanic

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

type Config struct {
	Name         string
	Currencies   []string
	Filter       string // "important" etc.
	Kind         string // "news"
	Regions      string // "en" etc.
	DedupeTTL    time.Duration // 7d из ENV DEDUPE_TTL
	NewsUIDBucket time.Duration // бакет для UID из ENV NEWS_UID_BUCKET
	BaseURL      string        // для тестов; если пусто => https://cryptopanic.com
	HTTPTimeout  time.Duration
	UserAgent    string
}

type Source struct {
	cfg Config
	hc  *http.Client
}

func New(cfg Config) *Source {
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 8 * time.Second
	}
	if cfg.DedupeTTL <= 0 {
		cfg.DedupeTTL = 7 * 24 * time.Hour
	}
	if cfg.NewsUIDBucket <= 0 {
		cfg.NewsUIDBucket = 6 * time.Hour
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://cryptopanic.com"
	}
	return &Source{
		cfg: cfg,
		hc:  &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

// Минимально достаточная форма ответа: берём Title/URL/Published/Currencies/Source.
// Всё остальное кладём в PayloadJSON целиком (для дебага/LLM/реплея).
type apiResp struct {
	Results []map[string]any `json:"results"`
}

func (s *Source) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
	token := os.Getenv("CRYPTOPANIC_AUTH_TOKEN")
	if token == "" {
		return nil, nil
	}

	u, _ := url.Parse(s.cfg.BaseURL + "/api/v1/posts/")
	q := u.Query()
	q.Set("auth_token", token)

	if len(s.cfg.Currencies) > 0 {
		q.Set("currencies", strings.Join(s.cfg.Currencies, ","))
	}
	if s.cfg.Kind != "" {
		q.Set("kind", s.cfg.Kind)
	}
	if s.cfg.Filter != "" {
		q.Set("filter", s.cfg.Filter)
	}
	if s.cfg.Regions != "" {
		q.Set("regions", s.cfg.Regions)
	}
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
		return nil, fmt.Errorf("cryptopanic http status=%d", resp.StatusCode)
	}

	var ar apiResp
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, err
	}

	nowMs := time.Now().UnixMilli()
	out := make([]ingestor.NewsRawItem, 0, len(ar.Results))

	for _, r := range ar.Results {
		title := getStr(r, "title")
		urlStr := getStr(r, "url")
		publishedAt := getStr(r, "published_at")
		pubMs := parseRFC3339Ms(publishedAt)
		if pubMs <= 0 {
			pubMs = nowMs
		}

		// currencies: [{"code":"BTC"}...]
		syms := extractCurrencyCodes(r)
		symsJSON := common.JSONArrayStrings(syms)

		// UID: provider + url + title + provider_id + ts_bucket
		bucketMs := ingestor.BucketStartMsOrZero(pubMs, s.cfg.NewsUIDBucket)
		providerID := getStr(r, "id")
		uid := ingestor.StableUID("cryptopanic", urlStr, title, providerID, strconv.FormatInt(bucketMs, 10))

		item := ingestor.NewsRawItem{
			UID:           uid,
			PublishedTSms: pubMs,
			IngestedTSms:  nowMs,
			Source:        "cryptopanic",
			Title:         title,
			URL:           urlStr,
			Summary:       "",       // не раздуваем; summary сделает analyzer/LLM
			SymbolsJSON:   symsJSON,  // строка JSON массива
			Importance:    0.0,       // без догадок: вес/важность пусть ставит analyzer/feature-store
			PayloadJSON:   common.JSONObject(r),
		}
		out = append(out, item)
	}

	return out, nil
}


func parseRFC3339Ms(s string) int64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

func getStr(m map[string]any, k string) string {
	v, ok := m[k]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func extractCurrencyCodes(m map[string]any) []string {
	raw, ok := m["currencies"]
	if !ok || raw == nil {
		return []string{}
	}
	arr, ok := raw.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, it := range arr {
		obj, ok := it.(map[string]any)
		if !ok {
			continue
		}
		code := getStr(obj, "code")
		if code != "" {
			out = append(out, code)
		}
	}
	return out
}
