package newsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"trade-news-ingestor/internal/ingestor"
	"trade-news-ingestor/internal/sources/common"
)

type Config struct {
	Name          string
	Q             string
	Language      string
	PageSize      int
	DedupeTTL     time.Duration
	NewsUIDBucket time.Duration
	BaseURL       string // для тестов; если пусто => https://newsapi.org
	HTTPTimeout   time.Duration
	UserAgent     string
}

type Source struct {
	cfg Config
	hc  *http.Client
}

func New(cfg Config) *Source {
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 8 * time.Second
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 50
	}
	if cfg.DedupeTTL <= 0 {
		cfg.DedupeTTL = 7 * 24 * time.Hour
	}
	if cfg.NewsUIDBucket <= 0 {
		cfg.NewsUIDBucket = 6 * time.Hour
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://newsapi.org"
	}
	return &Source{cfg: cfg, hc: &http.Client{Timeout: cfg.HTTPTimeout}}
}

type apiResp struct {
	Status   string `json:"status"`
	Articles []struct {
		Title       string `json:"title"`
		URL         string `json:"url"`
		PublishedAt string `json:"publishedAt"`
		Source      struct {
			Name string `json:"name"`
		} `json:"source"`
		Description string `json:"description"`
	} `json:"articles"`
}

func (s *Source) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
	key := os.Getenv("NEWSAPI_KEY")
	if key == "" {
		return nil, nil
	}

	u, _ := url.Parse(s.cfg.BaseURL + "/v2/everything")
	q := u.Query()
	q.Set("q", s.cfg.Q)
	if s.cfg.Language != "" {
		q.Set("language", s.cfg.Language)
	}
	q.Set("pageSize", fmt.Sprintf("%d", s.cfg.PageSize))
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	req.Header.Set("X-Api-Key", key)
	if s.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", s.cfg.UserAgent)
	}

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("newsapi http status=%d", resp.StatusCode)
	}

	var ar apiResp
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, err
	}

	nowMs := time.Now().UnixMilli()
	out := make([]ingestor.NewsRawItem, 0, len(ar.Articles))

	for _, a := range ar.Articles {
		pubMs := parseRFC3339Ms(a.PublishedAt)
		publishedForStream := pubMs
		if publishedForStream <= 0 {
			publishedForStream = nowMs
		}
		bucketMs := ingestor.BucketStartMsOrZero(pubMs, s.cfg.NewsUIDBucket)
		// NewsAPI: strengthen providerID with source to reduce collisions
		srcID := a.Source.Name
		if srcID == "" {
			srcID = "unknown"
		}
		providerID := srcID + "|" + a.PublishedAt
		if a.PublishedAt == "" {
			providerID = srcID + "|" + a.URL
		}
		uid := ingestor.StableUID("newsapi", a.URL, a.Title, providerID, strconv.FormatInt(bucketMs, 10))

		item := ingestor.NewsRawItem{
			UID:           uid,
			PublishedTSms: publishedForStream,
			IngestedTSms:  nowMs,
			Source:        "newsapi",
			Title:         a.Title,
			URL:           a.URL,
			Summary:       "",                    // не раздуваем
			SymbolsJSON:   common.JSONArrayStrings([]string{}), // неизвестно
			Importance:    0.0,
			PayloadJSON:   common.JSONObject(a),
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

