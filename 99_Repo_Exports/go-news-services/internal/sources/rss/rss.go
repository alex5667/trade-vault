package rss

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"

	"trade-news-ingestor/internal/ingestor"
)

type Config struct {
	Name        string
	URLs        []string
	HTTPTimeout time.Duration
	UserAgent   string

	// NEW: bucket for StableUID in RSS source
	NewsUIDBucket time.Duration
}

type RSSSource struct {
	cfg    Config
	client *http.Client
	parser *gofeed.Parser
}

func NewRSSSource(cfg Config) *RSSSource {
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 8 * time.Second
	}
	if cfg.NewsUIDBucket <= 0 {
		cfg.NewsUIDBucket = 6 * time.Hour
	}
	// safety clamp
	if cfg.NewsUIDBucket < 15*time.Minute {
		cfg.NewsUIDBucket = 6 * time.Hour
	}

	cl := &http.Client{Timeout: cfg.HTTPTimeout}
	p := gofeed.NewParser()
	return &RSSSource{cfg: cfg, client: cl, parser: p}
}

func (s *RSSSource) Fetch(ctx context.Context) ([]ingestor.NewsRawItem, error) {
	if len(s.cfg.URLs) == 0 {
		return nil, nil
	}
	var out []ingestor.NewsRawItem

	for _, u := range s.cfg.URLs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}

		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		if s.cfg.UserAgent != "" {
			req.Header.Set("User-Agent", s.cfg.UserAgent)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			continue // fail-open per-source
		}
		if resp.Body != nil {
			defer resp.Body.Close()
		}

		feed, err := s.parser.Parse(resp.Body)
		if err != nil || feed == nil {
			continue
		}

		now := time.Now().UnixMilli()
		for _, it := range feed.Items {
			if it == nil {
				continue
			}
			title := strings.TrimSpace(it.Title)
			link := strings.TrimSpace(it.Link)
			if title == "" || link == "" {
				continue
			}

			pubMs := int64(0)
			if it.PublishedParsed != nil {
				pubMs = it.PublishedParsed.UnixMilli()
			} else if it.UpdatedParsed != nil {
				pubMs = it.UpdatedParsed.UnixMilli()
			}

			publishedForStream := pubMs
			if publishedForStream <= 0 {
				publishedForStream = now
			}

			bucketMs := ingestor.BucketStartMsOrZero(pubMs, s.cfg.NewsUIDBucket)

			guid := strings.TrimSpace(it.GUID)
			uid := ingestor.StableUID(s.cfg.Name, link, title, guid, strconv.FormatInt(bucketMs, 10))

			summary := ""
			if it.Description != "" {
				summary = strings.TrimSpace(it.Description)
			}

			// На ingest не пытаемся “умно” парсить символы → оставляем пусто
			symbolsJSON, _ := json.Marshal([]string{})

			payload := map[string]any{
				"feed_title": feed.Title,
				"feed_link":  feed.Link,
			}
			payloadJSON, _ := json.Marshal(payload)

			out = append(out, ingestor.NewsRawItem{
				UID:           uid,
				PublishedTSms: publishedForStream,
				IngestedTSms:  now,
				Source:        s.cfg.Name,
				Title:         title,
				URL:           link,
				Summary:       summary,
				SymbolsJSON:   string(symbolsJSON),
				Importance:    0.0,
				PayloadJSON:   string(payloadJSON),
			})
		}
	}

	return out, nil
}

func ingestorTimeBucket(ms int64) string {
	// строкой, чтобы совпадать с python stable_uid(parts...)
	return strings.TrimSpace(time.UnixMilli(ms).UTC().Format(time.RFC3339))
}
