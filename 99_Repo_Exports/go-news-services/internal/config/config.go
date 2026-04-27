package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	RedisURL            string
	StreamNewsRaw       string
	StreamCalendarEvents string
	StreamNewsHB        string
	StreamCalHB         string

	RSSURLs            []string
	PollInterval       time.Duration
	CalPollInterval    time.Duration

	DedupeTTL          time.Duration
	NewsUIDBucket      time.Duration
	MaxStreamLen       int64
	HeartbeatTTL       time.Duration
	InstanceID         string

	HTTPTimeout        time.Duration
	UserAgent          string

	HTTPListen         string

	// News sources configuration
	Sources NewsSourcesConfig
}

func FromEnv() Config {
	get := func(k, def string) string {
		v := strings.TrimSpace(os.Getenv(k))
		if v == "" {
			return def
		}
		return v
	}
	getDur := func(k string, def time.Duration) time.Duration {
		v := strings.TrimSpace(os.Getenv(k))
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return def
		}
		return d
	}
	getInt64 := func(k string, def int64) int64 {
		v := strings.TrimSpace(os.Getenv(k))
		if v == "" {
			return def
		}
		// простая конверсия без паники
		var n int64
		for _, c := range v {
			if c < '0' || c > '9' {
				return def
			}
			n = n*10 + int64(c-'0')
		}
		if n <= 0 {
			return def
		}
		return n
	}
	splitCSV := func(s string) []string {
		var out []string
		for _, p := range strings.Split(s, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	// Load news sources configuration first
	sources := LoadNewsSourcesFromEnv()

	rssUrls := splitCSV(get("RSS_URLS", ""))
	if len(rssUrls) == 0 && sources.RSS.Enabled {
		rssUrls = sources.RSS.URLs
	}

	cfg := Config{
		RedisURL:            get("REDIS_URL", "redis://redis-worker-1:6379/0"),
		StreamNewsRaw:       get("STREAM_NEWS_RAW", "news:raw"),
		StreamCalendarEvents:get("STREAM_CALENDAR_EVENTS", "calendar:events"),
		StreamNewsHB:        get("STREAM_NEWS_HB", "news:hb"),
		StreamCalHB:         get("STREAM_CAL_HB", "calendar:hb"),

		RSSURLs:            rssUrls,
		PollInterval:       getDur("POLL_INTERVAL", 15*time.Second),
		CalPollInterval:    getDur("CAL_POLL_INTERVAL", 60*time.Second),

		DedupeTTL:          getDur("DEDUPE_TTL", 48*time.Hour),
		// NEWS_UID_BUCKET: time bucket used inside StableUID(...) for news items.
		// Smaller bucket => fewer accidental dedupe collisions, more reprocessing.
		// Recommended: 6h (default).
		NewsUIDBucket:      getDur("NEWS_UID_BUCKET", 6*time.Hour),
		MaxStreamLen:       getInt64("MAX_STREAM_LEN", 200000),
		HeartbeatTTL:       getDur("HEARTBEAT_TTL", 30*time.Second),
		InstanceID:         get("INSTANCE_ID", "news-ingestor-1"),

		HTTPTimeout:        getDur("HTTP_TIMEOUT", 12*time.Second),
		UserAgent:          get("USER_AGENT", "trade-news-ingestor/1.0"),
		HTTPListen:         get("HTTP_LISTEN", ":8097"),

		// News sources configuration
		Sources: sources,
	}

	// safety clamp (avoid zero / negative / too small buckets)
	if cfg.NewsUIDBucket < 15*time.Minute {
		cfg.NewsUIDBucket = 6 * time.Hour
	}

	return cfg
}
