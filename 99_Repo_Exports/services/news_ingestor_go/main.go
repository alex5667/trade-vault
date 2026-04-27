package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------- Config ----------------

type Config struct {
	RedisURL       string
	StreamNewsRaw  string
	DLQNewsRaw     string
	LeaderKey      string
	LeaderTTL      time.Duration
	PollInterval   time.Duration
	RSSURLs        []string
	HTTPTimeout    time.Duration
	DedupeTTL      time.Duration
	StreamMaxLen   int64
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func parseCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadConfig() Config {
	return Config{
		RedisURL:      getEnv("REDIS_URL", "redis://localhost:6379/0"),
		StreamNewsRaw: getEnv("NEWS_RAW_STREAM", "news:raw"),
		DLQNewsRaw:    getEnv("NEWS_RAW_DLQ", "news:raw:dlq"),

		LeaderKey:    getEnv("NEWS_INGESTOR_LEADER_KEY", "news:ingestor:leader"),
		LeaderTTL:    8 * time.Second,
		PollInterval: 10 * time.Second,

		RSSURLs:     parseCSV(getEnv("NEWS_RSS_URLS", "")),
		HTTPTimeout: 6 * time.Second,

		DedupeTTL:    30 * time.Minute,
		StreamMaxLen: 100000, // approximate trimming (maxlen ~)
	}
}

// ---------------- RSS parsing ----------------

type rss struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title     string `xml:"title"`
	Link      string `xml:"link"`
	GUID      string `xml:"guid"`
	PubDate   string `xml:"pubDate"`
	DCDate    string `xml:"date"` // sometimes
}

// best-effort parse pubDate
func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().UTC()
	}
	// RFC1123Z / RFC1123 / RFC3339
	layouts := []string{
		time.RFC1123Z,
		time.RFC1123,
		time.RFC3339,
		"Mon, 02 Jan 2006 15:04:05 -0700",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

// ---------------- Leader lock ----------------

// Simple SET NX PX lock, renew by SET XX
type LeaderLock struct {
	rdb   *redis.Client
	key   string
	value string
	ttl   time.Duration
}

func NewLeaderLock(rdb *redis.Client, key string, ttl time.Duration) *LeaderLock {
	// unique value per process
	return &LeaderLock{
		rdb:   rdb,
		key:   key,
		value: fmt.Sprintf("go:%d", time.Now().UnixNano()),
		ttl:   ttl,
	}
}

func (l *LeaderLock) TryAcquire(ctx context.Context) (bool, error) {
	ok, err := l.rdb.SetNX(ctx, l.key, l.value, l.ttl).Result()
	return ok, err
}

func (l *LeaderLock) Renew(ctx context.Context) error {
	// renew only if value matches: use Lua
	script := redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)
	ttlms := fmt.Sprintf("%d", l.ttl.Milliseconds())
	_, err := script.Run(ctx, l.rdb, []string{l.key}, l.value, ttlms).Result()
	return err
}

// ---------------- Dedupe ----------------

func stableUID(source, url, title string, tsBucket int64) string {
	// uid = sha1(source|url|title|bucket)
	h := sha1.New()
	fmt.Fprintf(h, "%s|%s|%s|%d", source, url, title, tsBucket)
	return hex.EncodeToString(h.Sum(nil))
}

func tsBucketMs(ts time.Time, bucketSec int64) int64 {
	sec := ts.Unix()
	return (sec / bucketSec) * bucketSec
}

// dedupe key: news:dedupe:<uid>
func markDedupe(ctx context.Context, rdb *redis.Client, uid string, ttl time.Duration) (bool, error) {
	key := "news:dedupe:" + uid
	ok, err := rdb.SetNX(ctx, key, "1", ttl).Result()
	return ok, err
}

// ---------------- Redis write ----------------

func xadd(ctx context.Context, rdb *redis.Client, stream string, maxlen int64, fields map[string]any) error {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: fields,
	}
	if maxlen > 0 {
		args.MaxLenApprox = maxlen
	}
	_, err := rdb.XAdd(ctx, args).Result()
	return err
}

// ---------------- Main loop ----------------

func fetchRSS(ctx context.Context, url string, timeout time.Duration) ([]rssItem, error) {
	cl := &http.Client{Timeout: timeout}
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var doc rss
	if err := xml.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}
	return doc.Channel.Items, nil
}

func main() {
	cfg := loadConfig()

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("bad REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opt)

	ctx := context.Background()
	lock := NewLeaderLock(rdb, cfg.LeaderKey, cfg.LeaderTTL)

	for {
		// leader election
		acq, err := lock.TryAcquire(ctx)
		if err != nil {
			log.Printf("leader acquire error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if !acq {
			// not leader -> sleep and retry
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("I am leader (go ingestor). RSS count=%d", len(cfg.RSSURLs))

		// leader loop
		for {
			_ = lock.Renew(ctx) // best-effort

			start := time.Now()
			for _, rssURL := range cfg.RSSURLs {
				items, err := fetchRSS(ctx, rssURL, cfg.HTTPTimeout)
				if err != nil {
					log.Printf("rss fetch error url=%s err=%v", rssURL, err)
					continue
				}
				for _, it := range items {
					title := strings.TrimSpace(it.Title)
					link := strings.TrimSpace(it.Link)
					if title == "" || link == "" {
						continue
					}
					t := parseTime(it.PubDate)
					if it.DCDate != "" {
						t = parseTime(it.DCDate)
					}

					// bucket 5 minutes for dedupe stability
					bucket := tsBucketMs(t, 300)
					uid := stableUID("rss:"+rssURL, link, title, bucket)

					ok, err := markDedupe(ctx, rdb, uid, cfg.DedupeTTL)
					if err != nil {
						log.Printf("dedupe err uid=%s err=%v", uid, err)
						continue
					}
					if !ok {
						continue // already seen
					}

					fields := map[string]any{
						"uid":    uid,
						"source": "rss:" + rssURL,
						"title":  title,
						"url":    link,
						"ts_ms":  t.UnixMilli(),
						// symbol/asset_class пустые: analyzer может дополнить
						"symbol":      "",
						"asset_class": "",
					}

					if err := xadd(ctx, rdb, cfg.StreamNewsRaw, cfg.StreamMaxLen, fields); err != nil {
						// fail-open with DLQ write (best-effort)
						_ = xadd(ctx, rdb, cfg.DLQNewsRaw, cfg.StreamMaxLen, map[string]any{
							"uid": uid, "err": err.Error(), "source": fields["source"], "url": link, "ts_ms": fields["ts_ms"],
						})
						log.Printf("xadd error: %v", err)
						continue
					}
				}
			}

			// pacing
			elapsed := time.Since(start)
			sleep := cfg.PollInterval - elapsed
			if sleep < 200*time.Millisecond {
				sleep = 200 * time.Millisecond
			}
			time.Sleep(sleep)

			// if lock lost -> break, let python takeover
			val, _ := rdb.Get(ctx, cfg.LeaderKey).Result()
			if val != lock.value {
				log.Printf("leader lost -> exit leader loop")
				break
			}
		}
	}
}
