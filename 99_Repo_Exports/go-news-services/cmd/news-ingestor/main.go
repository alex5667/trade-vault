package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"trade-news-ingestor/internal/calendar"
	"trade-news-ingestor/internal/config"
	"trade-news-ingestor/internal/ingestor"
	"trade-news-ingestor/internal/redisx"
	"trade-news-ingestor/internal/sources/rss"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.FromEnv()

	logger := log.New(os.Stdout, "[news-ingestor] ", log.LstdFlags|log.Lmicroseconds)

	// Log active providers
	logger.Printf("Active providers: RSS=%v CryptoPanic=%v FMP=%v NewsAPI=%v",
		cfg.Sources.Flags.RSS,
		cfg.Sources.Flags.Cryptopanic,
		cfg.Sources.Flags.FMP,
		cfg.Sources.Flags.NewsAPI)

	rdb := redisx.NewClient(cfg.RedisURL)
	defer func() { _ = rdb.Close() }()

	// Wait for Redis to be ready
	waitForRedis(context.Background(), rdb, logger)

	// --- Источники ---
	rssSource := rss.NewRSSSource(rss.Config{
		Name:          "rss",
		URLs:          cfg.RSSURLs,
		HTTPTimeout:   cfg.HTTPTimeout,
		UserAgent:     cfg.UserAgent,
		NewsUIDBucket: cfg.NewsUIDBucket,
	})

	// Calendar source: FMP (fail-open)
	calCfg := calendar.FMPCalendarConfig{
		Name:          "fmp-calendar",
		APIKey:        os.Getenv("FMP_API_KEY"),
		BaseURL:       os.Getenv("FMP_BASE_URL"), // optional
		HTTPTimeout:   cfg.HTTPTimeout,
		UserAgent:     cfg.UserAgent,
		LookaheadDays: getEnvInt("CALENDAR_LOOKAHEAD_DAYS", 14),
		BackDays:      getEnvInt("CALENDAR_BACK_DAYS", 1),
		Countries:     cfg.Sources.FMP.Economic.Countries,
		Importance:    cfg.Sources.FMP.Economic.Importance,
		Enabled:       cfg.Sources.Flags.FMP,
	}
	calSource := calendar.NewFMPCalendarSource(calCfg)

	pipe := ingestor.NewPipeline(ingestor.PipelineConfig{
		Redis:            rdb,
		StreamNewsRaw:    cfg.StreamNewsRaw,
		StreamCalEvents:  cfg.StreamCalendarEvents,
		StreamNewsHB:     cfg.StreamNewsHB,
		StreamCalHB:      cfg.StreamCalHB,
		DedupeTTL:        cfg.DedupeTTL,
		MaxStreamLen:     cfg.MaxStreamLen,
		HeartbeatTTL:     cfg.HeartbeatTTL,
		InstanceID:       cfg.InstanceID,
		Logger:           logger,
		RSS:              rssSource,
		Calendar:         calSource,
		PollInterval:     cfg.PollInterval,
		CalPollInterval:  cfg.CalPollInterval,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Health endpoint (по желанию)
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			// дешёвый health: Redis ping + наличие heartbeat (опционально)
			if err := rdb.Ping(r.Context()).Err(); err != nil {
				w.WriteHeader(500)
				_, _ = w.Write([]byte("redis_error"))
				return
			}
			_, _ = w.Write([]byte("ok"))
		})
		srv := &http.Server{Addr: cfg.HTTPListen, Handler: mux}
		logger.Printf("http listen %s", cfg.HTTPListen)
		_ = srv.ListenAndServe()
	}()

	// stop signals
	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		logger.Printf("shutdown signal received")
		cancel()
	}()

	if err := pipe.Run(ctx); err != nil {
		logger.Printf("pipeline stopped with error: %v", err)
		os.Exit(1)
	}
	logger.Printf("stopped cleanly")
	time.Sleep(50 * time.Millisecond)
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func waitForRedis(ctx context.Context, rdb *redis.Client, logger *log.Logger) {
	maxRetries := 60
	for i := 0; i < maxRetries; i++ {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			logger.Printf("Redis connection established")
			return
		}
		logger.Printf("Waiting for Redis (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(5 * time.Second)
	}
	logger.Printf("Failed to connect to Redis after %d attempts", maxRetries)
	os.Exit(1)
}
