package ingestor

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type PipelineConfig struct {
	Redis           *redis.Client
	StreamNewsRaw   string
	StreamCalEvents string
	StreamNewsHB    string
	StreamCalHB     string

	DedupeTTL       time.Duration
	MaxStreamLen    int64
	HeartbeatTTL    time.Duration
	InstanceID      string

	PollInterval    time.Duration
	CalPollInterval time.Duration

	Logger   *log.Logger
	RSS      RSSSource
	Calendar CalendarSource
}

type Pipeline struct{ cfg PipelineConfig }

func NewPipeline(cfg PipelineConfig) *Pipeline { return &Pipeline{cfg: cfg} }

func (p *Pipeline) Run(ctx context.Context) error {
	// Если RSS_URLS не заданы — это конфигурационная ошибка, но сервис всё равно может работать как календарь.
	p.cfg.Logger.Printf("start instance=%s news_stream=%s cal_stream=%s",
		p.cfg.InstanceID, p.cfg.StreamNewsRaw, p.cfg.StreamCalEvents)

	// Два таймера: новости и календарь (разные частоты)
	newsTicker := time.NewTicker(p.cfg.PollInterval)
	calTicker := time.NewTicker(p.cfg.CalPollInterval)
	defer newsTicker.Stop()
	defer calTicker.Stop()

	// Инициирующий проход сразу
	p.FetchNewsOnce(ctx)
	p.FetchCalendarOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-newsTicker.C:
			p.FetchNewsOnce(ctx)
		case <-calTicker.C:
			p.FetchCalendarOnce(ctx)
		}
	}
}

func (p *Pipeline) FetchNewsOnce(ctx context.Context) {
	if p.cfg.RSS == nil {
		return
	}

	items, err := p.cfg.RSS.Fetch(ctx)
	if err != nil {
		p.cfg.Logger.Printf("rss fetch error: %v", err)
		p.writeHeartbeat(ctx, p.cfg.StreamNewsHB, "news", false, err.Error(), 0)
		return
	}

	added := 0
	for _, it := range items {
		if ctx.Err() != nil {
			p.cfg.Logger.Printf("shutdown: skipping %d remaining news items", len(items)-added)
			break
		}
		ok, err := p.DedupeAndXAdd(ctx, p.cfg.StreamNewsRaw, "news:dedupe:", it.UID, it.ToStreamFields())
		if err != nil {
			p.cfg.Logger.Printf("xadd error uid=%s err=%v", it.UID, err)
			continue
		}
		if ok {
			added++
		}
	}
	p.writeHeartbeat(ctx, p.cfg.StreamNewsHB, "news", true, "", added)
}

func (p *Pipeline) FetchCalendarOnce(ctx context.Context) {
	if p.cfg.Calendar == nil {
		return
	}
	events, err := p.cfg.Calendar.Fetch(ctx)
	if err != nil {
		p.cfg.Logger.Printf("calendar fetch error: %v", err)
		p.writeHeartbeat(ctx, p.cfg.StreamCalHB, "calendar", false, err.Error(), 0)
		return
	}
	added := 0
	for _, ev := range events {
		if ctx.Err() != nil {
			p.cfg.Logger.Printf("shutdown: skipping %d remaining calendar events", len(events)-added)
			break
		}
		ok, err := p.DedupeAndXAdd(ctx, p.cfg.StreamCalEvents, "cal:dedupe:", ev.UID, ev.ToStreamFields())
		if err != nil {
			p.cfg.Logger.Printf("calendar xadd error uid=%s err=%v", ev.UID, err)
			continue
		}
		if ok {
			added++
		}
	}
	p.writeHeartbeat(ctx, p.cfg.StreamCalHB, "calendar", true, "", added)
}

func (p *Pipeline) DedupeAndXAdd(ctx context.Context, stream string, dedupePrefix string, uid string, fields map[string]any) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	key := dedupePrefix + uid

	var ok bool
	var err error

	retry := func(op func() error) error {
		var lastErr error
		for i := 0; i < 5; i++ {
			lastErr = op()
			if lastErr == nil {
				return nil
			}
			if !strings.Contains(strings.ToUpper(lastErr.Error()), "LOADING") {
				return lastErr
			}
			p.cfg.Logger.Printf("redis is loading, retrying in 1s (attempt %d/5)...", i+1)
			time.Sleep(1 * time.Second)
		}
		return lastErr
	}

	err = retry(func() error {
		ok, err = p.cfg.Redis.SetNX(ctx, key, "1", p.cfg.DedupeTTL).Result()
		return err
	})
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	err = retry(func() error {
		_, err = p.cfg.Redis.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			MaxLen: p.cfg.MaxStreamLen,
			Values: fields,
		}).Result()
		return err
	})

	if err != nil {
		// rollback дедупа, чтобы повтор мог пройти
		_, _ = p.cfg.Redis.Del(ctx, key).Result()
		return false, err
	}

	return true, nil
}

func (p *Pipeline) writeHeartbeat(ctx context.Context, hbStream string, kind string, ok bool, errMsg string, added int) {
	// Heartbeat в stream + key с TTL, чтобы watchdog мог быстро проверить.
	now := NowMs()
	obj := map[string]any{
		"ts_ms": now,
		"kind":  kind,
		"ok":    ok,
		"err":   errMsg,
		"added": added,
		"instance": p.cfg.InstanceID,
	}
	raw, _ := json.Marshal(obj)

	// ключ (быстрое чтение watchdog)
	_ = p.cfg.Redis.Set(ctx, "hb:"+kind, string(raw), p.cfg.HeartbeatTTL).Err()

	// stream (для истории)
	_, _ = p.cfg.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: hbStream,
		MaxLen: 10000,
		Values: map[string]any{
			"ts_ms": itoa64(now),
			"ok":    boolToStr(ok),
			"err":   errMsg,
			"added": itoa(int64(added)),
			"instance": p.cfg.InstanceID,
		},
	}).Result()
}

func boolToStr(b bool) string {
	if b { return "1" }
	return "0"
}
