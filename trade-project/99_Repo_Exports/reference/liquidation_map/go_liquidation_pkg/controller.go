package liquidation

import (
	"context"
	"log"
	"math"
	"os"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"

	"go-worker/internal/monitoring"
	"go-worker/internal/redis"
	"go-worker/internal/wsconn"
)

// Controller отвечает за получение liquidation feeds из WS и публикацию в Redis Streams.
//
// Граница ответственности:
//   - ingestion + нормализация + базовая DQ фильтрация
//   - публикация нормализованных событий (плоский DTO) в Redis Stream
//
// НЕ делает:
//   - построение heatmap/кластеров (это Python worker)
//   - хранение истории (это Timescale/ETL)
type Controller struct {
	cfg Config

	rdb *goredis.Client

	pub  *redis.BatchStreamPublisher
	qpub *redis.BatchStreamPublisher

	logger  *log.Logger
	metrics *monitoring.LiquidationMetrics

	wg sync.WaitGroup
}

func NewController(rdb *goredis.Client, cfg Config, logger *log.Logger) *Controller {
	if logger == nil {
		logger = log.New(os.Stdout, "[liq] ", log.LstdFlags|log.Lmicroseconds)
	}
	pub := redis.NewBatchStreamPublisher(rdb, cfg.Stream, cfg.StreamMaxLen, cfg.BatchSize, cfg.FlushInterval)
	var qpub *redis.BatchStreamPublisher
	if cfg.DQ.EnableQuarantine {
		// quarantine держим короче (чтобы не забивать Redis).
		qpub = redis.NewBatchStreamPublisher(rdb, cfg.QuarantineStream, 50_000, 200, 50*time.Millisecond)
	}
	return &Controller{
		cfg:     cfg,
		rdb:     rdb,
		pub:     pub,
		qpub:    qpub,
		logger:  logger,
		metrics: monitoring.NewLiquidationMetrics(),
	}
}

func (c *Controller) Start(ctx context.Context) {
	c.pub.Start(ctx)
	if c.qpub != nil {
		c.qpub.Start(ctx)
	}

	if !c.cfg.Enabled {
		c.logger.Printf("liquidation WS disabled (LIQ_WS_ENABLED=false)")
		return
	}

	if c.cfg.BinanceEnabled {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runBinance(ctx)
		}()
	}
	if c.cfg.BybitEnabled {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runBybit(ctx)
		}()
	}
}

func (c *Controller) Stop(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}

	_ = c.pub.Close(2 * time.Second)
	if c.qpub != nil {
		_ = c.qpub.Close(2 * time.Second)
	}
}

func (c *Controller) publishNormalized(ev NormalizedEvent) bool {
	values := map[string]interface{}{
		"src":          ev.Source,
		"symbol":       ev.Symbol,
		"ts_ms":        ev.EventTsMs,
		"recv_ts_ms":   ev.RecvTsMs,
		"price":        ev.Price,
		"qty":          ev.Qty,
		"notional_usd": ev.NotionalUsd,
		"liq_side":     ev.LiqSide,
		"raw_side":     ev.RawSide,
	}
	return c.pub.Enqueue(values)
}

func (c *Controller) quarantine(ev NormalizedEvent, reason string) {
	if c.qpub == nil {
		return
	}
	values := map[string]interface{}{
		"src":          ev.Source,
		"symbol":       ev.Symbol,
		"ts_ms":        ev.EventTsMs,
		"recv_ts_ms":   ev.RecvTsMs,
		"price":        ev.Price,
		"qty":          ev.Qty,
		"notional_usd": ev.NotionalUsd,
		"liq_side":     ev.LiqSide,
		"raw_side":     ev.RawSide,
		"reason":       reason,
	}
	_ = c.qpub.Enqueue(values)
}

func (c *Controller) runBinance(ctx context.Context) {
	source := "binance_usdm"
	backoff := time.Second

	wsCfg := wsconn.DefaultConfig()
	wsCfg.ReadTimeout = 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			c.metrics.SetConnected(source, false)
			return
		default:
		}

		conn, err := wsconn.Dial(ctx, c.cfg.BinanceWSURL, wsCfg)
		if err != nil {
			c.metrics.SetConnected(source, false)
			c.logger.Printf("binance dial error: %v", err)
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(15*time.Second), float64(backoff*2)))
			continue
		}
		backoff = time.Second
		c.metrics.SetConnected(source, true)
		c.logger.Printf("binance connected: %s", c.cfg.BinanceWSURL)

		_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))
		conn.SetPongHandler(func(appData string) error {
			_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))
			return nil
		})

		for {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				c.metrics.SetConnected(source, false)
				return
			default:
			}

			_, msg, err := conn.ReadMessage()
			if err != nil {
				c.metrics.SetConnected(source, false)
				c.logger.Printf("binance read error: %v", err)
				_ = conn.Close()
				break
			}
			nowMs := time.Now().UnixMilli()
			c.metrics.IncIn(source, 1)

			ev, err := ParseBinanceForceOrder(msg, nowMs)
			if err != nil {
				c.metrics.ParseErr(source)
				continue
			}

			ok, reason := c.cfg.DQ.Validate(ev, nowMs)
			if !ok {
				c.metrics.Drop(source, reason, 1)
				// filtered_symbol и dedup — это не "плохие" события, а шум/фильтрация,
				// поэтому не отправляем их в quarantine, чтобы не раздувать stream:liq_evt:quarantine.
				if reason != "filtered_symbol" && reason != "dedup" {
					c.quarantine(ev, reason)
				}
				continue
			}
			c.metrics.ObserveLag(source, nowMs-ev.EventTsMs)

			if !c.publishNormalized(ev) {
				c.metrics.Drop(source, "queue_full", 1)
				c.quarantine(ev, "queue_full")
				continue
			}
			c.metrics.IncPublished(source, 1)
		}
	}
}

func (c *Controller) runBybit(ctx context.Context) {
	source := "bybit_linear"
	backoff := time.Second

	wsCfg := wsconn.DefaultConfig()
	wsCfg.ReadTimeout = 90 * time.Second

	topics := make([]string, 0, len(c.cfg.Symbols))
	for _, s := range c.cfg.Symbols {
		topics = append(topics, "allLiquidation."+s)
	}
	subscribeMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": topics,
	}

	for {
		select {
		case <-ctx.Done():
			c.metrics.SetConnected(source, false)
			return
		default:
		}

		conn, err := wsconn.Dial(ctx, c.cfg.BybitWSURL, wsCfg)
		if err != nil {
			c.metrics.SetConnected(source, false)
			c.logger.Printf("bybit dial error: %v", err)
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(15*time.Second), float64(backoff*2)))
			continue
		}
		backoff = time.Second
		c.metrics.SetConnected(source, true)
		c.logger.Printf("bybit connected: %s", c.cfg.BybitWSURL)

		if err := conn.WriteJSON(subscribeMsg); err != nil {
			c.logger.Printf("bybit subscribe write error: %v", err)
			_ = conn.Close()
			c.metrics.SetConnected(source, false)
			continue
		}

		// Heartbeat (op:ping) — Bybit V5 требует application-level ping.
		pingStop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(c.cfg.BybitPingPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-pingStop:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					_ = conn.WriteJSON(map[string]interface{}{"op": "ping"})
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				close(pingStop)
				_ = conn.Close()
				c.metrics.SetConnected(source, false)
				return
			default:
			}

			mt, msg, err := conn.ReadMessage()
			if err != nil {
				close(pingStop)
				c.metrics.SetConnected(source, false)
				c.logger.Printf("bybit read error: %v", err)
				_ = conn.Close()
				break
			}
			if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
				continue
			}

			nowMs := time.Now().UnixMilli()
			evs, err := ParseBybitAllLiquidation(msg, nowMs)
			if err != nil {
				c.metrics.ParseErr(source)
				continue
			}
			if len(evs) == 0 {
				continue
			}
			c.metrics.IncIn(source, len(evs))
			for _, ev := range evs {
				ok, reason := c.cfg.DQ.Validate(ev, nowMs)
				if !ok {
					c.metrics.Drop(source, reason, 1)
					// filtered_symbol и dedup — это не "плохие" события, а шум/фильтрация,
					// поэтому не отправляем их в quarantine, чтобы не раздувать stream:liq_evt:quarantine.
					if reason != "filtered_symbol" && reason != "dedup" {
						c.quarantine(ev, reason)
					}
					continue
				}
				c.metrics.ObserveLag(source, nowMs-ev.EventTsMs)

				if !c.publishNormalized(ev) {
					c.metrics.Drop(source, "queue_full", 1)
					c.quarantine(ev, "queue_full")
					continue
				}
				c.metrics.IncPublished(source, 1)
			}
			c.metrics.TickRate(source, len(evs))
		}
	}
}
