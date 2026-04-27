package liquidation

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	goredis "github.com/redis/go-redis/v9"

	"go-worker/internal/monitoring"
	"go-worker/internal/redis"
	"go-worker/internal/streams"
	"go-worker/internal/wsconn"

	"go.uber.org/zap"
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

	logger  *zap.SugaredLogger
	metrics *monitoring.LiquidationMetrics

	wg sync.WaitGroup
}

func NewController(rdb *goredis.Client, cfg Config, logger *zap.SugaredLogger) *Controller {
	if logger == nil {
		logger = zap.S()
	}
	pub := redis.NewBatchStreamPublisher(rdb, cfg.Stream, cfg.StreamMaxLen, cfg.BatchSize, cfg.FlushInterval)
	var qpub *redis.BatchStreamPublisher
	if cfg.DQ != nil && cfg.DQ.EnableQuarantine {
		// quarantine держим короче (чтобы не забивать Redis).
		qpub = redis.NewBatchStreamPublisher(rdb, cfg.QuarantineStream, streams.MaxLenDLQ, 200, 50*time.Millisecond)
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
		c.logger.Infof("liquidation WS disabled (LIQ_WS_ENABLED=false)")
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
	eventID := ev.Source + ":" + ev.Symbol + ":" + strconv.FormatInt(ev.EventTsMs, 10)
	values := map[string]interface{}{
		"src":            ev.Source,
		"venue":          ev.Source,
		"symbol":         ev.Symbol,
		"ts_ms":          ev.EventTsMs,
		"ts_event_ms":    ev.EventTsMs,
		"recv_ts_ms":     ev.RecvTsMs,
		"ts_ingest_ms":   ev.RecvTsMs,
		"price":          ev.Price,
		"qty":            ev.Qty,
		"notional_usd":   ev.NotionalUsd,
		"liq_side":       ev.LiqSide,
		"order_side":     ev.RawSide,
		"raw_side":       ev.RawSide,
		"schema_version": "1",
		// ── CLAUDE.md contract fields ──────────────────────────────────────
		"event_time_ms":  ev.EventTsMs,
		"ingest_time_ms": ev.RecvTsMs,
		"event_id":       eventID,
		"trace_id":       eventID,
		"quality_flags":  "ok",
	}
	return c.pub.Enqueue(values) == nil
}

func (c *Controller) quarantine(ev NormalizedEvent, reason string) {
	if c.qpub == nil {
		return
	}
	values := map[string]interface{}{
		"src":            ev.Source,
		"venue":          ev.Source,
		"symbol":         ev.Symbol,
		"ts_ms":          ev.EventTsMs,
		"ts_event_ms":    ev.EventTsMs,
		"recv_ts_ms":     ev.RecvTsMs,
		"ts_ingest_ms":   ev.RecvTsMs,
		"price":          ev.Price,
		"qty":            ev.Qty,
		"notional_usd":   ev.NotionalUsd,
		"liq_side":       ev.LiqSide,
		"order_side":     ev.RawSide,
		"raw_side":       ev.RawSide,
		"reason":         reason,
		"schema_version": "1",
	}
	_ = c.qpub.Enqueue(values)
	// Счётчик только для событий, реально попавших в quarantine stream.
	// Используется для расчёта quarantine rate → алерт LiqQuarantineRateHigh.
	c.metrics.IncQuarantined(ev.Source, reason, 1)
	// Агрегированная метрика для простого алерта rate(liq_events_total{status="quarantined"})
	monitoring.LiqEventsTotal.WithLabelValues(ev.Source, "quarantined").Inc()
}

func (c *Controller) runBinance(ctx context.Context) {
	source := "binance_usdm"
	backoff := time.Second

	wsCfg := wsconn.DefaultConfig()
	wsCfg.ReadTimeout = 300 * time.Second

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
			c.logger.Errorf("binance dial error: %v", err)
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(15*time.Second), float64(backoff*2)))
			continue
		}
		backoff = time.Second
		c.metrics.SetConnected(source, true)
		c.logger.Infof("binance connected: %s", c.cfg.BinanceWSURL)

		var writeMu sync.Mutex

		conn.SetPingHandler(func(appData string) error {
			_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))
			writeMu.Lock()
			defer writeMu.Unlock()
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(wsCfg.WriteWait))
		})

		conn.SetPongHandler(func(appData string) error {
			_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))
			return nil
		})

		shouldReturn := c.runBinanceSession(ctx, conn, &writeMu, source, wsCfg)
		if shouldReturn {
			return
		}
	}
}

// runBinanceSession handles one established Binance WebSocket connection.
// Returns true if the parent loop should exit (ctx cancelled), false to reconnect.
func (c *Controller) runBinanceSession(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, source string, wsCfg wsconn.Config) bool {
	pingDone := make(chan struct{})
	defer close(pingDone)

	go func() {
		// Ping period is ENV-driven via LIQ_BINANCE_PING_MS (default 20s).
		// Do NOT hardcode 20*time.Second here — use cfg.BinancePingPeriod.
		pingTicker := time.NewTicker(c.cfg.BinancePingPeriod)
		defer pingTicker.Stop()

		for {
			select {
			case <-pingDone:
				return
			case <-ctx.Done():
				return
			case <-pingTicker.C:
				writeMu.Lock()
				err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(wsCfg.WriteWait))
				writeMu.Unlock()

				if err != nil {
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = conn.Close()
			c.metrics.SetConnected(source, false)
			return true // signal parent to exit
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))

		_, msg, err := conn.ReadMessage()
		if err != nil {
			c.metrics.SetConnected(source, false)
			if wsconn.IsTimeout(err) {
				c.logger.Infof("binance read idle timeout (%T %v), reconnecting...", err, err)
			} else {
				c.logger.Errorf("binance read error: %T %v", err, err)
			}
			_ = conn.Close()
			return false // signal parent to reconnect
		}

		nowMs := time.Now().UnixMilli()
		c.metrics.IncIn(source, 1)
		monitoring.LiqEventsTotal.WithLabelValues(source, "received").Inc()

		ev, err := ParseBinanceForceOrder(msg, nowMs)
		if err != nil {
			c.metrics.ParseErr(source)
			continue
		}

		ok, reason := c.cfg.DQ.Validate(ev, nowMs)
		if !ok {
			c.metrics.Drop(source, reason, 1)
			monitoring.LiqEventsTotal.WithLabelValues(source, "dropped").Inc()
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
			monitoring.LiqEventsTotal.WithLabelValues(source, "dropped").Inc()
			c.quarantine(ev, "queue_full")
			continue
		}
		c.metrics.IncPublished(source, 1)
		monitoring.LiqEventsTotal.WithLabelValues(source, "published").Inc()
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
			c.logger.Errorf("bybit dial error: %v", err)
			time.Sleep(backoff)
			backoff = time.Duration(math.Min(float64(15*time.Second), float64(backoff*2)))
			continue
		}
		backoff = time.Second
		c.metrics.SetConnected(source, true)
		c.logger.Infof("bybit connected: %s", c.cfg.BybitWSURL)

		if err := conn.WriteJSON(subscribeMsg); err != nil {
			c.logger.Errorf("bybit subscribe write error: %v", err)
			_ = conn.Close()
			c.metrics.SetConnected(source, false)
			continue
		}

		// runBybitSession encapsulates the read-loop for one connection.
		// It returns when the connection breaks or ctx is cancelled.
		// pingStop is scoped to this connection — no leak across reconnects.
		shouldReturn := c.runBybitSession(ctx, conn, source, wsCfg)
		if shouldReturn {
			return
		}
	}
}

// runBybitSession handles one established Bybit WebSocket connection.
// Returns true if the parent loop should exit (ctx cancelled), false to reconnect.
func (c *Controller) runBybitSession(ctx context.Context, conn *websocket.Conn, source string, wsCfg wsconn.Config) bool {
	// pingStop is created fresh for each connection and closed exactly once
	// via defer when this function returns (on any exit path).
	pingStop := make(chan struct{})
	defer close(pingStop)

	// Heartbeat (op:ping) — Bybit V5 requires application-level ping.
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
			_ = conn.Close()
			c.metrics.SetConnected(source, false)
			return true // signal parent to exit
		default:
		}

		// Refresh deadline for Bybit ReadLoop
		_ = conn.SetReadDeadline(time.Now().Add(wsCfg.ReadTimeout))

		mt, msg, err := conn.ReadMessage()
		if err != nil {
			c.metrics.SetConnected(source, false)
			c.logger.Errorf("bybit read error: %v", err)
			_ = conn.Close()
			return false // signal parent to reconnect
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
		monitoring.LiqEventsTotal.WithLabelValues(source, "received").Add(float64(len(evs)))
		for _, ev := range evs {
			ok, reason := c.cfg.DQ.Validate(ev, nowMs)
			if !ok {
				c.metrics.Drop(source, reason, 1)
				monitoring.LiqEventsTotal.WithLabelValues(source, "dropped").Inc()
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
				monitoring.LiqEventsTotal.WithLabelValues(source, "dropped").Inc()
				c.quarantine(ev, "queue_full")
				continue
			}
			c.metrics.IncPublished(source, 1)
			monitoring.LiqEventsTotal.WithLabelValues(source, "published").Inc()
		}
		c.metrics.TickRate(source, len(evs))
	}
}
