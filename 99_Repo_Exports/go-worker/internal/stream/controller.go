package stream

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-worker/internal/metrics"
	"go-worker/internal/models"
	"go-worker/internal/monitoring"
	"go-worker/internal/orderflow"
	internalredis "go-worker/internal/redis"
	"go-worker/internal/streams"
	"go-worker/internal/wsconn"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// CrossAssetHook is an optional interface that the crossasset.Tracker satisfies.
// Injecting it keeps the controller decoupled from the crossasset package.
type CrossAssetHook interface {
	OnTick(ctx context.Context, symbol string, price float64, tsMs int64)
	OnBook(ctx context.Context, symbol string, bestBidPx float64, tsMs int64)
}

// ExchangeManager interfaces basic operations to run an exchange-specific WebSocket.
type ExchangeManager interface {
	Connect(ctx context.Context) error
	// ReadLoop reads from connection and delegates raw payloads and symbol hints to handler.
	ReadLoop(ctx context.Context, handler func(symbol string, msg []byte)) error
	UpdateSubscriptions(addItems, removeItems []string) error
	Close()
}

// MessageNormalizer takes a raw WS payload and parses it into ticks and orderbooks.
type MessageNormalizer interface {
	Normalize(symbol string, payload []byte) (ticks []models.NormalizedTick, books []models.NormalizedDepth, err error)
}

// Controller is a generic loop runner for Futures streaming on Binance, Bybit, etc.
type Controller struct {
	exchangeName    string
	client          *redis.Client
	publisher       internalredis.Publisher
	logger          *zap.SugaredLogger
	symbolsKey      string
	refreshInterval time.Duration
	healthMetrics   *metrics.HealthMetrics
	stalenessConfig orderflow.StalenessConfig

	mu                      sync.Mutex
	currentSymbols          []string
	activeManagerCancel     context.CancelFunc
	futuresStreamStartCount uint64
	activeManager           ExchangeManager
	normalizer              MessageNormalizer
	errorCount              int64
	workerChans             []chan msgEnvelope
	workerWG                sync.WaitGroup

	// crossAssetHook is an optional v12_of metrics enricher (fail-open if nil).
	crossAssetHook CrossAssetHook

	// Function to spawn a new implementation of `ExchangeManager`
	newManagerFactory func(symbols []string, logger *zap.SugaredLogger) ExchangeManager

	// Pre-Processor for loading dynamic Symbols from Env overrides or formatting requirements
	symbolsLimitsFn func(symbols []string) []string
}

type msgEnvelope struct {
	symbol  string
	payload []byte
}

// NewController creates a generic WebSocket streams controller.
func NewController(
	exchangeName string,
	client *redis.Client,
	publisher internalredis.Publisher,
	logger *zap.SugaredLogger,
	symbolsKey string,
	refreshInterval time.Duration,
	healthMetrics *metrics.HealthMetrics,
	stalenessConfig orderflow.StalenessConfig,
	newManagerFactory func(symbols []string, logger *zap.SugaredLogger) ExchangeManager,
	normalizer MessageNormalizer,
	symbolsLimitsFn func(symbols []string) []string,
) *Controller {
	if symbolsLimitsFn == nil {
		symbolsLimitsFn = func(s []string) []string { return s }
	}

	return &Controller{
		exchangeName:      exchangeName,
		client:            client,
		publisher:         publisher,
		logger:            logger,
		symbolsKey:        symbolsKey,
		refreshInterval:   refreshInterval,
		healthMetrics:     healthMetrics,
		stalenessConfig:   stalenessConfig,
		newManagerFactory: newManagerFactory,
		normalizer:        normalizer,
		symbolsLimitsFn:   symbolsLimitsFn,
	}
}

// WithCrossAssetHook attaches a v12_of metrics enricher to the controller.
// Can be called after NewController, before Run.
func (c *Controller) WithCrossAssetHook(hook CrossAssetHook) *Controller {
	c.crossAssetHook = hook
	return c
}

// Run loops infinitely loading symbols, reconciling changes, and running the Manager.
func (c *Controller) Run(ctx context.Context, baseSymbols []string) {
	c.logger.Infof("%s futures controller стартует. Redis key: %s", strings.ToUpper(c.exchangeName), c.symbolsKey)

	// Initialize worker pool (configurable via ENV)
	workerCountStr := os.Getenv("GO_WORKER_COUNT")
	workerCount := 16
	if v, err := strconv.Atoi(workerCountStr); err == nil && v > 0 {
		workerCount = v
	}

	bufSizeStr := os.Getenv("GO_WORKER_BUFFER")
	bufSize := 2000
	if v, err := strconv.Atoi(bufSizeStr); err == nil && v > 0 {
		bufSize = v
	}

	c.workerChans = make([]chan msgEnvelope, workerCount)
	for i := 0; i < workerCount; i++ {
		c.workerChans[i] = make(chan msgEnvelope, bufSize)
		c.workerWG.Add(1)
		go c.workerLoop(ctx, c.workerChans[i])
	}
	defer func() {
		for _, ch := range c.workerChans {
			close(ch)
		}
		c.workerWG.Wait()
	}()

	desired := c.loadSymbols(ctx, baseSymbols)
	desired = c.symbolsLimitsFn(desired)
	c.reconcile(ctx, desired)

	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("%s futures controller остановка по контексту", strings.ToUpper(c.exchangeName))
			c.Stop()
			return
		case <-ticker.C:
			desired = c.loadSymbols(ctx, baseSymbols)
			desired = c.symbolsLimitsFn(desired)
			c.reconcile(ctx, desired)
		}
	}
}

// Stop closes active manager connections.
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeManagerCancel != nil {
		c.activeManagerCancel()
		c.activeManagerCancel = nil
	}
}

func (c *Controller) loadSymbols(ctx context.Context, base []string) []string {
	symbolSet := make(map[string]struct{}, len(base))
	for _, s := range base {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			symbolSet[s] = struct{}{}
		}
	}

	if c.client != nil && c.symbolsKey != "" {
		symbols, err := c.client.SMembers(ctx, c.symbolsKey).Result()
		if err != nil && err != redis.Nil {
			c.logger.Errorf("Ошибка чтения Redis ключа %s: %v", c.symbolsKey, err)
		} else {
			for _, s := range symbols {
				s = strings.ToUpper(strings.TrimSpace(s))
				if s != "" {
					symbolSet[s] = struct{}{}
				}
			}
		}
	}

	result := make([]string, 0, len(symbolSet))
	for s := range symbolSet {
		result = append(result, s)
	}
	return result
}

func (c *Controller) reconcile(ctx context.Context, symbols []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Сравниваем
	if len(symbols) == len(c.currentSymbols) {
		match := true
		currSet := make(map[string]struct{})
		for _, s := range c.currentSymbols {
			currSet[s] = struct{}{}
		}
		for _, s := range symbols {
			if _, ok := currSet[strings.ToUpper(s)]; !ok {
				match = false
				break
			}
		}
		if match {
			return
		}
	}

	currSet := make(map[string]struct{})
	for _, s := range c.currentSymbols {
		currSet[s] = struct{}{}
	}
	newSet := make(map[string]struct{})
	for _, s := range symbols {
		newSet[strings.ToUpper(s)] = struct{}{}
	}

	var toAdd, toRemove []string
	for s := range newSet {
		if _, ok := currSet[s]; !ok {
			toAdd = append(toAdd, s)
		}
	}
	for s := range currSet {
		if _, ok := newSet[s]; !ok {
			toRemove = append(toRemove, s)
		}
	}

	// Try Live Update
	if c.activeManager != nil {
		c.logger.Infof("%s Live Update: Add %d, Remove %d symbols", strings.ToUpper(c.exchangeName), len(toAdd), len(toRemove))
		err := c.activeManager.UpdateSubscriptions(toAdd, toRemove)
		if err == nil {
			c.currentSymbols = make([]string, len(symbols))
			for i, s := range symbols {
				c.currentSymbols[i] = strings.ToUpper(s)
			}
			return
		}
		c.logger.Errorf("⚠️ %s live update failed, falling back to reconnect: %v", strings.ToUpper(c.exchangeName), err)
	}

	c.logger.Infof("%s futures controller: список символов изменился (%d -> %d), перезапуск", strings.ToUpper(c.exchangeName), len(c.currentSymbols), len(symbols))

	if c.activeManagerCancel != nil {
		c.activeManagerCancel()
	}

	c.currentSymbols = make([]string, len(symbols))
	for i, s := range symbols {
		c.currentSymbols[i] = strings.ToUpper(s)
	}

	runCtx, cancel := context.WithCancel(ctx)
	c.activeManagerCancel = cancel

	go c.runMultiplexed(runCtx, c.currentSymbols)
}

func (c *Controller) runMultiplexed(ctx context.Context, symbols []string) {
	if len(symbols) == 0 {
		return
	}

	backoff := wsconn.DefaultReconnectBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		manager := c.newManagerFactory(symbols, c.logger)
		c.mu.Lock()
		c.activeManager = manager
		c.mu.Unlock()

		if err := manager.Connect(ctx); err != nil {
			c.logger.Infof("Ошибка %s WS подключения: %v", strings.ToUpper(c.exchangeName), err)
			monitoring.RecordFuturesReconnectUnified(c.exchangeName, "connect_error")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = wsconn.NextBackoff(backoff)
			continue
		}

		backoff = wsconn.DefaultReconnectBackoff
		c.logger.Infof("✅ %s WebSocket соединение установлено для %d символов", strings.ToUpper(c.exchangeName), len(symbols))

		err := manager.ReadLoop(ctx, func(symbol string, msg []byte) {
			c.dispatchMessage(symbol, msg)
		})

		if err != nil {
			if err == context.Canceled {
				return
			}
			currentErrorCount := atomic.AddInt64(&c.errorCount, 1)

			isConnectionReset := wsconn.IsConnectionReset(err)
			isTimeout := wsconn.IsTimeout(err)
			isContextCancelled := wsconn.IsContextError(err)

			monitoring.RecordFuturesReconnectUnified(c.exchangeName, "read_error")

			if isContextCancelled {
				return
			}

			if isConnectionReset || isTimeout {
				if currentErrorCount <= 5 || currentErrorCount%1000 == 0 {
					errorType := "connection reset"
					if isTimeout {
						errorType = "timeout"
					}
					c.logger.Warnf("⚠️ %s ReadLoop завершён (%s, попытка переподключения, всего ошибок: %d): %v",
						strings.ToUpper(c.exchangeName), errorType, currentErrorCount, err)
				}
			} else {
				if currentErrorCount <= 5 || currentErrorCount%100 == 0 {
					c.logger.Warnf("⚠️ %s ReadLoop завершён (попытка переподключения, всего ошибок: %d): %v",
						strings.ToUpper(c.exchangeName), currentErrorCount, err)
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = wsconn.NextBackoff(backoff)
			continue
		}
	}
}

func (c *Controller) dispatchMessage(symbol string, payload []byte) {
	if len(c.workerChans) == 0 {
		return
	}
	// Shard by symbol to preserve order
	id := c.getWorkerID(symbol)
	select {
	case c.workerChans[id] <- msgEnvelope{symbol: symbol, payload: payload}:
	default:
		if time.Now().Unix()%10 == 0 { // sampling to avoid flooding
			c.logger.Warnf("⚠️ %s: Queue FULL for %s, message dropped (id=%d)", c.exchangeName, symbol, id)
		}
		monitoring.RecordFuturesMessageDropped(c.exchangeName + "_worker_full")
	}
}

func (c *Controller) getWorkerID(symbol string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(symbol); i++ {
		h *= 16777619
		h ^= uint32(symbol[i])
	}
	return int(h % uint32(len(c.workerChans)))
}

func (c *Controller) workerLoop(ctx context.Context, ch chan msgEnvelope) {
	defer c.workerWG.Done()
	for msg := range ch {
		c.processMessage(ctx, msg.symbol, msg.payload, c.logger)
	}
}

func (c *Controller) processMessage(ctx context.Context, symbol string, payload []byte, logger *zap.SugaredLogger) {
	// ── Latency audit: track full processMessage duration ──
	pmStart := time.Now()
	msgType := "mixed" // overridden below if pure tick or pure book
	defer func() {
		dur := float64(time.Since(pmStart).Microseconds()) / 1000.0
		monitoring.RecordProcessMessageDuration(c.exchangeName, msgType, dur)
	}()

	ticks, books, err := c.normalizer.Normalize(symbol, payload)
	if err != nil {
		logger.Infof("Ошибка нормализации %s (%s): %v", strings.ToUpper(c.exchangeName), symbol, err)
		monitoring.RecordFuturesDecodeError(c.exchangeName, symbol)
		return
	}

	// Determine msgType for histogram label (low-cardinality)
	if len(ticks) > 0 && len(books) == 0 {
		msgType = "tick"
	} else if len(books) > 0 && len(ticks) == 0 {
		msgType = "book"
	}

	nowMs := time.Now().UnixMilli()

	for _, tick := range ticks {
		monitoring.RecordFuturesMessageUnified(c.exchangeName, tick.Symbol, "trades")

		if c.healthMetrics != nil {
			orderflowCtx := orderflow.NewOrderflowCtx(tick.Symbol, tick.Ts, nowMs)
			orderflowCtx.ComputeStaleness(c.stalenessConfig)
			c.healthMetrics.OnTick(metrics.TickMetricsInput{
				Symbol:       tick.Symbol,
				L2AgeMs:      orderflowCtx.L2AgeMsNow,
				L2AgeMsTick:  orderflowCtx.L2AgeMsTick,
				L2IsStale:    orderflowCtx.L2IsStale,
				L2IsStaleNow: orderflowCtx.L2IsStaleNow,
			})
		}

		// v12_of: notify cross-asset tracker (fail-open if hook is nil or price invalid).
		if c.crossAssetHook != nil {
			if px, e := strconv.ParseFloat(tick.Price, 64); e == nil && px > 0 {
				c.crossAssetHook.OnTick(ctx, tick.Symbol, px, tick.Ts)
			}
		}

		vals := internalredis.AcquireTickMap()
		tick.PopulateRedisValues(vals)
		xaddStart := time.Now()
		err := c.publisher.PublishTickPooled(ctx, tick.Symbol, vals)

		if err != nil {
			if err == context.Canceled {
				return
			}
			logger.Infof("Ошибка публикации тика %s: %v", strings.ToUpper(c.exchangeName), err)
		}
		monitoring.RecordRedisXaddDuration(c.exchangeName, "tick", float64(time.Since(xaddStart).Microseconds())/1000.0)
	}

	for _, book := range books {
		monitoring.RecordFuturesMessageUnified(c.exchangeName, book.Symbol, "l2Book")

		if c.healthMetrics != nil {
			orderflowCtx := orderflow.NewOrderflowCtx(book.Symbol, nowMs, book.Ts)
			orderflowCtx.ComputeStaleness(c.stalenessConfig)
			c.healthMetrics.OnTick(metrics.TickMetricsInput{
				Symbol:       book.Symbol,
				L2AgeMs:      orderflowCtx.L2AgeMsNow,
				L2AgeMsTick:  orderflowCtx.L2AgeMsTick,
				L2IsStale:    orderflowCtx.L2IsStale,
				L2IsStaleNow: orderflowCtx.L2IsStaleNow,
			})
		}

		// ── Gap guard: sequence gap detected by normaliser ────────────────
		// The local book was already flushed inside ApplyUpdate.  We must NOT
		// publish the stale/empty book to the main stream.  Instead:
		//   1. Write a diagnostic event to dlq:book_deltas.
		//   2. Increment the Prometheus counter.
		//   3. Log a warning (sampled).
		// The next full snapshot from Bybit will transparently restore state.
		if book.GapDetected {
			monitoring.RecordBybitBookDeltaGap(book.Symbol)
			logger.Warnf("⚠️ bybit book delta GAP symbol=%s exchange=%s expected_id=%d actual_id=%d ts=%d — flushed, awaiting resnapshot",
				book.Symbol, c.exchangeName, book.GapExpected, book.GapActual, book.Ts)

			if c.client != nil {
				dlqCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				err := c.client.XAdd(dlqCtx, &redis.XAddArgs{
					Stream: streams.DLQBookDeltas,
					MaxLen: streams.MaxLenDLQ,
					Approx: true,
					Values: map[string]any{
						"symbol":         book.Symbol,
						"exchange":       c.exchangeName,
						"gap_expected":   book.GapExpected,
						"gap_actual":     book.GapActual,
						"prev_final_id":  book.PrevFinal,
						"ts":             book.Ts,
						"ingest_time_ms": nowMs,
						"seq":            book.Seq,
					},
				}).Err()
				cancel()
				if err != nil {
					monitoring.RecordDLQWriteError(streams.DLQBookDeltas)
					logger.Warnf("⚠️ dlq:book_deltas write error symbol=%s: %v", book.Symbol, err)
				}
			}
			// Do NOT publish to the main book stream — the book is stale.
			continue
		}

		// v12_of: notify cross-asset tracker with best-bid price from L2 snapshot (fail-open).
		if c.crossAssetHook != nil {
			if bestBid := extractBestBid(book); bestBid > 0 {
				c.crossAssetHook.OnBook(ctx, book.Symbol, bestBid, book.Ts)
			}
		}

		vals := internalredis.AcquireTickMap()
		book.PopulateRedisValues(vals)
		xaddStart := time.Now()
		err := c.publisher.PublishBookPooled(ctx, book.Symbol, vals)

		if err != nil {
			if err == context.Canceled {
				return
			}
			logger.Infof("Ошибка публикации книги %s: %v", strings.ToUpper(c.exchangeName), err)
		}
		monitoring.RecordRedisXaddDuration(c.exchangeName, "book", float64(time.Since(xaddStart).Microseconds())/1000.0)
	}

}

// extractBestBid returns the best (highest) bid price from a NormalizedDepth snapshot.
// Returns 0 if unavailable.
func extractBestBid(book models.NormalizedDepth) float64 {
	// Bids are sorted descending by price (best first) per Binance/Bybit convention.
	if len(book.Bids) == 0 || len(book.Bids[0]) == 0 {
		return 0
	}
	px, err := strconv.ParseFloat(book.Bids[0][0], 64)
	if err != nil {
		return 0
	}
	return px
}
