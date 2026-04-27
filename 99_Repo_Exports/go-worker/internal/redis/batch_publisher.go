package redis

import (
	"context"
	"fmt"
	"go-worker/internal/monitoring"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sync"

	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// batchEntry represents a single XADD entry queued for batch publishing.
type batchEntry struct {
	stream   string
	values   map[string]any
	fromPool bool // true → release values map back to TickMapPool after flush
}

// TickMapPool reuses map[string]any allocations across ticks.
// Reduces GC alloc pressure at 10k+ msg/sec (each tick previously allocated a fresh map).
// Maps are cleared before return so callers start with an empty map.
var TickMapPool = sync.Pool{
	New: func() any { return make(map[string]any, 16) },
}

// AcquireTickMap returns a cleared map[string]any from the pool.
// Caller MUST either pass fromPool:true when enqueueing (so flush releases it)
// OR call ReleaseTickMap explicitly if the map is not enqueued.
func AcquireTickMap() map[string]any {
	m := TickMapPool.Get().(map[string]any)
	for k := range m {
		delete(m, k)
	}
	return m
}

// ReleaseTickMap returns a map to TickMapPool.
// Only call this when NOT passing fromPool:true to enqueue.
func ReleaseTickMap(m map[string]any) {
	TickMapPool.Put(m)
}

// BatchTickPublisher publishes ticks/books using Redis Pipeline batching.
//
// Instead of spawning a new goroutine per XADD (which causes massive GC pressure
// at 10k+ msg/sec), it buffers entries in a channel and flushes them:
//   - every FlushInterval (default: 10ms), OR
//   - when the internal buffer reaches BatchSize (default: 100 entries)
//
// All flushes are done via a single pipeline.Exec() call, reducing Redis IOPS
// by ~100x under high load compared to one XADD goroutine per message.
//
// Configuration via ENV:
//
//	TICK_STREAM_MAXLEN: max entries per stream (default: streams.MaxLenPerSymbol=10000, ~MAXLEN trimming)
//
// Usage:
//
//	p := NewBatchTickPublisher(redisClient, 100, 10*time.Millisecond)
//	// Optionally start the background flusher:
//	ctx, cancel := context.WithCancel(context.Background())
//	p.Start(ctx)
//	defer p.Stop()
//
//	// In hot path — never blocks if buffer not full; drops with metric if full:
//	p.PublishTick(ctx, "BTCUSDT", values)
type BatchTickPublisher struct {
	client           *redis.Client
	batchSize        int
	flushInterval    time.Duration
	tickStreamMaxLen int64 // P3: ENV-configurable stream MAXLEN (default streams.MaxLenPerSymbol=10000)
	backpressureMs   int64 // Wait this many ms before dropping; 0 = drop immediately
	drainTimeout     time.Duration
	ch               chan batchEntry

	// Metrics
	droppedTotal      atomic.Int64
	publishedTotal    atomic.Int64
	flushErrors       atomic.Int64
	backpressureTotal atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// tickStreamMaxLenFromEnv reads TICK_STREAM_MAXLEN from environment, defaulting to 5000.
func tickStreamMaxLenFromEnv() int64 {
	v := strings.TrimSpace(os.Getenv("TICK_STREAM_MAXLEN"))
	if v == "" {
		return streams.MaxLenPerSymbol
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		zap.S().Warnf("⚠️ TICK_STREAM_MAXLEN invalid (%q), using default %d", v, streams.MaxLenPerSymbol)
		return streams.MaxLenPerSymbol
	}
	return n
}

// backpressureMsFromEnv reads BATCH_PUBLISHER_BACKPRESSURE_MS from environment.
// Default: 50ms — wait one flush cycle before dropping, preventing silent tick loss on peaks.
// Set to 0 explicitly to restore legacy drop-immediately behaviour.
func backpressureMsFromEnv() int64 {
	v := strings.TrimSpace(os.Getenv("BATCH_PUBLISHER_BACKPRESSURE_MS"))
	if v == "" {
		return 50 // default: wait 50ms (≥1 flush cycle) before dropping
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		zap.S().Warnf("⚠️ BATCH_PUBLISHER_BACKPRESSURE_MS invalid (%q), using default 50ms", v)
		return 50
	}
	return n
}

// drainTimeoutFromEnv reads DRAIN_TIMEOUT_SEC from environment.
func drainTimeoutFromEnv() time.Duration {
	v := strings.TrimSpace(os.Getenv("DRAIN_TIMEOUT_SEC"))
	if v == "" {
		return 10 * time.Second
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 10 * time.Second
	}
	return time.Duration(n) * time.Second
}

// NewBatchTickPublisher creates a BatchTickPublisher with a buffered channel.
//
// Parameters:
//   - client: Redis client to use for XADD
//   - batchSize: flush immediately when this many entries are queued (e.g. 100)
//   - flushInterval: flush at least this often even if batchSize not reached (e.g. 10ms)
//
// channelCapacity is set to 20_000 to absorb ~2s of 10k msg/sec bursts.
// P3: tickStreamMaxLen is read from ENV TICK_STREAM_MAXLEN (default streams.MaxLenPerSymbol=10000).
func NewBatchTickPublisher(client *redis.Client, batchSize int, flushInterval time.Duration) *BatchTickPublisher {
	bpMs := backpressureMsFromEnv()
	if bpMs > 0 {
		zap.S().Infof("ℹ️ BatchTickPublisher: backpressure enabled, wait %dms before drop", bpMs)
	}
	return &BatchTickPublisher{
		client:           client,
		batchSize:        batchSize,
		flushInterval:    flushInterval,
		tickStreamMaxLen: tickStreamMaxLenFromEnv(),
		backpressureMs:   bpMs,
		drainTimeout:     drainTimeoutFromEnv(),
		ch:               make(chan batchEntry, 20_000),
		stopCh:           make(chan struct{}),
	}
}

// Start launches the background flush worker. Must be called before publishing.
// The worker exits when ctx is cancelled or Stop() is called.
func (p *BatchTickPublisher) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.worker(ctx)
}

// Stop signals the worker to drain and exit. Blocks until the final flush completes.
func (p *BatchTickPublisher) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// Close закрывает прием новых тиков и ждет опустошения буферов с таймаутом.
func (p *BatchTickPublisher) Close(timeout time.Duration) error {
	// Останавливаем воркер (он сам вызовет финальный flush)
	close(p.stopCh)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		monitoring.RecordDrainTimeout()
		return fmt.Errorf("timeout waiting for BatchTickPublisher to drain")
	}
}

// PublishTick enqueues a tick for the stream:tick_<SYMBOL> stream.
// Non-blocking: if the internal channel is full, the entry is dropped and counted.
func (p *BatchTickPublisher) PublishTick(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	return p.enqueue(streams.TickStream(symbol), values, false)
}

// PublishBook enqueues a book snapshot for the stream:book_<SYMBOL> stream.
// Non-blocking: if the internal channel is full, the entry is dropped and counted.
func (p *BatchTickPublisher) PublishBook(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	return p.enqueue(streams.BookStream(symbol), values, false)
}

// PublishTickPooled is like PublishTick but takes ownership of a map from AcquireTickMap().
// The map is returned to TickMapPool after the pipeline flush, reducing GC allocs.
func (p *BatchTickPublisher) PublishTickPooled(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	return p.enqueue(streams.TickStream(symbol), values, true)
}

// PublishBookPooled is like PublishBook but takes ownership of a map from AcquireTickMap().
func (p *BatchTickPublisher) PublishBookPooled(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	return p.enqueue(streams.BookStream(symbol), values, true)
}

// enqueue adds an entry to the batch channel without blocking.
// Returns ErrBufferFull if the channel is at capacity (load shedding).
// If fromPool is true, the values map was acquired from TickMapPool and will be
// returned to the pool after the pipeline flush completes.
func (p *BatchTickPublisher) enqueue(stream string, values map[string]any, fromPool bool) error {
	if len(values) == 0 {
		if fromPool {
			ReleaseTickMap(values)
		}
		return fmt.Errorf("empty payload for %s", stream)
	}

	if _, ok := values["trace_id"]; !ok {
		values["trace_id"] = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	// Fast path: try non-blocking send.
	select {
	case p.ch <- batchEntry{stream: stream, values: values, fromPool: fromPool}:
		return nil
	default:
	}

	// Channel full — record fill ratio for observability.
	monitoring.RecordBatchPublisherChanFillRatio(float64(len(p.ch)) / float64(cap(p.ch)))

	// Backpressure: wait a short timeout before dropping, letting the flush worker drain.
	if p.backpressureMs > 0 {
		p.backpressureTotal.Add(1)
		monitoring.RecordBatchPublisherBackpressure()
		select {
		case p.ch <- batchEntry{stream: stream, values: values, fromPool: fromPool}:
			return nil
		case <-time.After(time.Duration(p.backpressureMs) * time.Millisecond):
			// Still full after wait — fall through to drop.
		}
	}

	// Load shedding: drop this entry and count it.
	var dlqValues map[string]any
	if fromPool {
		dlqValues = make(map[string]any, len(values))
		for k, v := range values {
			dlqValues[k] = v
		}
		ReleaseTickMap(values) // avoid map leak on drop
	} else {
		dlqValues = values
	}

	p.droppedTotal.Add(1)
	monitoring.RecordBatchPublisherDropped(stream) // Prometheus metric (Priority 8)

	if p.client != nil {
		go func(s string, vals map[string]any) {
			dlqCtx, cancel := context.WithTimeout(context.Background(), p.drainTimeout)
			defer cancel()
			dlqStream := streams.DLQPrefix + s
			err := p.client.XAdd(dlqCtx, &redis.XAddArgs{
				Stream: dlqStream,
				Values: vals,
				MaxLen: streams.MaxLenDLQ,
				Approx: true,
			}).Err()
			if err != nil {
				monitoring.RecordDLQWriteError(dlqStream)
				zap.S().Warnf("⚠️ DLQ write error stream=%s: %v", dlqStream, err)
			}
		}(stream, dlqValues)
	}

	if p.droppedTotal.Load()%1000 == 0 {
		zap.S().Warnf("⚠️ BatchTickPublisher: buffer full, dropped %d messages total (stream=%s)",
			p.droppedTotal.Load(), stream)
	}
	return fmt.Errorf("buffer full for %s", stream)
}

// worker reads from the channel and flushes via pipeline on tick or batch size.
func (p *BatchTickPublisher) worker(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	batch := make([]batchEntry, 0, p.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		flushCtx, cancel := context.WithTimeout(context.Background(), p.drainTimeout)
		defer cancel()

		pipe := p.client.Pipeline()
		for _, e := range batch {
			if len(e.values) == 0 {
				zap.S().Infof("CRITICAL BUG: Empty map found in batch for stream %s just before XAdd, dropping entry!", e.stream)
				continue
			}

			pipe.XAdd(flushCtx, &redis.XAddArgs{
				Stream: e.stream,
				MaxLen: p.tickStreamMaxLen, // P3: ENV-configurable (TICK_STREAM_MAXLEN, default streams.MaxLenPerSymbol=10000)
				// Approx: true = MAXLEN ~ N. Реальная длина стрима может временно
				// превышать MaxLen на ~10% при burst (Redis trim по radix-tree node).
				// Алерт на длину стрима должен иметь запас ≥ 15% над maxLen.
				// См. подробнее: internal/redis/batch_stream_publisher.go.
				Approx: true,
				ID:     "*",
				Values: e.values,
			})
		}

		if _, err := pipe.Exec(flushCtx); err != nil {
			p.flushErrors.Add(1)
			monitoring.RecordBatchPublisherError() // Prometheus metric (Priority 8)
			// Log every 100th pipeline error to avoid spam.
			if p.flushErrors.Load()%100 == 0 {
				zap.S().Errorf("❌ BatchTickPublisher: pipeline flush error (total=%d): %v",
					p.flushErrors.Load(), err)
			}
		} else {
			batchLen := int64(len(batch))
			p.publishedTotal.Add(batchLen)
			monitoring.RecordBatchPublisherPublished(batchLen) // Prometheus metric (Priority 8)
		}

		// Return pooled maps to TickMapPool before clearing the batch slice.
		for i := range batch {
			if batch[i].fromPool {
				ReleaseTickMap(batch[i].values)
				batch[i].values = nil
			}
		}

		// Reuse the slice to avoid allocations.
		batch = batch[:0]
	}

	for {
		select {
		case entry, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)

			// Try to read more items before flushing (up to batchSize).
		drainLoop:
			for len(batch) < p.batchSize {
				select {
				case entry, ok := <-p.ch:
					if !ok {
						flush()
						return
					}
					batch = append(batch, entry)
				default:
					// No more items available without blocking.
					break drainLoop
				}
			}

			// Batch size reached or channel empty right now.
			if len(batch) >= p.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-p.stopCh:
			// Drain remaining entries.
			for {
				select {
				case entry := <-p.ch:
					batch = append(batch, entry)
				default:
					flush()
					return
				}
			}

		case <-ctx.Done():
			flush()
			return
		}
	}
}

// Stats returns current publisher metrics (for logging/Prometheus integration).
func (p *BatchTickPublisher) Stats() (published, dropped, flushErrors int64) {
	return p.publishedTotal.Load(), p.droppedTotal.Load(), p.flushErrors.Load()
}

// BackpressureCount returns total backpressure events (channel-full waits).
func (p *BatchTickPublisher) BackpressureCount() int64 {
	return p.backpressureTotal.Load()
}
