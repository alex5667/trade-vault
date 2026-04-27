package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// BenchmarkBatchTickPublisher_Enqueue measures the hot path of enqueuing
// tick data, assuring allocation bounds and concurrency safety.
func BenchmarkBatchTickPublisher_Enqueue_Parallel(b *testing.B) {
	p := &BatchTickPublisher{
		client:        nil,
		batchSize:     1000,
		flushInterval: 10 * time.Millisecond,
		ch:            make(chan batchEntry, 5000),
		stopCh:        make(chan struct{}),
	}
	ctx := context.Background()
	payload := map[string]any{"price": "50000", "qty": "1.0", "ts": "123456789000"}

	// To consume from the channel so it doesn't block.
	go func() {
		for {
			select {
			case <-p.ch:
			case <-p.stopCh:
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.PublishTick(ctx, "BTCUSDT", payload)
		}
	})

	close(p.stopCh)
}

// BenchmarkBatchStreamPublisher_Enqueue measures the slice-based enqueue hot path
// used by the stream publisher (e.g. for books/liquidations), testing lock contention.
func BenchmarkBatchStreamPublisher_Enqueue_Parallel(b *testing.B) {
	// Initialize struct similar to NewBatchStreamPublisher without running Start() flush loop.
	p := &BatchStreamPublisher{
		client:        nil,
		stream:        "stream:test",
		maxLenApprox:  50000,
		maxBatch:      1000,
		flushInterval: 10 * time.Millisecond,
		buffer:        make([]goredis.XAddArgs, 0, 1000),
		maxBuffer:     50000,
		triggerCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}

	// mock consumer for the trigger channel to prevent block
	go func() {
		for {
			select {
			case <-p.triggerCh:
				// When maxBatch hits, we clear the buffer under lock just like a real flush
				p.mu.Lock()
				p.buffer = p.buffer[:0]
				p.mu.Unlock()
			case <-p.stopCh:
				return
			}
		}
	}()

	payload := map[string]interface{}{"price": "50000", "qty": "1.0", "ts": "123456789000"}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Enqueue(payload)
		}
	})

	close(p.stopCh)
}
