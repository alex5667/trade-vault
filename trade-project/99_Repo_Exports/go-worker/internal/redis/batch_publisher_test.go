package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	goredis "github.com/redis/go-redis/v9"
)

// newTestPublisher creates a publisher with tiny channel for testing.
func newTestPublisher(batchSize int, flushInterval time.Duration) *BatchTickPublisher {
	// nil client — tests that don't flush can use this.
	p := &BatchTickPublisher{
		client:        nil,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		ch:            make(chan batchEntry, 50),
		stopCh:        make(chan struct{}),
	}
	return p
}

func TestBatchTickPublisher_EnqueueNonBlocking(t *testing.T) {
	p := newTestPublisher(100, 10*time.Millisecond)

	// Should never block on enqueue.
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := p.PublishTick(context.Background(), "BTCUSDT", map[string]any{"price": "50000"})
		assert.NoError(t, err)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(100 * time.Millisecond):
		t.Fatal("PublishTick blocked unexpectedly")
	}

	assert.Equal(t, 1, len(p.ch))
}

func TestBatchTickPublisher_LoadShedding(t *testing.T) {
	// Channel capacity = 50 (from newTestPublisher).
	p := newTestPublisher(100, 10*time.Millisecond)

	payload := map[string]any{"price": "1"}
	// Fill channel to capacity.
	for i := 0; i < 50; i++ {
		_ = p.PublishTick(context.Background(), "ETHUSDT", payload)
	}
	require.Equal(t, int64(0), p.droppedTotal.Load(), "no drops yet")

	// Next enqueue must drop (not block).
	done := make(chan bool, 1)
	go func() {
		_ = p.PublishTick(context.Background(), "ETHUSDT", payload)
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("load shedding blocked — expected immediate drop")
	}
	assert.Equal(t, int64(1), p.droppedTotal.Load())
}

func TestBatchTickPublisher_EmptyPayloadReturnsError(t *testing.T) {
	p := newTestPublisher(10, 10*time.Millisecond)
	err := p.PublishTick(context.Background(), "BTCUSDT", map[string]any{})
	assert.Error(t, err, "empty payload should return error")
}

func TestBatchTickPublisher_StatsZeroOnStart(t *testing.T) {
	p := newTestPublisher(10, 10*time.Millisecond)
	pub, dropped, errs := p.Stats()
	assert.Equal(t, int64(0), pub)
	assert.Equal(t, int64(0), dropped)
	assert.Equal(t, int64(0), errs)
}

func TestBatchTickPublisher_FlushWithRealRedis(t *testing.T) {
	t.Skip("requires live Redis — run manually with REDIS_URL set")

	redisURL := "redis://localhost:6379/0"
	opts, err := goredis.ParseURL(redisURL)
	require.NoError(t, err)
	client := goredis.NewClient(opts)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	p := NewBatchTickPublisher(client, 10, 50*time.Millisecond)
	p.Start(ctx)

	for i := 0; i < 5; i++ {
		_ = p.PublishTick(ctx, "BTCUSDT", map[string]any{"price": "50000", "i": i})
	}

	time.Sleep(200 * time.Millisecond) // wait for flush

	pub, dropped, flushErrs := p.Stats()
	assert.Equal(t, int64(5), pub, "all 5 messages should be published")
	assert.Equal(t, int64(0), dropped)
	assert.Equal(t, int64(0), flushErrs)
}

func TestBatchTickPublisher_BackpressureMode(t *testing.T) {
	// Create publisher with small channel and explicit backpressure.
	p := &BatchTickPublisher{
		client:         nil,
		batchSize:      100,
		flushInterval:  10 * time.Millisecond,
		backpressureMs: 20, // wait 20ms before drop
		ch:             make(chan batchEntry, 5),
		stopCh:         make(chan struct{}),
	}

	payload := map[string]any{"price": "1"}

	// Fill channel to capacity.
	for i := 0; i < 5; i++ {
		_ = p.PublishTick(context.Background(), "BTCUSDT", payload)
	}
	require.Equal(t, int64(0), p.droppedTotal.Load(), "no drops yet")
	require.Equal(t, int64(0), p.backpressureTotal.Load(), "no backpressure yet")

	// Next enqueue should trigger backpressure wait, then drop.
	start := time.Now()
	done := make(chan bool, 1)
	go func() {
		_ = p.PublishTick(context.Background(), "BTCUSDT", payload)
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("backpressure should complete within timeout")
	}

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(15), "should have waited >= ~20ms (backpressure)")
	assert.Equal(t, int64(1), p.droppedTotal.Load(), "should have dropped after backpressure wait")
	assert.Equal(t, int64(1), p.backpressureTotal.Load(), "should have recorded 1 backpressure event")
}

func TestBatchTickPublisher_BackpressureCountZeroByDefault(t *testing.T) {
	p := newTestPublisher(10, 10*time.Millisecond)
	assert.Equal(t, int64(0), p.BackpressureCount())
}

func BenchmarkBatchTickPublisher_Enqueue(b *testing.B) {
	p := newTestPublisher(1000, 10*time.Millisecond)
	ctx := context.Background()
	payload := map[string]any{"price": "50000", "qty": "1.0"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.PublishTick(ctx, "BTCUSDT", payload)
		}
	})
}
