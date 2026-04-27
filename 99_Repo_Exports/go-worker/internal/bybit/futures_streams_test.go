package bybit

import (
	"context"
	"sort"
	"testing"
	"time"

	"go-worker/internal/orderflow"
	"go-worker/internal/stream"

	"go.uber.org/zap"
)

// noopPublisher satisfies internalredis.Publisher interface for tests.
type noopPublisher struct {
	ticks []string
	books []string
}

func (p *noopPublisher) PublishTick(_ context.Context, symbol string, _ map[string]any) error {
	p.ticks = append(p.ticks, symbol)
	return nil
}

func (p *noopPublisher) PublishBook(_ context.Context, symbol string, _ map[string]any) error {
	p.books = append(p.books, symbol)
	return nil
}

func (p *noopPublisher) PublishTickPooled(_ context.Context, symbol string, _ map[string]any) error {
	p.ticks = append(p.ticks, symbol)
	return nil
}

func (p *noopPublisher) PublishBookPooled(_ context.Context, symbol string, _ map[string]any) error {
	p.books = append(p.books, symbol)
	return nil
}

func newTestController() *stream.Controller {
	logger := zap.S()
	c := stream.NewController(
		"bybit",
		nil,
		&noopPublisher{},
		logger,
		"test_key",
		time.Second,
		nil,
		orderflow.StalenessConfig{},
		func(symbols []string, logger *zap.SugaredLogger) stream.ExchangeManager {
			return NewFuturesMultiplexManager(symbols, logger, 50, 1)
		},
		NewNormalizer(50),
		nil,
	)
	return c
}

func sortedStrings(ss []string) []string {
	out := make([]string, len(ss))
	copy(out, ss)
	sort.Strings(out)
	return out
}

func TestReconcile_NoOpOnSameSymbols(t *testing.T) {
	c := newTestController()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial run
	go c.Run(ctx, []string{"BTCUSDT", "ETHUSDT"})
	defer c.Stop()
}
