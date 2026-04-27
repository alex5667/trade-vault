package hyperliquid

import (
	"context"
	"sort"
	"testing"
	"time"

	"go-worker/internal/orderflow"
	"go-worker/internal/stream"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

// noopPublisher satisfies internalredis.Publisher interface for testing.
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

func (p *noopPublisher) PublishTickPooled(ctx context.Context, symbol string, values map[string]any) error {
	return p.PublishTick(ctx, symbol, values)
}

func (p *noopPublisher) PublishBookPooled(ctx context.Context, symbol string, values map[string]any) error {
	return p.PublishBook(ctx, symbol, values)
}

func newTestController() *stream.Controller {
	logger := zap.S()
	c := stream.NewController(
		"hyperliquid",
		nil,
		&noopPublisher{},
		logger,
		"test_key",
		time.Second,
		nil,
		orderflow.StalenessConfig{},
		func(coins []string, logger *zap.SugaredLogger) stream.ExchangeManager {
			return NewHyperliquidFuturesManager(coins, logger)
		},
		NewNormalizer(),
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

func TestReconcile_NoOpOnSameCoins(t *testing.T) {
	c := newTestController()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Run(ctx, []string{"BTC", "ETH"})
	defer c.Stop()
}

func TestUpdateSubscriptions_NoConn(t *testing.T) {
	m := NewHyperliquidFuturesManager([]string{"BTC"}, zap.S())
	err := m.UpdateSubscriptions([]string{"ETH"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active connection")
}

func TestLoadBaseCoins_FromSymbols(t *testing.T) {
	coins := LoadBaseCoins([]string{"BTCUSDT", "ETHUSDT", "1000PEPEUSDT"})
	require.Equal(t, []string{"1000PEPE", "BTC", "ETH"}, coins)
}
