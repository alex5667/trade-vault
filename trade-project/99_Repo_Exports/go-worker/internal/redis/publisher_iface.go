// Package redis (internal) — интерфейсы для публикации тиков и книг заявок в Redis Streams.
package redis

import "context"

// Publisher — единый интерфейс для публикации market data в Redis Streams.
// Реализован двумя backend-ами:
//   - TickPublisher     — прямой XADD (low-latency, goroutine-per-write)
//   - BatchTickPublisher — pipeline батчинг (high-throughput, ≤10ms latency budget)
type Publisher interface {
	// PublishTick публикует нормализованный тик в stream:tick_<SYMBOL>.
	PublishTick(ctx context.Context, symbol string, values map[string]any) error
	// PublishBook публикует снапшот книги заявок в stream:book_<SYMBOL>.
	PublishBook(ctx context.Context, symbol string, values map[string]any) error

	// PublishTickPooled takes ownership of a pooled map and publishes it.
	PublishTickPooled(ctx context.Context, symbol string, values map[string]any) error
	// PublishBookPooled takes ownership of a pooled map and publishes a book snapshot.
	PublishBookPooled(ctx context.Context, symbol string, values map[string]any) error
}
