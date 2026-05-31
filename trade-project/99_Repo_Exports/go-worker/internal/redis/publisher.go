package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"
)

// TickPublisher публикует нормализованные тики и книги заявок в Redis Streams.
type TickPublisher struct {
	clients []*redis.Client
}

// NewTickPublisher создаёт новый TickPublisher.
func NewTickPublisher(clients ...*redis.Client) *TickPublisher {
	unique := make([]*redis.Client, 0, len(clients))
	seen := make(map[*redis.Client]struct{}, len(clients))
	for _, client := range clients {
		if client == nil {
			continue
		}
		if _, exists := seen[client]; exists {
			continue
		}
		seen[client] = struct{}{}
		unique = append(unique, client)
	}
	return &TickPublisher{clients: unique}
}

// PublishTick отправляет тик в stream:tick_<SYMBOL>.
func (p *TickPublisher) PublishTick(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	stream := streams.TickStream(symbol)
	return p.publish(ctx, stream, symbol, values)
}

// PublishBook отправляет книгу заявок в stream:book_<SYMBOL>.
func (p *TickPublisher) PublishBook(ctx context.Context, symbol string, values map[string]any) error {
	symbol = strings.ToUpper(symbol)
	stream := streams.BookStream(symbol)
	return p.publish(ctx, stream, symbol, values)
}

// PublishTickPooled отправляет тик и возвращает map в пул.
func (p *TickPublisher) PublishTickPooled(ctx context.Context, symbol string, values map[string]any) error {
	err := p.PublishTick(ctx, symbol, values)
	ReleaseTickMap(values)
	return err
}

// PublishBookPooled отправляет книгу заявок и возвращает map в пул.
func (p *TickPublisher) PublishBookPooled(ctx context.Context, symbol string, values map[string]any) error {
	err := p.PublishBook(ctx, symbol, values)
	ReleaseTickMap(values)
	return err
}

func (p *TickPublisher) publish(ctx context.Context, stream string, symbol string, values map[string]any) error {
	if p == nil || len(p.clients) == 0 {
		return fmt.Errorf("tick publisher not initialised")
	}

	if len(values) == 0 {
		return fmt.Errorf("empty payload for %s", stream)
	}

	// ПРАВИЛЬНО: Используем независимый контекст для критической операции записи,
	// чтобы SIGTERM (отмена ctx) не приводил к потере последнего тика.
	flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(p.clients))

	for _, client := range p.clients {
		wg.Add(1)
		go func(c *redis.Client) {
			defer wg.Done()
			// Используем flushCtx вместо ctx
			if _, err := redisclient.XAddWithRetry(flushCtx, c, &redis.XAddArgs{
				Stream: stream,
				ID:     "*",
				Values: values,
			}); err != nil {
				// В лог пишем исходный контекст для понимания причины ошибки если надо,
				// но саму запись делали через Background.
				errCh <- err
			}
		}(client)
	}

	// Ждем завершения всех горутин в отдельной горутине, чтобы не блокировать закрытие канала
	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errList []error
	var success bool
	var canceled bool

	for err := range errCh {
		if err != nil {
			if err == context.Canceled {
				canceled = true
			} else {
				errList = append(errList, err)
			}
			continue
		}
		success = true
	}

	if success {
		return nil
	}

	if canceled {
		return context.Canceled
	}

	if len(errList) > 0 {
		return fmt.Errorf("publish %s (%s): %w", stream, symbol, errors.Join(errList...))
	}

	return nil
}
