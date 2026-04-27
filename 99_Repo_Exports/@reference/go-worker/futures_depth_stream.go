// Пакет binance (internal) реализует подключение к Binance Futures depth20@100ms
// и публикацию DOM в Redis Streams.
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/monitoring"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

const (
	depthStreamMaxLen      = 1000
	depthStreamReadLimit   = 2 << 20 // 2 MB
)

// Get configurable durations from env
func getDepthReadTimeout() time.Duration {
	return getEnvDuration("FUTURES_WS_READ_TIMEOUT", 300*time.Second) // Harmonized with futures_streams.go
}

func getDepthPingPeriod() time.Duration {
	return getEnvDuration("FUTURES_WS_PING_PERIOD", 27*time.Second)
}

func getDepthWriteWait() time.Duration {
	return getEnvDuration("FUTURES_WS_WRITE_WAIT", 5*time.Second)
}

// FuturesDepthStream подписывается на Binance Futures depth20@100ms и
// транслирует данные книги заявок в Redis Streams.
type FuturesDepthStream struct {
	Symbols     []string
	Redis       *redis.Client
	errorCount  int64 // Счетчик ошибок для фильтрации логов
}

type depthUpdate struct {
	EventTime int64      `json:"E"`
	UpdateID  int64      `json:"u"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
	Symbol    string     `json:"s"`
}

func (s *FuturesDepthStream) targetClients() []*redis.Client {
	candidates := []*redis.Client{
		s.Redis,
		redisclient.ClientTicks,
		redisclient.Client,
	}

	seen := make(map[*redis.Client]struct{}, len(candidates))
	result := make([]*redis.Client, 0, len(candidates))
	for _, client := range candidates {
		if client == nil {
			continue
		}
		if _, exists := seen[client]; exists {
			continue
		}
		seen[client] = struct{}{}
		result = append(result, client)
	}
	return result
}

// Run подключается к Binance Futures multiplex stream и публикует обновления DOM.
func (s *FuturesDepthStream) Run(ctx context.Context) error {
	if len(s.Symbols) == 0 {
		log.Println("⚠️ FuturesDepthStream: список символов пуст, поток не запущен")
		return nil
	}

	streams := make([]string, 0, len(s.Symbols))
	for _, sym := range s.Symbols {
		streams = append(streams, strings.ToLower(sym)+"@depth20@100ms")
	}
	url := "wss://fstream.binance.com/stream?streams=" + strings.Join(streams, "/")

	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: getEnvDuration("WS_HANDSHAKE_TIMEOUT", 45*time.Second),
		EnableCompression: false, // 🎯 FIX: Disable compression for stability
		NetDial: func(network, addr string) (net.Conn, error) {
			netDialer := &net.Dialer{
				Timeout:   getEnvDuration("WS_DIAL_TIMEOUT", 30*time.Second),
				KeepAlive: getEnvDuration("WS_TCP_KEEPALIVE", 60*time.Second),
			}
			// 🎯 FIX: Force IPv4 (tcp4)
			return netDialer.Dial("tcp4", addr)
		},
	}
	retryCount := 0
	lastMessageTime := time.Now()

	for {
		conn, _, err := dialer.Dial(url, nil)
		if err != nil {
			retryCount++
			log.Printf("❌ depth stream dial error (попытка %d): %v", retryCount, err)
			for _, sym := range s.Symbols {
				monitoring.RecordFuturesReconnect(sym, "depth_dial_error")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.getBackoffDelay(retryCount)):
				continue
			}
		}

		// Сбрасываем счетчик при успешном подключении
		retryCount = 0
		log.Printf("✅ Futures depth stream connected: %s", url)

		if err := s.consume(ctx, conn, &lastMessageTime); err != nil {
			// Проверяем, не является ли ошибка отменой контекста (graceful shutdown)
			if err == context.Canceled {
				log.Printf("🛑 Futures depth stream terminated: контекст отменен")
				return err
			}

			retryCount++
			currentErrorCount := atomic.AddInt64(&s.errorCount, 1)
			errStr := err.Error()

			// Улучшенная обработка ошибок с категоризацией
			isConnectionReset := strings.Contains(errStr, "connection reset by peer") ||
				strings.Contains(errStr, "connection closed") ||
				strings.Contains(errStr, "unexpected EOF") ||
				strings.Contains(errStr, "1006") ||
				strings.Contains(errStr, "abnormal closure")
			isTimeout := strings.Contains(errStr, "i/o timeout") ||
				strings.Contains(errStr, "timeout")
			isContextCancelled := strings.Contains(errStr, "context cancelled")

			// Не логируем context cancelled - это нормальное завершение
			if isContextCancelled {
				return err
			}

			// Connection reset и timeout - это нормальные сетевые ошибки, Binance может закрывать соединения
			// Логируем реже, чтобы не засорять логи (каждую 1000-ю ошибку или первые 5)
			if isConnectionReset || isTimeout {
				if currentErrorCount <= 5 || currentErrorCount%1000 == 0 {
					errorType := "connection reset"
					if isTimeout {
						errorType = "timeout"
					}
					log.Printf("⚠️ Futures depth stream terminated (%s, попытка переподключения, всего ошибок: %d): %v",
						errorType, currentErrorCount, err)
				}
			} else {
				// Для других ошибок логируем чаще (каждую 100-ю)
				if currentErrorCount <= 5 || currentErrorCount%100 == 0 {
					log.Printf("⚠️ Futures depth stream terminated (попытка переподключения, всего ошибок: %d): %v",
						currentErrorCount, err)
				}
			}

			for _, sym := range s.Symbols {
				monitoring.RecordFuturesReconnect(sym, "depth_read_error")
			}
			_ = conn.Close()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.getBackoffDelay(retryCount)):
			}
		}
	}
}

// getBackoffDelay возвращает задержку перед переподключением с экспоненциальным backoff
func (s *FuturesDepthStream) getBackoffDelay(retryCount int) time.Duration {
	baseDelay := 2 * time.Second
	maxDelay := 30 * time.Second

	// Экспоненциальный backoff: 2s, 4s, 8s, 16s, 30s (максимум)
	delay := baseDelay * time.Duration(1<<uint(min(retryCount, 4)))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *FuturesDepthStream) consume(ctx context.Context, conn *websocket.Conn, lastMessageTime *time.Time) error {
	defer conn.Close()

	conn.SetReadLimit(depthStreamReadLimit)

	// Устанавливаем начальный таймаут чтения
	_ = conn.SetReadDeadline(time.Now().Add(getDepthReadTimeout()))

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Обработчик ping от Binance - отвечаем pong
	// Binance отправляет ping, и мы должны отвечать pong, иначе соединение будет закрыто
	conn.SetPingHandler(func(appData string) error {
		*lastMessageTime = time.Now()
		// Сбрасываем таймаут чтения при получении ping
		_ = conn.SetReadDeadline(time.Now().Add(getDepthReadTimeout()))
		// Отправляем pong в ответ на ping от Binance
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(getDepthWriteWait()))
	})

	// Обработчик pong - сбрасывает таймаут чтения (когда мы отправляем ping и получаем pong)
	conn.SetPongHandler(func(string) error {
		*lastMessageTime = time.Now()
		return conn.SetReadDeadline(time.Now().Add(getDepthReadTimeout()))
	})

	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(getDepthPingPeriod())
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				// Отправляем ping для поддержания соединения (используем WriteControl для надежности)
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(getDepthWriteWait())); err != nil {
					errCh <- fmt.Errorf("ping error: %w", err)
					return
				}
				// Сбрасываем таймаут чтения после успешной отправки ping
				_ = conn.SetReadDeadline(time.Now().Add(getDepthReadTimeout()))
			}
		}
	}()

	// Канал для сообщений, чтобы не блокировать чтение
	msgChan := make(chan []byte, 2000) // Buffer for burst protection
	processCtx, processCancel := context.WithCancel(ctx)
	defer processCancel()

	// Запускаем воркер для обработки сообщений и записи в Redis
	go func() {
		for {
			select {
			case <-processCtx.Done():
				return
			case msg := <-msgChan:
				s.processMessage(processCtx, msg, s.targetClients())
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Устанавливаем таймаут чтения перед каждым ReadMessage
		readTimeout := getDepthReadTimeout()
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

		_, message, err := conn.ReadMessage()
		if err != nil {
			// Улучшенная обработка ошибок чтения
			errStr := err.Error()

			// Проверяем на таймаут
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Проверяем, когда было последнее сообщение
				timeSinceLastMsg := time.Since(*lastMessageTime)

				// Если прошло много времени, то это реальная проблема
				if timeSinceLastMsg > readTimeout {
					return fmt.Errorf("read error: i/o timeout (соединение неактивно, последнее сообщение: %v назад): %w", timeSinceLastMsg, err)
				}
				// Нормальный таймаут (wait) - просто нет данных, но соединение живое
				continue
			}

			// Проверяем на connection reset и unexpected EOF
			if strings.Contains(errStr, "connection reset by peer") ||
				strings.Contains(errStr, "connection closed") ||
				strings.Contains(errStr, "unexpected EOF") {
				return fmt.Errorf("read error: %w", err)
			}

			// Для всех остальных ошибок
			return fmt.Errorf("read error: %w", err)
		}

		// Обновляем время последнего сообщения
		*lastMessageTime = time.Now()

		// Сбрасываем таймаут чтения после успешного чтения
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

		// Отправляем в канал для асинхронной обработки
		select {
		case msgChan <- message:
		default:
			log.Printf("⚠️ FuturesDepthStream: буфер сообщений переполнен, пропускаем обновление!")
		}
	}
}

func (s *FuturesDepthStream) processMessage(ctx context.Context, message []byte, targets []*redis.Client) {
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(message, &wrapper); err != nil {
		return
	}

	var du depthUpdate
	if err := json.Unmarshal(wrapper.Data, &du); err != nil {
		return
	}

	symbol := strings.ToUpper(du.Symbol)
	streamName := fmt.Sprintf("stream:book_%s", symbol)

	if len(targets) == 0 {
		return // No logging to reduce noise, already logged at start if empty
	}

	payload := map[string]interface{}{
		"data": string(wrapper.Data),
		"ts":   du.EventTime,
	}

	var wg sync.WaitGroup
	// Use buffered channel to avoid blocking if one redis is slow
	// but we still wait for all writes to finish for this message to preserve order?
	// Actually for depth stream order is crucial. We should write parallel but wait.

	for _, client := range targets {
		wg.Add(1)
		go func(c *redis.Client) {
			defer wg.Done()
			args := &redis.XAddArgs{
				Stream: streamName,
				MaxLen: depthStreamMaxLen,
				Approx: true,
				Values: payload,
			}

			// Use shorter timeout for Redis writes to avoid backing up too much
			writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			if _, err := redisclient.XAddWithRetry(writeCtx, c, args); err != nil {
				// Log rarely
				// log.Printf("⚠️ depth stream XADD error (%s): %v", streamName, err)
			}
		}(client)
	}

	wg.Wait()
	monitoring.RecordFuturesMessage(symbol, "depth")
}
