package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	internalmodels "go-worker/internal/models"
	"go-worker/internal/monitoring"
	"go-worker/internal/wsconn"

	"github.com/gorilla/websocket"

	"go.uber.org/zap"
)

type Normalizer struct{}

func (n *Normalizer) Normalize(symbol string, payload []byte) ([]internalmodels.NormalizedTick, []internalmodels.NormalizedDepth, error) {
	return NormalizeFuturesMessage(symbol, payload)
}

// Config logic moved to wsconn package, use wsconn.Config where appropriate.

// FuturesMultiplexManager управляет мультиплексным WS соединением Binance Futures для набора символов.
type FuturesMultiplexManager struct {
	symbols []string
	conn    *websocket.Conn
	log     *zap.SugaredLogger
	mu      sync.Mutex // Защищает состояние структуры
	writeMu sync.Mutex // СТРОГО защищает конкурентную запись в сокет (Gorilla WS rule)
}

// BinanceWSCommand представляет JSON-команда для Binance WS API.
type BinanceWSCommand struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int      `json:"id"`
}

// NewFuturesMultiplexManager создаёт менеджер для нескольких символов.
func NewFuturesMultiplexManager(symbols []string, logger *zap.SugaredLogger) *FuturesMultiplexManager {
	return &FuturesMultiplexManager{
		symbols: symbols,
		log:     logger,
	}
}

const futuresWsEndpoint = "wss://fstream.binance.com/stream"

// Connect устанавливает мультиплексное соединение.
func (m *FuturesMultiplexManager) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}

	streams := make([]string, 0, len(m.symbols)*2)
	for _, sym := range m.symbols {
		s := strings.ToLower(sym)
		// Используем depth20@100ms вместо полного depth для экономии трафика
		streams = append(streams, s+"@depth20@100ms")
		streams = append(streams, s+"@aggTrade")
	}

	url := fmt.Sprintf("%s?streams=%s", futuresWsEndpoint, strings.Join(streams, "/"))

	cfg := wsconn.DefaultConfig()
	// Override config with env vars
	cfg.HandshakeTimeout = getEnvDuration("WS_HANDSHAKE_TIMEOUT", cfg.HandshakeTimeout)
	cfg.DialTimeout = getEnvDuration("WS_DIAL_TIMEOUT", cfg.DialTimeout)
	cfg.TCPKeepAlive = getEnvDuration("WS_TCP_KEEPALIVE", cfg.TCPKeepAlive)
	cfg.ReadTimeout = getEnvDuration("FUTURES_WS_READ_TIMEOUT", cfg.ReadTimeout)
	cfg.PingPeriod = getEnvDuration("FUTURES_WS_PING_PERIOD", cfg.PingPeriod)
	cfg.WriteWait = getEnvDuration("FUTURES_WS_WRITE_WAIT", cfg.WriteWait)

	conn, err := wsconn.Dial(ctx, url, cfg)
	if err != nil {
		return err
	}

	wsconn.SetupPingPong(conn, cfg.ReadTimeout, cfg.WriteWait, &m.writeMu, func(msg []byte) error {
		return conn.WriteControl(websocket.PongMessage, msg, time.Now().Add(cfg.WriteWait))
	})

	m.conn = conn
	return nil
}

// ReadLoop читает входящие сообщения и вызывает handler.
func (m *FuturesMultiplexManager) ReadLoop(ctx context.Context, handler func(symbol string, msg []byte)) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		return errors.New("websocket connection is nil")
	}

	readTimeout := getEnvDuration("FUTURES_WS_READ_TIMEOUT", 300*time.Second)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

	lastMessageTime := time.Now()
	msgChanCap := getEnvInt("BINANCE_WS_MSG_CHAN_CAP", 50_000)
	msgChan := make(chan []byte, msgChanCap)

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		for msg := range msgChan {
			// Мы должны распарсить конверт, чтобы узнать символ
			var envelope BinanceStreamEnvelope
			if err := json.Unmarshal(msg, &envelope); err != nil {
				continue
			}
			// Извлекаем символ из имени стрима (например, "btcusdt@aggTrade")
			parts := strings.Split(envelope.Stream, "@")
			if len(parts) > 0 {
				// Передаем исходный msg, так как NormalizeFuturesMessage ожидает полный конверт
				handler(parts[0], msg)
			}
		}
	}()

	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(getEnvDuration("FUTURES_WS_PING_PERIOD", 20*time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				if err := m.sendPing(); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	defer func() {
		close(msgChan)
		<-handlerDone
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if time.Since(lastMessageTime) > readTimeout {
					return fmt.Errorf("read timeout: %w", err)
				}
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

		lastMessageTime = time.Now()
		select {
		case msgChan <- msg:
		default:
			// P1 fix: load-shed instead of reconnect.
			// Buffer overflow means the handler goroutine is slower than WS ingestion.
			// Dropping here is safe — individual ticks are non-critical (next tick arrives soon).
			// Reconnecting would cause a ~1s gap, which is far worse than a single dropped tick.
			monitoring.RecordFuturesMessageDropped("MULTIPLEX")
			monitoring.RecordWSChanFillRatio("binance", float64(len(msgChan))/float64(cap(msgChan)))
		}
	}
}

func (m *FuturesMultiplexManager) sendPing() error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		return nil
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(getEnvDuration("FUTURES_WS_WRITE_WAIT", 5*time.Second)))
}

// UpdateSubscriptions отправляет команды SUBSCRIBE/UNSUBSCRIBE в активное соединение
// для обновления списка стримов без переподключения.
func (m *FuturesMultiplexManager) UpdateSubscriptions(addSymbols, removeSymbols []string) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		return errors.New("no active connection")
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	// Формируем стримы для добавления
	if len(addSymbols) > 0 {
		toAdd := make([]string, 0, len(addSymbols)*2)
		for _, sym := range addSymbols {
			s := strings.ToLower(sym)
			toAdd = append(toAdd, s+"@depth20@100ms", s+"@aggTrade")
		}

		cmd := BinanceWSCommand{
			Method: "SUBSCRIBE",
			Params: toAdd,
			ID:     int(time.Now().UnixMilli() % 10000),
		}
		if err := conn.WriteJSON(cmd); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
		m.mu.Lock()
		m.symbols = append(m.symbols, addSymbols...)
		m.mu.Unlock()
	}

	// Формируем стримы для удаления
	if len(removeSymbols) > 0 {
		toRemove := make([]string, 0, len(removeSymbols)*2)
		for _, sym := range removeSymbols {
			s := strings.ToLower(sym)
			toRemove = append(toRemove, s+"@depth20@100ms", s+"@aggTrade")
		}

		cmd := BinanceWSCommand{
			Method: "UNSUBSCRIBE",
			Params: toRemove,
			ID:     int(time.Now().UnixMilli()%10000) + 1,
		}
		if err := conn.WriteJSON(cmd); err != nil {
			return fmt.Errorf("unsubscribe failed: %w", err)
		}

		m.mu.Lock()
		newSymbols := make([]string, 0, len(m.symbols))
		removeSet := make(map[string]struct{})
		for _, s := range removeSymbols {
			removeSet[strings.ToUpper(s)] = struct{}{}
		}
		for _, s := range m.symbols {
			if _, ok := removeSet[strings.ToUpper(s)]; !ok {
				newSymbols = append(newSymbols, s)
			}
		}
		m.symbols = newSymbols
		m.mu.Unlock()
	}

	return nil
}

// Close закрывает текущий websocket.
func (m *FuturesMultiplexManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}
