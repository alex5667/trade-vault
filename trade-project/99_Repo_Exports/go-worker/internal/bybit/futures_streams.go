package bybit

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

type Normalizer struct {
	bookDepth int
	// 32 sharded mutexes to avoid a single global lock under parallel workloads.
	// Shard is chosen by FNV hash of symbol, so per-symbol ordering is preserved.
	bookShards [32]sync.Mutex
	bookStates map[string]*BookState
	bookMu     sync.RWMutex // only for map read/write (not ApplyUpdate)
}

func (n *Normalizer) shardIdx(symbol string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(symbol); i++ {
		h *= 16777619
		h ^= uint32(symbol[i])
	}
	return int(h % 32)
}

func NewNormalizer(bookDepth int) *Normalizer {
	if bookDepth <= 0 {
		bookDepth = getEnvInt("BYBIT_BOOK_DEPTH", 50)
	}
	return &Normalizer{
		bookDepth:  bookDepth,
		bookStates: make(map[string]*BookState, 128),
	}
}

func (n *Normalizer) Normalize(symbol string, payload []byte) (ticks []internalmodels.NormalizedTick, books []internalmodels.NormalizedDepth, err error) {
	// 1. Попробуем сперва распарсить trades
	ticks, errDecode := ParsePublicTrade(payload)
	if errDecode == nil && len(ticks) > 0 {
		return ticks, nil, nil
	}

	// Если это не trade (ошибка декодинга или len==0), то попробуем parse orderbook update
	upd, errDecodeBk := ParseOrderbook(payload)
	if errDecodeBk != nil || upd == nil {
		// Ошибка декодинга или невалидное сообщение
		return nil, nil, fmt.Errorf("could not decode either publicTrade or orderbook msg: payload=%s", symbol)
	}

	// Get or create per-symbol BookState under the global map lock.
	n.bookMu.RLock()
	bs, ok := n.bookStates[upd.Symbol]
	n.bookMu.RUnlock()
	if !ok {
		n.bookMu.Lock()
		bs, ok = n.bookStates[upd.Symbol]
		if !ok {
			bs = newBookState()
			n.bookStates[upd.Symbol] = bs
		}
		n.bookMu.Unlock()
	}

	// Apply the update under the per-symbol shard lock.
	shard := n.shardIdx(upd.Symbol)
	n.bookShards[shard].Lock()
	bids, asks, prevU, gapDetected, gapExpected, gapActual := bs.ApplyUpdate(upd, n.bookDepth)
	n.bookShards[shard].Unlock()

	book := internalmodels.NormalizedDepth{
		Symbol:       upd.Symbol,
		Ts:           upd.TsMs,
		FirstID:      upd.UpdateID,
		FinalID:      upd.UpdateID,
		PrevFinal:    prevU,
		Bids:         bids,
		Asks:         asks,
		Source:       "bybit-linear",
		Market:       "USDT-M",
		Seq:          upd.Seq,
		GapDetected:  gapDetected,
		GapExpected:  gapExpected,
		GapActual:    gapActual,
		QualityFlags: upd.QualityFlags,
	}

	books = append(books, book)
	return nil, books, nil

}

// Bybit Futures/Perps public WS endpoint (Linear USDT-M):
//
//	wss://stream.bybit.com/v5/public/linear
//
// Overridable via ENV BYBIT_FUTURES_WS_URL.
const defaultBybitFuturesWsEndpoint = "wss://stream.bybit.com/v5/public/linear"

// BybitWSCommand represents V5 subscribe/unsubscribe/ping.
//
// Example:
//
//	{"op":"subscribe","args":["publicTrade.BTCUSDT","orderbook.50.BTCUSDT"]}
//
// Docs:
//
//	https://bybit-exchange.github.io/docs/v5/websocket/public/trade
//	https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
//
// NOTE: Bybit requires application-level heartbeat (op:ping).
// We send it periodically in ReadLoop.
type BybitWSCommand struct {
	Op   string   `json:"op"`
	Args []string `json:"args,omitempty"`
}

// FuturesMultiplexManager manages one Bybit WS connection subscribed to multiple symbols.
// Unlike Binance URL-multiplexing, Bybit uses subscribe messages.
type FuturesMultiplexManager struct {
	symbols []string
	conn    *websocket.Conn
	log     *zap.SugaredLogger

	mu      sync.Mutex
	writeMu sync.Mutex // protects concurrent conn.Write*

	bookDepth  int
	pingPeriod time.Duration
}

func NewFuturesMultiplexManager(symbols []string, logger *zap.SugaredLogger, bookDepth int, pingPeriod time.Duration) *FuturesMultiplexManager {
	if bookDepth <= 0 {
		bookDepth = 50
	}
	if pingPeriod <= 0 {
		pingPeriod = 20 * time.Second
	}
	return &FuturesMultiplexManager{symbols: symbols, log: logger, bookDepth: bookDepth, pingPeriod: pingPeriod}
}

func (m *FuturesMultiplexManager) endpoint() string {
	return getEnvString("BYBIT_FUTURES_WS_URL", defaultBybitFuturesWsEndpoint)
}

// Connect establishes WS and subscribes to required topics.
func (m *FuturesMultiplexManager) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}

	url := m.endpoint()

	cfg := wsconn.DefaultConfig()
	cfg.HandshakeTimeout = getEnvDuration("BYBIT_WS_HANDSHAKE_TIMEOUT", cfg.HandshakeTimeout)
	cfg.DialTimeout = getEnvDuration("BYBIT_WS_DIAL_TIMEOUT", cfg.DialTimeout)
	cfg.TCPKeepAlive = getEnvDuration("BYBIT_WS_TCP_KEEPALIVE", cfg.TCPKeepAlive)
	cfg.ReadTimeout = getEnvDuration("BYBIT_WS_READ_TIMEOUT", 90*time.Second)
	cfg.WriteWait = getEnvDuration("BYBIT_WS_WRITE_WAIT", cfg.WriteWait)

	conn, err := wsconn.Dial(ctx, url, cfg)
	if err != nil {
		return err
	}

	// Setup WS ping/pong handlers (frame-level). Bybit main heartbeat is op:ping.
	wsconn.SetupPingPong(conn, cfg.ReadTimeout, cfg.WriteWait, &m.writeMu, func(msg []byte) error {
		return conn.WriteControl(websocket.PongMessage, msg, time.Now().Add(cfg.WriteWait))
	})

	m.conn = conn

	// Subscribe for symbols
	args := make([]string, 0, len(m.symbols)*2)
	for _, sym := range m.symbols {
		s := strings.ToUpper(strings.TrimSpace(sym))
		if s == "" {
			continue
		}
		args = append(args, "publicTrade."+s)
		args = append(args, fmt.Sprintf("orderbook.%d.%s", m.bookDepth, s))
	}
	cmd := BybitWSCommand{Op: "subscribe", Args: args}

	m.writeMu.Lock()
	err = conn.WriteJSON(cmd)
	m.writeMu.Unlock()
	if err != nil {
		_ = conn.Close()
		m.conn = nil
		return fmt.Errorf("subscribe write failed: %w", err)
	}

	monitoring.RecordBybitLiveSubscription("subscribe")

	return nil
}

// ReadLoop reads messages and dispatches them to handler.
// handler is called with a best-effort symbol extracted from topic.
func (m *FuturesMultiplexManager) ReadLoop(ctx context.Context, handler func(symbol string, msg []byte)) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	if conn == nil {
		return errors.New("websocket connection is nil")
	}

	readTimeout := getEnvDuration("BYBIT_WS_READ_TIMEOUT", 90*time.Second)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

	lastMessageTime := time.Now()
	msgChanCap := getEnvInt("BYBIT_WS_MSG_CHAN_CAP", 20_000)
	msgChan := make(chan []byte, msgChanCap)

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		for msg := range msgChan {
			// Minimal envelope to route by symbol.
			var env struct {
				Topic string `json:"topic"`
				Op    string `json:"op"`
			}
			if err := json.Unmarshal(msg, &env); err != nil {
				continue
			}
			if env.Op != "" {
				continue
			}
			sym := extractSymbolFromTopic(env.Topic)
			handler(sym, msg)
		}
	}()

	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		// Bybit V5 requires op:ping heartbeat.
		ticker := time.NewTicker(m.pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				m.writeMu.Lock()
				err := conn.WriteJSON(BybitWSCommand{Op: "ping"})
				m.writeMu.Unlock()
				if err != nil {
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
			// load-shed instead of reconnect.
			monitoring.RecordFuturesMessageDropped("BYBIT_MULTIPLEX")
			monitoring.RecordWSChanFillRatio("bybit", float64(len(msgChan))/float64(cap(msgChan)))
		}
	}
}

// UpdateSubscriptions sends SUBSCRIBE/UNSUBSCRIBE to update topics without reconnect.
func (m *FuturesMultiplexManager) UpdateSubscriptions(addSymbols, removeSymbols []string) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	if conn == nil {
		return errors.New("no active connection")
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	if len(addSymbols) > 0 {
		args := make([]string, 0, len(addSymbols)*2)
		for _, sym := range addSymbols {
			s := strings.ToUpper(strings.TrimSpace(sym))
			if s == "" {
				continue
			}
			args = append(args, "publicTrade."+s)
			args = append(args, fmt.Sprintf("orderbook.%d.%s", m.bookDepth, s))
		}
		cmd := BybitWSCommand{Op: "subscribe", Args: args}
		if err := conn.WriteJSON(cmd); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
		monitoring.RecordBybitLiveSubscription("subscribe")
		m.mu.Lock()
		m.symbols = append(m.symbols, addSymbols...)
		m.mu.Unlock()
	}

	if len(removeSymbols) > 0 {
		args := make([]string, 0, len(removeSymbols)*2)
		for _, sym := range removeSymbols {
			s := strings.ToUpper(strings.TrimSpace(sym))
			if s == "" {
				continue
			}
			args = append(args, "publicTrade."+s)
			args = append(args, fmt.Sprintf("orderbook.%d.%s", m.bookDepth, s))
		}
		cmd := BybitWSCommand{Op: "unsubscribe", Args: args}
		if err := conn.WriteJSON(cmd); err != nil {
			return fmt.Errorf("unsubscribe failed: %w", err)
		}
		monitoring.RecordBybitLiveSubscription("unsubscribe")

		m.mu.Lock()
		removeSet := make(map[string]struct{}, len(removeSymbols))
		for _, s := range removeSymbols {
			removeSet[strings.ToUpper(s)] = struct{}{}
		}
		newSymbols := make([]string, 0, len(m.symbols))
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

func (m *FuturesMultiplexManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

// extractSymbolFromTopic extracts symbol from topic name.
// Supported patterns:
//
//	publicTrade.BTCUSDT
//	orderbook.50.BTCUSDT
func extractSymbolFromTopic(topic string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return ""
	}
	parts := strings.Split(topic, ".")
	if len(parts) == 0 {
		return ""
	}
	sym := parts[len(parts)-1]
	return strings.ToUpper(strings.TrimSpace(sym))
}
