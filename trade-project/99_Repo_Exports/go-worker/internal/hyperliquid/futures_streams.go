package hyperliquid

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
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
	symbolMap     map[string]string
	symbolSuffix  string
	maxBookLevels int
}

func NewNormalizer() *Normalizer {
	suffix := getEnv("HYPERLIQUID_SYMBOL_SUFFIX", "USDT")
	if getEnvBool("HYPERLIQUID_KEEP_COIN_SYMBOL", false) {
		suffix = ""
	}
	return &Normalizer{
		symbolMap:     parseSymbolMap(getEnv("HYPERLIQUID_SYMBOL_MAP", "")),
		symbolSuffix:  suffix,
		maxBookLevels: getEnvInt("HYPERLIQUID_BOOK_MAX_LEVELS", 20),
	}
}

func (n *Normalizer) Normalize(symbol string, payload []byte) (ticks []internalmodels.NormalizedTick, books []internalmodels.NormalizedDepth, err error) {
	// For HL, symbol is embedded or not needed inside the parser because payload contains Coin.
	return NormalizeFuturesMessage(payload, n.symbolMap, n.symbolSuffix, n.maxBookLevels)
}

// HyperliquidFuturesManager manages a single WS connection to Hyperliquid.
// One connection can hold many subscriptions (limit ~1000 per IP per docs).
//
// Key differences vs Binance:
//   - Subscriptions are sent as JSON messages (subscribe/unsubscribe) after connect.
//   - Server expects client heartbeats if it didn't send any message for 60s.
//     Heartbeat format: {"method":"ping"} and server responds {"channel":"pong"}.
//     (See: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)
type HyperliquidFuturesManager struct {
	coins []string
	conn  *websocket.Conn
	log   *zap.SugaredLogger

	mu      sync.Mutex
	writeMu sync.Mutex

	tradesEnabled bool
	bookEnabled   bool
}

type subscription struct {
	Type string `json:"type"`
	Coin string `json:"coin"`
	// Optional params exist (nSigFigs/mantissa for l2Book) but we omit them;
	// we cap levels client-side before publishing to Redis.
}

type wsCommand struct {
	Method       string        `json:"method"`
	Subscription *subscription `json:"subscription,omitempty"`
}

func NewHyperliquidFuturesManager(coins []string, logger *zap.SugaredLogger) *HyperliquidFuturesManager {
	return &HyperliquidFuturesManager{
		coins:         coins,
		log:           logger,
		tradesEnabled: getEnvBool("HYPERLIQUID_TRADES_ENABLED", true),
		bookEnabled:   getEnvBool("HYPERLIQUID_L2BOOK_ENABLED", true),
	}
}

func (m *HyperliquidFuturesManager) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}

	wsURL := getEnv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")

	cfg := wsconn.DefaultConfig()
	cfg.HandshakeTimeout = getEnvDuration("HYPERLIQUID_WS_HANDSHAKE_TIMEOUT", cfg.HandshakeTimeout)
	cfg.DialTimeout = getEnvDuration("HYPERLIQUID_WS_DIAL_TIMEOUT", cfg.DialTimeout)
	cfg.TCPKeepAlive = getEnvDuration("HYPERLIQUID_WS_TCP_KEEPALIVE", cfg.TCPKeepAlive)
	cfg.ReadTimeout = getEnvDuration("HYPERLIQUID_WS_READ_TIMEOUT", 90*time.Second)
	cfg.PingPeriod = getEnvDuration("HYPERLIQUID_WS_PING_PERIOD", 20*time.Second)
	cfg.WriteWait = getEnvDuration("HYPERLIQUID_WS_WRITE_WAIT", cfg.WriteWait)
	cfg.MsgChanCapacity = getEnvInt("HYPERLIQUID_WS_MSG_CHAN", cfg.MsgChanCapacity)

	conn, err := wsconn.Dial(ctx, wsURL, cfg)
	if err != nil {
		return err
	}

	// Setup WS control ping/pong for transport-level health.
	// Hyperliquid keepalive is application-level ping message; we do both.
	wsconn.SetupPingPong(conn, cfg.ReadTimeout, cfg.WriteWait, &m.writeMu, func(msg []byte) error {
		return conn.WriteControl(websocket.PongMessage, msg, time.Now().Add(cfg.WriteWait))
	})

	m.conn = conn

	// Subscribe to initial coins.
	if err := m.subscribeCoinsLocked(m.coins); err != nil {
		_ = conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

func (m *HyperliquidFuturesManager) ReadLoop(ctx context.Context, handler func(symbol string, msg []byte)) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	if conn == nil {
		return errors.New("websocket connection is nil")
	}

	readTimeout := getEnvDuration("HYPERLIQUID_WS_READ_TIMEOUT", 90*time.Second)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

	lastMessageTime := time.Now()
	msgChanCap := getEnvInt("HYPERLIQUID_WS_MSG_CHAN", wsconn.DefaultConfig().MsgChanCapacity)
	msgChan := make(chan []byte, msgChanCap)

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		for msg := range msgChan {
			handler("", msg)
		}
	}()

	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		// Hyperliquid requires client to send {method:ping} if server hasn't sent any message for 60s.
		// We send it periodically regardless — cheap and robust.
		ticker := time.NewTicker(getEnvDuration("HYPERLIQUID_WS_PING_PERIOD", 20*time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				if err := m.sendAppPing(); err != nil {
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
			m.log.Errorf("ReadLoop error after %v: %v", time.Since(lastMessageTime), err)
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
			// load-shed (do not reconnect). Same logic as Binance futures.
			monitoring.RecordFuturesMessageDropped("HYPERLIQUID")
		}
	}
}

func (m *HyperliquidFuturesManager) sendAppPing() error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	if conn == nil {
		return nil
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return conn.WriteJSON(wsCommand{Method: "ping"})
}

// UpdateSubscriptions updates active subscriptions without reconnect.
// Hyperliquid requires a separate subscribe/unsubscribe per (type, coin).
func (m *HyperliquidFuturesManager) UpdateSubscriptions(addCoins, removeCoins []string) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	if conn == nil {
		return errors.New("no active connection")
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	if len(removeCoins) > 0 {
		for _, coin := range removeCoins {
			coin = strings.ToUpper(strings.TrimSpace(coin))
			if coin == "" {
				continue
			}
			if m.tradesEnabled {
				if err := conn.WriteJSON(wsCommand{Method: "unsubscribe", Subscription: &subscription{Type: "trades", Coin: coin}}); err != nil {
					return fmt.Errorf("unsubscribe trades(%s): %w", coin, err)
				}
			}
			if m.bookEnabled {
				if err := conn.WriteJSON(wsCommand{Method: "unsubscribe", Subscription: &subscription{Type: "l2Book", Coin: coin}}); err != nil {
					return fmt.Errorf("unsubscribe l2Book(%s): %w", coin, err)
				}
			}
		}
		monitoring.RecordHyperliquidLiveSubscription("UNSUBSCRIBE")
	}

	if len(addCoins) > 0 {
		for _, coin := range addCoins {
			coin = strings.ToUpper(strings.TrimSpace(coin))
			if coin == "" {
				continue
			}
			if m.tradesEnabled {
				if err := conn.WriteJSON(wsCommand{Method: "subscribe", Subscription: &subscription{Type: "trades", Coin: coin}}); err != nil {
					return fmt.Errorf("subscribe trades(%s): %w", coin, err)
				}
			}
			if m.bookEnabled {
				if err := conn.WriteJSON(wsCommand{Method: "subscribe", Subscription: &subscription{Type: "l2Book", Coin: coin}}); err != nil {
					return fmt.Errorf("subscribe l2Book(%s): %w", coin, err)
				}
			}
		}
		monitoring.RecordHyperliquidLiveSubscription("SUBSCRIBE")
	}

	// Update internal list.
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(addCoins) > 0 {
		m.coins = append(m.coins, addCoins...)
	}
	if len(removeCoins) > 0 {
		removeSet := map[string]struct{}{}
		for _, c := range removeCoins {
			removeSet[strings.ToUpper(c)] = struct{}{}
		}
		out := make([]string, 0, len(m.coins))
		for _, c := range m.coins {
			if _, ok := removeSet[strings.ToUpper(c)]; !ok {
				out = append(out, c)
			}
		}
		m.coins = out
	}
	return nil
}

func (m *HyperliquidFuturesManager) subscribeCoinsLocked(coins []string) error {
	conn := m.conn
	if conn == nil {
		return errors.New("no active connection")
	}
	// Sort for determinism (helps logs and tests).
	out := make([]string, 0, len(coins))
	seen := map[string]struct{}{}
	for _, c := range coins {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.Strings(out)

	// Subscription limits: 1000 per IP per docs; each coin uses up to 2 subscriptions.
	maxSubs := getEnvInt("HYPERLIQUID_MAX_SUBSCRIPTIONS", 900)
	needed := 0
	if m.tradesEnabled {
		needed += len(out)
	}
	if m.bookEnabled {
		needed += len(out)
	}
	if needed > maxSubs {
		return fmt.Errorf("hyperliquid subscriptions requested=%d exceeds limit=%d (reduce HYPERLIQUID_COINS)", needed, maxSubs)
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	// Rate-limit subscribe writes: max 50 msgs/s (one per 20ms tick).
	// Using a ticker instead of Sleep avoids accumulating drift and is non-blocking
	// relative to the caller (writeMu is held, but we're in Connect, not hot-path).
	rateTicker := time.NewTicker(20 * time.Millisecond)
	defer rateTicker.Stop()

	for _, coin := range out {
		if m.tradesEnabled {
			if err := conn.WriteJSON(wsCommand{Method: "subscribe", Subscription: &subscription{Type: "trades", Coin: coin}}); err != nil {
				return fmt.Errorf("subscribe trades(%s): %w", coin, err)
			}
			<-rateTicker.C
		}
		if m.bookEnabled {
			if err := conn.WriteJSON(wsCommand{Method: "subscribe", Subscription: &subscription{Type: "l2Book", Coin: coin}}); err != nil {
				return fmt.Errorf("subscribe l2Book(%s): %w", coin, err)
			}
			<-rateTicker.C
		}
	}
	monitoring.RecordHyperliquidLiveSubscription("SUBSCRIBE")
	return nil
}

func (m *HyperliquidFuturesManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

// parseCoinsFromSymbols converts Binance-style symbols to Hyperliquid coins.
// Examples: BTCUSDT -> BTC, 1000PEPEUSDT -> 1000PEPE.
func parseCoinsFromSymbols(symbols []string) []string {
	out := make([]string, 0, len(symbols))
	for _, s := range symbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		// Most of your pipeline uses USDT quote. Hyperliquid coins are base tickers.
		s = strings.TrimSuffix(s, "USDT")
		s = strings.TrimSuffix(s, "USDC")
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return []string{"BTC", "ETH"}
	}
	// Dedup
	seen := map[string]struct{}{}
	uniq := make([]string, 0, len(out))
	for _, c := range out {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		uniq = append(uniq, c)
	}
	sort.Strings(uniq)
	return uniq
}

// LoadBaseCoins reads HYPERLIQUID_COINS or derives coins from FUTURES_SYMBOLS.
func LoadBaseCoins(futuresSymbols []string) []string {
	raw := strings.TrimSpace(os.Getenv("HYPERLIQUID_COINS"))
	if raw != "" {
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			c := strings.ToUpper(strings.TrimSpace(p))
			if c != "" {
				out = append(out, c)
			}
		}
		if len(out) > 0 {
			sort.Strings(out)
			return out
		}
	}
	return parseCoinsFromSymbols(futuresSymbols)
}
