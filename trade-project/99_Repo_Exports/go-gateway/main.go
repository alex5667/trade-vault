package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"context"
	gwinternal "scanner-gw/internal"
	handlersx "scanner-gw/internal/handlers"
	runtimex "scanner-gw/internal/runtime"

	metrics "scanner-gw/internal/metrics"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	redisv9 "github.com/redis/go-redis/v9"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// OrderCommand represents a command for MT5 executor (open/modify/cancel/resize).
type OrderCommand struct {
	Action     string         `json:"action"`
	SID        string         `json:"sid"`
	Symbol     string         `json:"symbol,omitempty"`
	Side       string         `json:"side,omitempty"`
	Lot        float64        `json:"lot,omitempty"`
	Entry      *float64       `json:"entry,omitempty"`
	SL         *float64       `json:"sl,omitempty"`
	TPLevels   []float64      `json:"tp_levels,omitempty"`
	Mode       string         `json:"mode,omitempty"`
	ATRMult    *float64       `json:"atr_mult,omitempty"`
	TrailPts   *float64       `json:"trail_points,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	PositionID string         `json:"position_id,omitempty"`
	Source     string         `json:"source,omitempty"`
	Timestamp  int64          `json:"timestamp,omitempty"`
}

func (c *OrderCommand) normalize() {
	if c.Metadata == nil {
		c.Metadata = make(map[string]any)
	}

	// Sanitize metadata: convert booleans to integers for serialization compatibility
	for k, v := range c.Metadata {
		if b, ok := v.(bool); ok {
			if b {
				c.Metadata[k] = 1
			} else {
				c.Metadata[k] = 0
			}
		}
	}

	c.Action = strings.ToLower(strings.TrimSpace(c.Action))
	if c.Action == "" {
		c.Action = "open"
	}

	// Normalize legacy actions
	switch c.Action {
	case "modify_sl", "trail":
		if c.SL != nil {
			c.Metadata["trail_request"] = 1 // Convert boolean to int for serialization compatibility
			if c.Mode != "" {
				c.Metadata["trail_mode"] = c.Mode
			}
			if c.TrailPts != nil {
				c.Metadata["trail_points"] = *c.TrailPts
			}
			c.Action = "modify"
		}
	}

	if c.Timestamp == 0 {
		c.Timestamp = time.Now().UnixMilli()
	}
}

// RedisOrderQueue stores commands in Redis list for durability.
type RedisOrderQueue struct {
	client *redisv9.Client
	key    string
}

func NewRedisOrderQueue(client *redisv9.Client, key string) *RedisOrderQueue {
	if key == "" {
		key = "orders:queue:binance"
	}
	return &RedisOrderQueue{client: client, key: key}
}

func (q *RedisOrderQueue) Enqueue(ctx context.Context, cmd OrderCommand) (int64, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	res := q.client.LPush(ctx, q.key, payload)
	return res.Result()
}

func (q *RedisOrderQueue) Dequeue(ctx context.Context, symbol string) (*OrderCommand, bool, error) {
	raw, err := q.client.RPop(ctx, q.key).Result()
	if err == redisv9.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var cmd OrderCommand
	if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
		zap.S().Errorf("❌ Failed to decode order command from queue: %v", err)
		return nil, false, err
	}

	if strings.TrimSpace(cmd.SID) == "" {
		// zap.S().Warnf("⚠️ Dropping order command with empty SID: %s", raw)
		return nil, false, nil
	}

	if symbol != "" && !strings.EqualFold(cmd.Symbol, symbol) {
		// Push back to the tail to preserve original order
		if pushErr := q.client.RPush(ctx, q.key, raw).Err(); pushErr != nil {
			zap.S().Errorf("❌ Failed to requeue order command: %v", pushErr)
			return nil, false, pushErr
		}
		return nil, false, nil
	}

	return &cmd, true, nil
}

// Telegram handles Telegram bot API interactions
type Telegram struct {
	bot    *tgbotapi.BotAPI
	chatID int64
	obiURL string // http://127.0.0.1:8090
}

// NewTelegram creates a new Telegram client
func NewTelegram() *Telegram {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	cid := os.Getenv("TELEGRAM_CHAT_ID")
	obiHost := os.Getenv("OBI_HOST")
	if obiHost == "" {
		obiHost = "http://127.0.0.1:8090"
	}
	if token == "" || cid == "" {
		zap.S().Warn("⚠️  Telegram disabled: BOT_TOKEN or CHAT_ID not set")
		return &Telegram{nil, 0, obiHost}
	}
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		zap.S().Fatalf("❌ Telegram initialization failed: %v", err)
	}
	id, _ := strconv.ParseInt(cid, 10, 64)
	zap.S().Infof("✅ Telegram bot connected: @%s", bot.Self.UserName)
	return &Telegram{bot, id, obiHost}
}

// SendText sends a text message to Telegram
func (t *Telegram) SendText(msg string) {
	if t.bot == nil {
		zap.S().Warn("⚠️  Telegram bot is nil, cannot send message")
		return
	}
	m := tgbotapi.NewMessage(t.chatID, msg)
	m.ParseMode = "HTML"
	if _, err := t.bot.Send(m); err != nil {
		zap.S().Errorf("❌ Failed to send Telegram message: %v", err)
	} else {
		zap.S().Infof("✅ Telegram message sent successfully to chat %d", t.chatID)
	}
}

// SendOBIPhoto fetches and sends OBI PNG from OBI service
func (t *Telegram) SendOBIPhoto(symbol string, caption string) {
	if t.bot == nil {
		return
	}
	url := fmt.Sprintf("%s/render/obi.png?symbol=%s&last=300", t.obiURL, symbol)
	resp, err := http.Get(url)
	if err != nil {
		zap.S().Errorf("⚠️  Failed to fetch OBI PNG: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		zap.S().Warnf("⚠️  OBI PNG returned status %d", resp.StatusCode)
		return
	}

	b, _ := io.ReadAll(resp.Body)
	photo := tgbotapi.FileBytes{Name: "obi.png", Bytes: b}
	msg := tgbotapi.NewPhoto(t.chatID, photo)
	msg.Caption = caption

	if _, err := t.bot.Send(msg); err != nil {
		zap.S().Errorf("⚠️  Failed to send photo: %v", err)
	}
}

// NotifyPayload represents an OBI event notification from Python service
type NotifyPayload struct {
	TS         int64   `json:"ts"` // milliseconds
	Symbol     string  `json:"symbol"`
	Type       string  `json:"type"`
	DurationMs int     `json:"duration_ms"`
	OBI        float64 `json:"obi"`
	Threshold  float64 `json:"threshold"`
}

func main() {

	// Initialize structured logging
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			zap.S().Errorf("❌ CRITICAL PANIC: %v", r)
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8088"
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	zap.S().Infof("🚀 Go Gateway initialization starting...")
	zap.S().Infof("   Port: %s", port)
	zap.S().Infof("   Redis: %s", os.Getenv("REDIS_URL"))
	zap.S().Infof("   Symbol: %s", os.Getenv("SYMBOL"))

	tg := NewTelegram()

	// Initialize Prometheus metrics
	metrics.Register()
	metrics.Heartbeat()

	mux := http.NewServeMux()

	// Redis client for paper-mode hooks
	rc := os.Getenv("REDIS_URL")
	if rc == "" {
		rc = "redis://localhost:6379/0"
	}
	zap.S().Infof("   Connecting to Redis: %s", rc)
	ropt, err := redisv9.ParseURL(rc)
	if err != nil {
		zap.S().Fatalf("❌ Failed to parse Redis URL '%s': %v", rc, err)
	}
	rdb := redisv9.NewClient(ropt)
	rctx := context.Background()

	// waitForRedis blocks until Redis is ready (handles LOADING state)
	waitForRedis := func(rdb *redisv9.Client, ctx context.Context) {
		zap.S().Infof("⏳ Waiting for Redis to be ready...")
		baseDelay := 1 * time.Second
		maxDelay := 30 * time.Second
		currentDelay := baseDelay
		lastLogTime := time.Now()
		logInterval := 5 * time.Second // Log at most every 5 seconds
		attempt := 0

		for {
			err := rdb.Ping(ctx).Err()
			if err == nil {
				if attempt > 0 {
					zap.S().Infof("✅ Redis connection successful (after %d attempts)", attempt)
				} else {
					zap.S().Infof("✅ Redis connection successful")
				}
				return
			}

			attempt++
			now := time.Now()
			shouldLog := now.Sub(lastLogTime) >= logInterval

			// Check for loading error
			isLoading := strings.Contains(err.Error(), "LOADING")

			if shouldLog {
				if isLoading {
					zap.S().Infof("⏳ Redis is LOADING dataset... (attempt %d, retrying in %.1fs)", attempt, currentDelay.Seconds())
				} else {
					zap.S().Errorf("⚠️  Redis connection failed: %v (attempt %d, retrying in %.1fs)", err, attempt, currentDelay.Seconds())
				}
				lastLogTime = now
			}

			time.Sleep(currentDelay)

			// Exponential backoff with max cap
			currentDelay = currentDelay * 2
			if currentDelay > maxDelay {
				currentDelay = maxDelay
			}
		}
	}

	// Wait for Redis connection
	waitForRedis(rdb, rctx)

	ordersQueueKey := os.Getenv("ORDERS_QUEUE_KEY")
	orderQueue := NewRedisOrderQueue(rdb, ordersQueueKey)
	zap.S().Infof("✅ Order queue initialized (redis list: %s)", orderQueue.key)

	tradeStream := os.Getenv("TRADE_EVENTS_STREAM")
	eventsHandler := handlersx.NewEventsHandler(rdb, tradeStream)
	zap.S().Infof("✅ Events handler ready (stream: %s)", tradeStream)

	// Snapshot provider for JSON endpoint
	snapshot := gwinternal.NewSnapshotProvider()
	zap.S().Infof("✅ Snapshot provider initialized")

	// Runtime streaming hub (SSE/WS)
	symbol := os.Getenv("SYMBOL")
	if symbol == "" {
		symbol = "XAUUSD"
	}
	sb := runtimex.NewSnapshotBuilder(rdb, symbol)
	intervalMs := 1000
	if v := os.Getenv("SSE_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			intervalMs = n
		}
	}
	limitDOM := 20
	if v := os.Getenv("DOM_LEVELS_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limitDOM = n
		}
	}
	zap.S().Infof("   Initializing Stream Hub (symbol=%s, interval=%dms, DOM limit=%d)", symbol, intervalMs, limitDOM)
	hub := runtimex.NewStreamHub(rdb, sb, time.Duration(intervalMs)*time.Millisecond, limitDOM)
	hub.Start(rctx)
	zap.S().Infof("✅ Stream Hub started")

	// Runtime ATR service (/runtime/atr)
	repo := &runtimex.RedisListCandleRepo{RDB: rdb}
	atrSvc := runtimex.NewATRService(rdb, repo, 14, 30*time.Second)
	zap.S().Infof("✅ ATR Service initialized")

	// Order enqueue handler (shared)
	enqueueHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var cmd OrderCommand
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
			return
		}

		cmd.normalize()

		if cmd.SID == "" {
			http.Error(w, "sid required", http.StatusBadRequest)
			return
		}

		switch cmd.Action {
		case "open":
			if cmd.Symbol == "" || cmd.Side == "" {
				http.Error(w, "symbol and side required for action=open", http.StatusBadRequest)
				return
			}
			if cmd.Lot < 0 {
				cmd.Lot = 0
			}
		case "modify":
			if cmd.Symbol == "" {
				http.Error(w, "symbol required for action=modify", http.StatusBadRequest)
				return
			}
			if cmd.SL == nil && len(cmd.TPLevels) == 0 {
				http.Error(w, "sl or tp_levels required for action=modify", http.StatusBadRequest)
				return
			}
		case "cancel", "resize":
			// sid already validated; additional fields optional
		default:
			http.Error(w, "unsupported action", http.StatusBadRequest)
			return
		}

		queued, err := orderQueue.Enqueue(rctx, cmd)
		if err != nil {
			zap.S().Errorf("❌ Failed to enqueue order command: %v", err)
			http.Error(w, "failed to enqueue command", http.StatusInternalServerError)
			return
		}

		// zap.S().Infof("✅ Order command queued: action=%s sid=%s symbol=%s (queue len=%d)", cmd.Action, cmd.SID, cmd.Symbol, queued)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queued": queued,
			"sid":    cmd.SID,
			"action": cmd.Action,
		})
	}

	// POST /orders/enqueue - Add order to queue
	mux.HandleFunc("/orders/enqueue", enqueueHandler)

	// POST /orders/push - Alias for /orders/enqueue
	mux.HandleFunc("/orders/push", enqueueHandler)

	// Event ingestion endpoints for MT5/bridges
	mux.HandleFunc("/events/publish", eventsHandler.HandlePublishEvent)
	mux.HandleFunc("/events/health", eventsHandler.HandleHealthCheck)

	// Register ATR endpoint
	atrSvc.RegisterHTTPHandlers(mux)

	// GET /runtime/snapshot - aggregated state
	mux.HandleFunc("/runtime/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		s, err := snapshot.BuildSnapshot()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(s)
	})

	// SSE/WS runtime stream (+ alias /runtime/ws)
	mux.HandleFunc("/runtime/stream", hub.SSEHandler)
	mux.HandleFunc("/ws/runtime", hub.WSHandler)
	mux.HandleFunc("/runtime/ws", hub.WSHandler)

	// Account balance endpoint
	type AccountState struct{ Balance float64 }
	accountState := &AccountState{Balance: 10000.0}

	// connect balance into snapshot
	snapshot.BalanceFn = func() float64 { return accountState.Balance }

	// GET /account/balance
	mux.HandleFunc("/account/balance", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"balance": %.2f}`, accountState.Balance)
	})

	// POST /account/updateBalance
	mux.HandleFunc("/account/updateBalance", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		type payload struct {
			Balance float64 `json:"balance"`
		}
		var p payload
		if err := json.NewDecoder(req.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		accountState.Balance = p.Balance
		w.WriteHeader(http.StatusNoContent)
	})

	// GET /orders/poll?symbol=XAUUSD - Poll for orders (MT5 OrderExecutor)
	mux.HandleFunc("/orders/poll", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		symbol := r.URL.Query().Get("symbol")
		if symbol == "" {
			http.Error(w, "symbol required", http.StatusBadRequest)
			return
		}

		cmd, ok, err := orderQueue.Dequeue(rctx, symbol)
		if err != nil {
			zap.S().Errorf("❌ Failed to dequeue order command: %v", err)
			http.Error(w, "failed to dequeue", http.StatusInternalServerError)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// zap.S().Infof("📤 Polled command: action=%s sid=%s symbol=%s", cmd.Action, cmd.SID, cmd.Symbol)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cmd); err != nil {
			zap.S().Errorf("❌ Failed to encode polled command: %v", err)
		}
	})

	// POST /orders/confirm - Receive execution confirmation from MT5
	mux.HandleFunc("/orders/confirm", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}

		// Parse for Execution Quality metrics
		var confirm map[string]any
		if err := json.Unmarshal(body, &confirm); err == nil {
			status := ""
			if v, ok := confirm["status"]; ok && v != nil {
				status = fmt.Sprintf("%v", v)
			}

			if status == "opened" || status == "opened_net" {
				sid := ""
				if v, ok := confirm["sid"]; ok && v != nil {
					sid = fmt.Sprintf("%v", v)
				}

				if sid != "" {
					// Extract SignalPrice and Timestamp from sid (format: {ts}:{side}:{price_normalized})
					parts := strings.Split(sid, ":")
					if len(parts) >= 3 {
						signalTs, errTs := strconv.ParseInt(parts[0], 10, 64)
						priceInt, errPrice := strconv.ParseInt(parts[2], 10, 64)

						if errTs == nil && errPrice == nil {
							signalEntry := float64(priceInt) / 100.0
							side := parts[1]

							// Extract FillPrice
							var fillPrice float64
							if v, ok := confirm["price"]; ok && v != nil {
								if f, ok := v.(float64); ok {
									fillPrice = f
								} else if s, ok := v.(string); ok {
									if parsed, err := strconv.ParseFloat(s, 64); err == nil {
										fillPrice = parsed
									}
								}
							}

							if fillPrice > 0 {
								// Calculate Latency (SignalToFill)
								nowMs := time.Now().UnixMilli()
								latencyMs := nowMs - signalTs
								if latencyMs < 0 {
									latencyMs = 0
								}

								// Calculate Slippage BPS
								// Positive Slippage = cost to trader.
								// LONG: fill > signal = bad (positive slippage)
								// SHORT: fill < signal = bad (positive slippage)
								slippageBps := 0.0
								if side == "LONG" {
									slippageBps = (fillPrice - signalEntry) / signalEntry * 10000.0
								} else if side == "SHORT" {
									slippageBps = (signalEntry - fillPrice) / signalEntry * 10000.0
								}

								// Record metrics
								symbol := "XAUUSD" // Default if not provided
								if sym, ok := confirm["symbol"]; ok && sym != "" {
									symbol = fmt.Sprintf("%v", sym)
								}
								source := "hub"

								if metrics.M != nil {
									metrics.M.SignalToFillLatencyHist.WithLabelValues(source, symbol).Observe(float64(latencyMs))
									metrics.M.ExecutionSlippageHist.WithLabelValues(source, symbol).Observe(slippageBps)
								}

								// SRE Alert for high slippage
								if slippageBps > 5.0 {
									zap.S().Warnf("⚠️ WARNING: High execution slippage detected | sid=%s slippage=%.2fbps fill=%.2f signal=%.2f",
										sid, slippageBps, fillPrice, signalEntry)
								}
							}
						}
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	// POST /notify - Receive OBI event notifications from Python service
	mux.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var p NotifyPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Format timestamp
		t := time.UnixMilli(p.TS).UTC().Format("15:04:05")

		// Emoji based on type
		emoji := "🟢⬆️"
		if p.Type == "obi_sustain_down" {
			emoji = "🔴⬇️"
		}

		// Format message
		msg := fmt.Sprintf(
			"%s <b>%s %s</b>\n\n"+
				"OBI: <code>%.3f</code> (threshold: ±%.2f)\n"+
				"Duration: <b>%dms</b> sustained\n"+
				"Time: %s UTC",
			emoji, p.Symbol, p.Type, p.OBI, p.Threshold, p.DurationMs, t,
		)

		zap.S().Infof("⚡ OBI Event: %s %s OBI=%.3f dur=%dms", p.Symbol, p.Type, p.OBI, p.DurationMs)

		// Send text
		tg.SendText(msg)

		// Send OBI chart
		tg.SendOBIPhoto(p.Symbol, fmt.Sprintf("📊 %s OBI Timeline", p.Symbol))

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	// GET /healthz - Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"service":"scanner-gw"}`))
	})

	// GET /metrics - Prometheus metrics export
	mux.Handle("/metrics", metrics.Handler())

	s := &http.Server{
		Addr:              ":" + port,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	zap.S().Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	zap.S().Infof("🚀 Go Gateway v2.0 Ready")
	zap.S().Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	zap.S().Infof("📡 Server Address: http://0.0.0.0:%s", port)
	zap.S().Infof("🤖 Telegram Bot: %v", tg.bot != nil)
	zap.S().Infof("📊 OBI Service: %s", tg.obiURL)
	zap.S().Infof("📈 Symbol: %s", symbol)
	zap.S().Infof("🔄 Poll Log Interval: Every %d requests", pollLogInterval)
	zap.S().Info()
	zap.S().Info("📊 Available Endpoints:")
	zap.S().Info("   ├─ POST   /orders/enqueue   - Add order to queue")
	zap.S().Info("   ├─ POST   /orders/push      - Add order to queue (alias)")
	zap.S().Info("   ├─ POST   /orders/confirm   - Execution confirmation")
	zap.S().Info("   ├─ POST   /notify           - OBI event notifications")
	zap.S().Info("   ├─ GET    /healthz          - Health check")
	zap.S().Info("   ├─ GET    /metrics          - Prometheus metrics")
	zap.S().Info("   ├─ GET    /runtime/stream   - SSE/WebSocket stream")
	zap.S().Info("   └─ GET    /runtime/atr      - ATR data")
	zap.S().Info()
	zap.S().Infof("✅ All systems initialized successfully")
	zap.S().Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if err := s.ListenAndServe(); err != nil {
		zap.S().Fatalf("❌ Server failed: %v", err)
	}
}

// Request counters for frequent endpoints
var (
	pollCounter     uint64 = 0
	pollLogInterval uint64 = 10000 // Log every 10,000th poll request
	atrCounter      uint64 = 0
	atrLogInterval  uint64 = 10000 // Log every 10,000th ATR request
)

// logMiddleware logs requests with reduced verbosity for frequent endpoints
func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := &bytes.Buffer{}
		tee := io.TeeReader(r.Body, buf)
		body, _ := io.ReadAll(tee)
		r.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))

		// Check if this is a frequent poll request
		if r.Method == "GET" && r.URL.Path == "/orders/poll" {
			count := atomic.AddUint64(&pollCounter, 1)
			if count%pollLogInterval == 0 {
				zap.S().Infof("→ GET /orders/poll [#%d polls]", count)
			}
		} else if r.Method == "GET" && r.URL.Path == "/runtime/atr" {
			// Log every 10,000th ATR request
			count := atomic.AddUint64(&atrCounter, 1)
			if count%atrLogInterval == 0 {
				zap.S().Infof("→ GET /runtime/atr [#%d requests]", count)
			}
		} else {
			// Log all other requests normally
			bodyPreview := truncate(string(body), 512)
			if bodyPreview != "" {
				zap.S().Infof("→ %s %s | body=%s", r.Method, r.URL.String(), bodyPreview)
			} else {
				zap.S().Infof("→ %s %s", r.Method, r.URL.String())
			}
		}

		next.ServeHTTP(w, r)
	})
}

// truncate truncates a string to max length
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// getenvBool reads boolean from env
func getenvBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	}
	return false
}
