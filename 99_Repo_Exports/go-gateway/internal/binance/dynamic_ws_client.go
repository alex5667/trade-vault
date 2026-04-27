package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// DynamicWSClient - WebSocket клиент с динамической подпиской/отпиской
type DynamicWSClient struct {
	baseURL           string
	conn              *websocket.Conn
	subscribedSymbols map[string]bool
	mu                sync.RWMutex

	// Redis для команд и публикации
	redisClient   *redis.Client
	commandStream string
	consumerGroup string

	// Управление подписками
	subscriptionID  int
	pendingCommands chan *SymbolCommand
	rateLimiter     *time.Ticker

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// SymbolCommand - команда управления символами
type SymbolCommand struct {
	Action  string   `json:"action"` // add, remove, set
	Symbols []string `json:"symbols"`
	Source  string   `json:"source"`
	TS      int64    `json:"ts"`
}

// SubscriptionRequest - запрос подписки для Binance WS
type SubscriptionRequest struct {
	Method string   `json:"method"` // SUBSCRIBE или UNSUBSCRIBE
	Params []string `json:"params"` // ["btcusdt@kline_1m", ...]
	ID     int      `json:"id"`
}

// SubscriptionResponse - ответ от Binance
type SubscriptionResponse struct {
	Result interface{} `json:"result"`
	ID     int         `json:"id"`
	Error  *struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error,omitempty"`
}

// NewDynamicWSClient создает новый динамический WS клиент
func NewDynamicWSClient(
	baseURL string,
	redisClient *redis.Client,
	commandStream string,
	logger *zap.Logger,
) *DynamicWSClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &DynamicWSClient{
		baseURL:           baseURL,
		subscribedSymbols: make(map[string]bool),
		redisClient:       redisClient,
		commandStream:     commandStream,
		consumerGroup:     "ws-dynamic-group",
		subscriptionID:    1,
		pendingCommands:   make(chan *SymbolCommand, 100),
		rateLimiter:       time.NewTicker(200 * time.Millisecond), // 5 команд/сек max
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Start запускает клиент
func (c *DynamicWSClient) Start(initialSymbols []string) error {
	c.logger.Info("Starting Dynamic WebSocket Client",
		zap.Strings("initial_symbols", initialSymbols))

	// Подключаемся к WebSocket
	if err := c.connect(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Создаем consumer group для команд
	if err := c.createConsumerGroup(); err != nil {
		c.logger.Warn("Consumer group already exists or error",
			zap.Error(err))
	}

	// Подписываемся на начальные символы
	if len(initialSymbols) > 0 {
		if err := c.subscribeSymbols(initialSymbols); err != nil {
			c.logger.Error("Failed to subscribe initial symbols",
				zap.Error(err))
		}
	}

	// Запускаем горутины
	go c.readMessages()
	go c.processCommands()
	go c.watchCommandStream()
	go c.heartbeat()

	c.logger.Info("Dynamic WebSocket Client started successfully")
	return nil
}

// connect устанавливает WS соединение
func (c *DynamicWSClient) connect() error {
	c.logger.Info("Connecting to WebSocket", zap.String("url", c.baseURL))

	conn, _, err := websocket.DefaultDialer.Dial(c.baseURL, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.logger.Info("WebSocket connected successfully")
	return nil
}

// createConsumerGroup создает consumer group для Redis stream
func (c *DynamicWSClient) createConsumerGroup() error {
	return c.redisClient.XGroupCreateMkStream(
		c.ctx,
		c.commandStream,
		c.consumerGroup,
		"$",
	).Err()
}

// subscribeSymbols подписывается на список символов
func (c *DynamicWSClient) subscribeSymbols(symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Формируем список streams для подписки
	var streams []string
	for _, symbol := range symbols {
		// Преобразуем BTCUSD → btcusdt
		binanceSymbol := symbolToBinanceFormat(symbol)
		stream := fmt.Sprintf("%s@kline_1m", binanceSymbol)

		if !c.subscribedSymbols[symbol] {
			streams = append(streams, stream)
			c.subscribedSymbols[symbol] = true
		}
	}

	if len(streams) == 0 {
		return nil // Все уже подписаны
	}

	// Отправляем SUBSCRIBE запрос
	req := SubscriptionRequest{
		Method: "SUBSCRIBE",
		Params: streams,
		ID:     c.subscriptionID,
	}
	c.subscriptionID++

	if err := c.conn.WriteJSON(req); err != nil {
		// Откатываем подписку при ошибке
		for _, symbol := range symbols {
			delete(c.subscribedSymbols, symbol)
		}
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	c.logger.Info("Subscribed to symbols",
		zap.Strings("symbols", symbols),
		zap.Strings("streams", streams))

	return nil
}

// unsubscribeSymbols отписывается от символов
func (c *DynamicWSClient) unsubscribeSymbols(symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var streams []string
	for _, symbol := range symbols {
		if c.subscribedSymbols[symbol] {
			binanceSymbol := symbolToBinanceFormat(symbol)
			stream := fmt.Sprintf("%s@kline_1m", binanceSymbol)
			streams = append(streams, stream)
			delete(c.subscribedSymbols, symbol)
		}
	}

	if len(streams) == 0 {
		return nil
	}

	req := SubscriptionRequest{
		Method: "UNSUBSCRIBE",
		Params: streams,
		ID:     c.subscriptionID,
	}
	c.subscriptionID++

	if err := c.conn.WriteJSON(req); err != nil {
		return fmt.Errorf("failed to send UNSUBSCRIBE: %w", err)
	}

	c.logger.Info("Unsubscribed from symbols",
		zap.Strings("symbols", symbols),
		zap.Strings("streams", streams))

	return nil
}

// watchCommandStream следит за командами в Redis stream
func (c *DynamicWSClient) watchCommandStream() {
	c.logger.Info("Starting command stream watcher",
		zap.String("stream", c.commandStream))

	consumerName := fmt.Sprintf("ws-client-%d", time.Now().Unix())

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Читаем команды из stream
		streams, err := c.redisClient.XReadGroup(c.ctx, &redis.XReadGroupArgs{
			Group:    c.consumerGroup,
			Consumer: consumerName,
			Streams:  []string{c.commandStream, ">"},
			Count:    10,
			Block:    1 * time.Second,
		}).Result()

		if err != nil {
			if err != redis.Nil {
				c.logger.Error("Error reading command stream", zap.Error(err))
			}
			continue
		}

		// Обрабатываем команды
		for _, stream := range streams {
			for _, message := range stream.Messages {
				c.handleCommand(message)

				// ACK сообщение
				c.redisClient.XAck(c.ctx, c.commandStream, c.consumerGroup, message.ID)
			}
		}
	}
}

// handleCommand обрабатывает команду из stream
func (c *DynamicWSClient) handleCommand(message redis.XMessage) {
	dataStr, ok := message.Values["data"].(string)
	if !ok {
		c.logger.Warn("Invalid command format", zap.Any("values", message.Values))
		return
	}

	var cmd SymbolCommand
	if err := json.Unmarshal([]byte(dataStr), &cmd); err != nil {
		c.logger.Error("Failed to parse command", zap.Error(err))
		return
	}

	c.logger.Info("Received command",
		zap.String("action", cmd.Action),
		zap.Strings("symbols", cmd.Symbols))

	// Отправляем в очередь команд (rate limited)
	select {
	case c.pendingCommands <- &cmd:
	case <-time.After(5 * time.Second):
		c.logger.Warn("Command queue full, dropping command")
	}
}

// processCommands обрабатывает команды с rate limiting
func (c *DynamicWSClient) processCommands() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case <-c.rateLimiter.C:
			// Можем обработать одну команду (rate limit)
			select {
			case cmd := <-c.pendingCommands:
				c.executeCommand(cmd)
			default:
				// Нет команд в очереди
			}
		}
	}
}

// executeCommand выполняет команду
func (c *DynamicWSClient) executeCommand(cmd *SymbolCommand) {
	switch cmd.Action {
	case "add":
		if err := c.subscribeSymbols(cmd.Symbols); err != nil {
			c.logger.Error("Failed to add symbols", zap.Error(err))
		}

	case "remove":
		if err := c.unsubscribeSymbols(cmd.Symbols); err != nil {
			c.logger.Error("Failed to remove symbols", zap.Error(err))
		}

	case "set":
		// Получаем текущий список
		c.mu.RLock()
		current := make([]string, 0, len(c.subscribedSymbols))
		for symbol := range c.subscribedSymbols {
			current = append(current, symbol)
		}
		c.mu.RUnlock()

		// Находим символы для удаления и добавления
		toRemove := difference(current, cmd.Symbols)
		toAdd := difference(cmd.Symbols, current)

		if err := c.unsubscribeSymbols(toRemove); err != nil {
			c.logger.Error("Failed to unsubscribe symbols", zap.Error(err))
		}

		if err := c.subscribeSymbols(toAdd); err != nil {
			c.logger.Error("Failed to subscribe symbols", zap.Error(err))
		}

	default:
		c.logger.Warn("Unknown command action", zap.String("action", cmd.Action))
	}

	// Сохраняем текущий список в Redis
	c.saveCurrentSymbols()
}

// readMessages читает сообщения от WebSocket
func (c *DynamicWSClient) readMessages() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			c.logger.Error("WebSocket read error", zap.Error(err))
			c.reconnect()
			continue
		}

		// Обрабатываем сообщение
		c.handleMessage(message)
	}
}

// handleMessage обрабатывает входящее сообщение
func (c *DynamicWSClient) handleMessage(data []byte) {
	// Проверяем, это ответ на подписку или данные
	var subResp SubscriptionResponse
	if err := json.Unmarshal(data, &subResp); err == nil && subResp.ID > 0 {
		// Это ответ на SUBSCRIBE/UNSUBSCRIBE
		if subResp.Error != nil {
			c.logger.Error("Subscription error",
				zap.Int("code", subResp.Error.Code),
				zap.String("msg", subResp.Error.Msg))
		} else {
			c.logger.Debug("Subscription confirmed", zap.Int("id", subResp.ID))
		}
		return
	}

	// Это данные kline - публикуем в Redis
	// TODO: Парсинг kline данных и публикация в stream
	// Формат: stream:tick_{SYMBOL}
	c.publishToRedis(data)
}

// publishToRedis публикует данные в Redis stream
func (c *DynamicWSClient) publishToRedis(data []byte) {
	// TODO: Реализовать парсинг и публикацию
	// Пример: извлечь symbol из данных и опубликовать в stream:tick_{SYMBOL}
}

// saveCurrentSymbols сохраняет текущий список символов в Redis
func (c *DynamicWSClient) saveCurrentSymbols() {
	c.mu.RLock()
	symbols := make([]string, 0, len(c.subscribedSymbols))
	for symbol := range c.subscribedSymbols {
		symbols = append(symbols, symbol)
	}
	c.mu.RUnlock()

	data, _ := json.Marshal(symbols)
	c.redisClient.Set(c.ctx, "config:symbols:current", string(data), 0)
}

// reconnect переподключается к WebSocket
func (c *DynamicWSClient) reconnect() {
	c.logger.Warn("Reconnecting to WebSocket...")

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	// Exponential backoff
	for attempt := 1; attempt <= 10; attempt++ {
		if err := c.connect(); err != nil {
			c.logger.Error("Reconnect failed",
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Восстанавливаем подписки
		c.mu.RLock()
		symbols := make([]string, 0, len(c.subscribedSymbols))
		for symbol := range c.subscribedSymbols {
			symbols = append(symbols, symbol)
			// Очищаем, чтобы subscribeSymbols заново подписал
			delete(c.subscribedSymbols, symbol)
		}
		c.mu.RUnlock()

		if err := c.subscribeSymbols(symbols); err != nil {
			c.logger.Error("Failed to restore subscriptions", zap.Error(err))
		}

		c.logger.Info("Reconnected successfully")
		return
	}

	c.logger.Fatal("Failed to reconnect after 10 attempts")
}

// heartbeat отправляет ping для поддержания соединения
func (c *DynamicWSClient) heartbeat() {
	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn != nil {
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					c.logger.Error("Ping failed", zap.Error(err))
				}
			}
		}
	}
}

// Stop останавливает клиент
func (c *DynamicWSClient) Stop() {
	c.logger.Info("Stopping Dynamic WebSocket Client")

	c.cancel()
	c.rateLimiter.Stop()

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	c.logger.Info("Dynamic WebSocket Client stopped")
}

// GetSubscribedSymbols возвращает список подписанных символов
func (c *DynamicWSClient) GetSubscribedSymbols() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	symbols := make([]string, 0, len(c.subscribedSymbols))
	for symbol := range c.subscribedSymbols {
		symbols = append(symbols, symbol)
	}
	return symbols
}

// Utility functions

func symbolToBinanceFormat(symbol string) string {
	// BTCUSD → btcusdt
	// XAUUSD → xauusdt (для крипты - может не существовать)
	// Для Binance нужен формат: btcusdt, ethusdt и т.д.
	return symbolToLower(symbol) + "t"
}

func symbolToLower(s string) string {
	return string([]rune(s[:])) // Simplified - implement proper lowercasing
}

func difference(a, b []string) []string {
	mb := make(map[string]bool, len(b))
	for _, x := range b {
		mb[x] = true
	}

	var diff []string
	for _, x := range a {
		if !mb[x] {
			diff = append(diff, x)
		}
	}
	return diff
}
