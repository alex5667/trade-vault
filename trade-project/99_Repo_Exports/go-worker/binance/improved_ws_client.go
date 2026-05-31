package binance

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/interfaces"

	"github.com/gorilla/websocket"

	"go.uber.org/zap"
)

// convertTimestampSafely безопасно преобразует timestamp из различных типов в int64
func convertTimestampSafely(timestamp interface{}) int64 {
	switch v := timestamp.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		// Если не удалось распарсить, возвращаем текущее время
		return time.Now().UnixMilli()
	default:
		// Для неизвестных типов возвращаем текущее время
		return time.Now().UnixMilli()
	}
}

// Глобальная переменная для доступа к publisher
var globalCandlePublisher interfaces.CandlePublisher

// SetGlobalCandlePublisher устанавливает глобальный publisher для доступа из WebSocket клиента
func SetGlobalCandlePublisher(publisher interfaces.CandlePublisher) {
	globalCandlePublisher = publisher
}

// Глобальный счетчик для отслеживания количества сообщений по символам и таймфреймам
var (
	messageCounters = make(map[string]int)
	counterMutex    sync.RWMutex
)

// Глобальный счетчик для отслеживания количества подключений по символам и таймфреймам
var (
	connectionCounters = make(map[string]int)
	connectionMutex    sync.RWMutex
)

// Глобальный счетчик для отслеживания ошибок WebSocket по символам и таймфреймам
var (
	errorCounters = make(map[string]int)
	errorMutex    sync.RWMutex
)

// Глобальная map для отслеживания времени последнего получения данных по символам и таймфреймам
var (
	lastDataReceived = make(map[string]time.Time)
	lastDataMutex    sync.RWMutex
)

// getStaticCounter возвращает и увеличивает счетчик для конкретного символа и таймфрейма
func getStaticCounter(symbol, timeframe string) int {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	counterMutex.Lock()
	defer counterMutex.Unlock()

	messageCounters[key]++
	return messageCounters[key]
}

// getConnectionCounter возвращает и увеличивает счетчик подключений для конкретного символа и таймфрейма
func getConnectionCounter(symbol, timeframe string) int {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	connectionMutex.Lock()
	defer connectionMutex.Unlock()

	connectionCounters[key]++
	return connectionCounters[key]
}

// getErrorCounter возвращает и увеличивает счетчик ошибок для конкретного символа и таймфрейма
func getErrorCounter(symbol, timeframe string) int {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	errorMutex.Lock()
	defer errorMutex.Unlock()

	errorCounters[key]++
	return errorCounters[key]
}

// getLastDataTime возвращает время последнего получения данных для символа и таймфрейма
func getLastDataTime(symbol, timeframe string) time.Time {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	lastDataMutex.RLock()
	defer lastDataMutex.RUnlock()

	if lastTime, exists := lastDataReceived[key]; exists {
		return lastTime
	}
	return time.Time{} // Нулевое время если данных еще не было
}

// setLastDataTime устанавливает время последнего получения данных для символа и таймфрейма
func setLastDataTime(symbol, timeframe string) {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	lastDataMutex.Lock()
	defer lastDataMutex.Unlock()

	lastDataReceived[key] = time.Now()
}

// calculateDelayForTimeframe возвращает время задержки для следующего подключения в зависимости от таймфрейма
func calculateDelayForTimeframe(timeframe string) time.Duration {
	switch timeframe {
	case "kline_1m":
		return 1 * time.Minute // 1 минута для 1-минутных свечей
	case "kline_5m":
		return 5 * time.Minute // 5 минут для 5-минутных свечей
	case "kline_15m":
		return 15 * time.Minute // 15 минут для 15-минутных свечей
	case "kline_30m":
		return 30 * time.Minute // 30 минут для 30-минутных свечей
	case "kline_1h":
		return 1 * time.Hour // 1 час для часовых свечей
	case "kline_4h":
		return 4 * time.Hour // 4 часа для 4-часовых свечей
	case "kline_1d":
		return 24 * time.Hour // 1 день для дневных свечей
	case "kline_1w":
		return 7 * 24 * time.Hour // 1 неделя для недельных свечей
	case "kline_1M":
		return 30 * 24 * time.Hour // 1 месяц для месячных свечей
	case "kline_3M":
		return 90 * 24 * time.Hour // 3 месяца для квартальных свечей
	case "kline_1y":
		return 365 * 24 * time.Hour // 1 год для годовых свечей
	default:
		return 5 * time.Minute // По умолчанию 5 минут
	}
}

// GetLastDataStats возвращает статистику по времени последнего получения данных для всех символов и таймфреймов
func GetLastDataStats() map[string]interface{} {
	lastDataMutex.RLock()
	defer lastDataMutex.RUnlock()

	stats := make(map[string]interface{})

	for key, lastTime := range lastDataReceived {
		if !lastTime.IsZero() {
			parts := strings.Split(key, "@")
			if len(parts) == 2 {
				symbol := parts[0]
				timeframe := parts[1]

				delay := calculateDelayForTimeframe(timeframe)
				timeSinceLastData := time.Since(lastTime)
				remainingTime := delay - timeSinceLastData

				stats[key] = map[string]interface{}{
					"symbol":            symbol,
					"timeframe":         timeframe,
					"lastDataTime":      lastTime.Format(time.RFC3339),
					"timeSinceLastData": timeSinceLastData.String(),
					"delay":             delay.String(),
					"remainingTime":     remainingTime.String(),
					"canConnect":        remainingTime <= 0,
				}
			}
		}
	}

	return stats
}

// ImprovedWSConfig конфигурация для улучшенного WebSocket клиента
type ImprovedWSConfig struct {
	Symbol       string
	Timeframe    string
	MaxRetries   int
	PingInterval time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// Специальные настройки для разных таймфреймов
	WaitForData    bool          // Ждать ли данные или сразу завершаться
	MaxWaitTime    time.Duration // Максимальное время ожидания данных
	LogAllMessages bool          // Логировать ли все сообщения
}

// DefaultImprovedWSConfig возвращает конфигурацию по умолчанию
func DefaultImprovedWSConfig(symbol, timeframe string) *ImprovedWSConfig {
	return &ImprovedWSConfig{
		Symbol:         symbol,
		Timeframe:      timeframe,
		MaxRetries:     10,
		PingInterval:   20 * time.Second,
		ReadTimeout:    65 * time.Second,
		WriteTimeout:   10 * time.Second,
		WaitForData:    true,
		MaxWaitTime:    5 * time.Minute, // Ждем максимум 5 минут для данных
		LogAllMessages: false,
	}
}

// StartImprovedWS - ОТКЛЮЧЕНО: теперь используется MultiplexedManager
func StartImprovedWS(symbol string, timeframe ...string) {
	// ВСЕ WebSocket соединения теперь управляются через MultiplexedManager
	// Старые отдельные соединения отключены для экономии ресурсов
	zap.S().Infof("📤 StartImprovedWS: символ %s:%v передан в MultiplexedManager", symbol, timeframe)
}

// configureTimeframeSettings настраивает параметры в зависимости от таймфрейма
func configureTimeframeSettings(config *ImprovedWSConfig, timeframe string) {
	switch timeframe {
	case "kline_1m":
		// 1-минутные свечи - данные приходят часто
		config.WaitForData = true
		config.MaxWaitTime = 2 * time.Minute
		config.LogAllMessages = false

	case "kline_5m", "kline_15m", "kline_30m":
		// 5-15-30 минутные свечи - данные приходят реже
		config.WaitForData = true
		config.MaxWaitTime = 10 * time.Minute
		config.LogAllMessages = true

	case "kline_1h", "kline_4h":
		// Часовые свечи - данные приходят очень редко
		config.WaitForData = true
		config.MaxWaitTime = 1 * time.Hour
		config.LogAllMessages = true

	case "kline_1d", "kline_1w", "kline_1M":
		// Дневные и более - данные приходят крайне редко
		config.WaitForData = false // Не ждем данные
		config.MaxWaitTime = 0
		config.LogAllMessages = true

	case "kline_3M", "kline_1y":
		// Квартальные и годовые свечи - данные приходят очень редко
		config.WaitForData = false // Не ждем данные
		config.MaxWaitTime = 0
		config.LogAllMessages = true

	default:
		// По умолчанию
		config.WaitForData = true
		config.MaxWaitTime = 5 * time.Minute
		config.LogAllMessages = false
	}
}

// runImprovedWebSocket основной цикл улучшенного WebSocket
func runImprovedWebSocket(config *ImprovedWSConfig, url string) {
	retryCount := 0

	for {
		// Проверяем, нужно ли ждать перед следующим подключением
		lastDataTime := getLastDataTime(config.Symbol, config.Timeframe)
		if !lastDataTime.IsZero() {
			delay := calculateDelayForTimeframe(config.Timeframe)
			timeSinceLastData := time.Since(lastDataTime)

			if timeSinceLastData < delay {
				remainingTime := delay - timeSinceLastData
				// Показываем сообщение об ожидании только каждые 1000 раз
				connectionCount := getConnectionCounter(config.Symbol, config.Timeframe)
				if connectionCount%1000 == 0 {
					zap.S().Infof("⏳ %s@%s: ожидание %v перед следующим подключением (прошло %v из %v)",
						config.Symbol, config.Timeframe, remainingTime, timeSinceLastData, delay)
				}
				time.Sleep(remainingTime)
			}
		}

		if err := connectAndHandleImprovedWS(config, url); err != nil {
			retryCount++
			errorCount := getErrorCounter(config.Symbol, config.Timeframe)

			if retryCount >= config.MaxRetries {
				// Выводим только каждую 10000-ю ошибку о превышении попыток
				if errorCount%10000 == 0 {
					zap.S().Errorf("❌ Превышено максимальное количество попыток для %s@%s: %v (всего ошибок: %d)",
						config.Symbol, config.Timeframe, err, errorCount)
				}
				return
			}

			backoffTime := time.Duration(retryCount) * 5 * time.Second
			if backoffTime > 60*time.Second {
				backoffTime = 60 * time.Second
			}

			// Выводим только каждую 10000-ю ошибку
			if errorCount%10000 == 0 {
				zap.S().Errorf("⚠️ Ошибка WebSocket для %s@%s (попытка %d/%d, всего ошибок: %d): %v",
					config.Symbol, config.Timeframe, retryCount, config.MaxRetries, errorCount, err)
				zap.S().Infof("⏳ Повторная попытка через %v...", backoffTime)
			}

			time.Sleep(backoffTime)
			continue
		}

		// Сбрасываем счетчик при успешном подключении
		retryCount = 0

		// Показываем сообщение о переподключении только каждые 1000 раз
		// Используем тот же счетчик подключений для консистентности
		connectionCount := getConnectionCounter(config.Symbol, config.Timeframe)
		if connectionCount%1000 == 0 {
			zap.S().Infof("🔄 Переподключение к WebSocket для %s@%s", config.Symbol, config.Timeframe)
		}
		time.Sleep(5 * time.Second)
	}
}

// connectAndHandleImprovedWS устанавливает соединение и обрабатывает сообщения
func connectAndHandleImprovedWS(config *ImprovedWSConfig, url string) error {
	// Увеличиваем счетчик подключений при каждой попытке
	_ = getConnectionCounter(config.Symbol, config.Timeframe)

	// Устанавливаем соединение
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("ошибка подключения: %v", err)
	}
	defer conn.Close()

	// Показываем сообщение о подключении только каждые 1000 раз
	// Закомментировано для уменьшения шума в логах
	// if connectionCount%1000 == 0 {
	// 	zap.S().Infof("✅ WebSocket соединение установлено для %s@%s", config.Symbol, config.Timeframe)
	// }

	// Настраиваем таймауты
	conn.SetReadDeadline(time.Now().Add(config.ReadTimeout))
	conn.SetWriteDeadline(time.Now().Add(config.WriteTimeout))

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Обработчик ping от Binance - отвечаем pong
	// Binance отправляет ping, и мы должны отвечать pong, иначе соединение будет закрыто
	conn.SetPingHandler(func(appData string) error {
		// Сбрасываем таймаут чтения при получении ping
		conn.SetReadDeadline(time.Now().Add(config.ReadTimeout))
		// Отправляем pong в ответ на ping от Binance
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(config.WriteTimeout))
	})

	// Настраиваем обработчик pong (когда мы отправляем ping и получаем pong)
	var lastPong time.Time = time.Now()
	conn.SetPongHandler(func(appData string) error {
		lastPong = time.Now()
		conn.SetReadDeadline(time.Now().Add(config.ReadTimeout))
		return nil
	})

	// Таймер для ping
	pingTicker := time.NewTicker(config.PingInterval)
	defer pingTicker.Stop()

	// Таймер для статистики
	statsTicker := time.NewTicker(2 * time.Minute)
	defer statsTicker.Stop()

	// Счетчики
	var totalMessages int64 = 0
	var closedCandles int64 = 0

	// Канал для завершения чтения
	readDone := make(chan struct{})

	// Горутина для чтения сообщений
	go func() {
		defer close(readDone)

		startTime := time.Now()

		for {
			// Проверяем, не превышено ли время ожидания
			if config.WaitForData && time.Since(startTime) > config.MaxWaitTime {
				zap.S().Infof("⏰ Превышено время ожидания данных для %s@%s (%v)",
					config.Symbol, config.Timeframe, config.MaxWaitTime)
				return
			}

			_, msg, err := conn.ReadMessage()
			if err != nil {
				zap.S().Errorf("❌ Ошибка чтения для %s@%s: %v", config.Symbol, config.Timeframe, err)
				return
			}

			totalMessages++

			// Логируем каждое 300-е общее сообщение
			if totalMessages%300 == 0 {
				zap.S().Infof("📨 %s@%s: получено сообщений #%d", config.Symbol, config.Timeframe, totalMessages)
			}

			// Обрабатываем сообщение
			if err := processImprovedWSMessage(config, msg); err != nil {
				zap.S().Errorf("⚠️ Ошибка обработки сообщения для %s@%s: %v",
					config.Symbol, config.Timeframe, err)
				continue
			}

			closedCandles++

			// Логируем только каждую 300-ю закрытую свечу
			if closedCandles%300 == 0 {
				zap.S().Infof("📊 %s@%s: закрытая свеча #%d", config.Symbol, config.Timeframe, closedCandles)
			}
		}
	}()

	// Основной цикл
	for {
		select {
		case <-readDone:
			// Проверяем, получили ли мы хоть одно сообщение
			if totalMessages == 0 && config.WaitForData {
				zap.S().Warnf("⚠️ %s@%s: не получено ни одного сообщения",
					config.Symbol, config.Timeframe)
			}
			return fmt.Errorf("чтение сообщений завершено")

		case <-pingTicker.C:
			// Проверяем pong
			if time.Since(lastPong) > 60*time.Second {
				return fmt.Errorf("нет pong ответа более 60 секунд")
			}

			// Отправляем ping
			conn.SetWriteDeadline(time.Now().Add(config.WriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return fmt.Errorf("ошибка отправки ping: %v", err)
			}

		case <-statsTicker.C:
			// Логируем статистику каждые 2 минуты
			if totalMessages > 0 {
				zap.S().Infof("📈 %s@%s: всего сообщений: %d, закрытых свечей: %d",
					config.Symbol, config.Timeframe, totalMessages, closedCandles)
			} else {
				// Выводим сообщение "ожидание данных..." только каждые 1000 раз
				static := getStaticCounter(config.Symbol, config.Timeframe)
				if static%1000 == 0 {
					zap.S().Infof("⏳ %s@%s: ожидание данных...",
						config.Symbol, config.Timeframe)
				}
			}

			// Проверяем Redis
			if err := redisclient.Client.Ping(redisclient.Ctx).Err(); err != nil {
				zap.S().Errorf("⚠️ Redis недоступен для %s@%s: %v", config.Symbol, config.Timeframe, err)
			}
		}
	}
}

// processImprovedWSMessage обрабатывает сообщение от улучшенного WebSocket
func processImprovedWSMessage(config *ImprovedWSConfig, msg []byte) error {
	// Парсим JSON
	var payload map[string]interface{}
	if err := json.Unmarshal(msg, &payload); err != nil {
		return fmt.Errorf("ошибка парсинга JSON: %v", err)
	}

	// Логируем все сообщения если включено
	if config.LogAllMessages {
		zap.S().Infof("📨 %s@%s: получено сообщение: %s", config.Symbol, config.Timeframe, string(msg))
	}

	// Проверяем наличие поля kline
	klineData, hasKline := payload["k"].(map[string]interface{})
	if !hasKline {
		return nil // Не kline сообщение, пропускаем
	}

	// Проверяем, закрыта ли свеча (поле 'x' = true)
	isClosed, hasX := klineData["x"].(bool)

	// Добавляем отладочное логирование для поля x только каждые 300 сообщений
	static := getStaticCounter(config.Symbol, config.Timeframe)
	if static%300 == 0 {
		if hasX {
			zap.S().Infof("🔍 %s@%s: поле x = %v (тип: %T)", config.Symbol, config.Timeframe, isClosed, isClosed)
		} else {
			zap.S().Infof("🔍 %s@%s: поле x отсутствует", config.Symbol, config.Timeframe)
			// Проверяем все доступные поля в kline
			for key, value := range klineData {
				zap.S().Infof("🔍 %s@%s: kline[%s] = %v (тип: %T)", config.Symbol, config.Timeframe, key, value, value)
			}
		}
	}

	if !hasX || !isClosed {
		return nil // Свеча не закрыта, пропускаем
	}

	// Свеча закрыта - публикуем в Redis
	return publishImprovedWSToRedis(config, msg, klineData)
}

// publishImprovedWSToRedis публикует данные в единый Redis Stream candles:data
func publishImprovedWSToRedis(config *ImprovedWSConfig, msg []byte, klineData map[string]interface{}) error {
	// Публикуем только через CandlePublisher в единый stream candles:data
	if globalCandlePublisher != nil {
		// Формируем данные свечи для единого stream
		candleData := map[string]interface{}{
			"openTime":            convertTimestampSafely(klineData["t"]), // Время открытия свечи
			"closeTime":           convertTimestampSafely(klineData["T"]), // Время закрытия свечи
			"open":                klineData["o"],
			"high":                klineData["h"],
			"low":                 klineData["l"],
			"close":               klineData["c"],
			"volume":              klineData["v"],
			"trades":              klineData["n"],
			"quoteVolume":         klineData["q"],
			"takerBuyVolume":      klineData["V"],
			"takerBuyQuoteVolume": klineData["Q"],
			"isClosed":            klineData["x"],
		}

		// Добавляем дополнительные поля если они доступны
		if klineData["f"] != nil {
			candleData["firstTradeId"] = klineData["f"]
		}
		if klineData["L"] != nil {
			candleData["lastTradeId"] = klineData["L"]
		}

		// Нормализуем таймфрейм: убираем префикс "kline_" если есть
		tf := config.Timeframe
		if strings.HasPrefix(tf, "kline_") {
			tf = strings.TrimPrefix(tf, "kline_")
		}

		// Публикуем через интерфейс в единый stream candles:data
		if err := globalCandlePublisher.PublishCandleData(redisclient.Ctx, config.Symbol, tf, candleData); err != nil {
			zap.S().Errorf("⚠️ Ошибка публикации в candles:data: %v", err)
			return fmt.Errorf("ошибка публикации в candles:data: %v", err)
		}
	} else {
		return fmt.Errorf("CandlePublisher не инициализирован")
	}

	// Обновляем время последнего получения данных при успешной публикации
	setLastDataTime(config.Symbol, config.Timeframe)

	// Логируем только каждую 300-ю отправленную свечу
	static := getStaticCounter(config.Symbol, config.Timeframe)
	if static%300 == 0 {
		zap.S().Infof("📤 Закрытая свеча %s@%s отправлена в candles:data",
			config.Symbol, config.Timeframe)
		zap.S().Infof("⏰ %s@%s: время последнего получения данных обновлено",
			config.Symbol, config.Timeframe)
	}

	return nil
}
