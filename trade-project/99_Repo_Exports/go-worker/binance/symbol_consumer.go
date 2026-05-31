package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"

	"go-worker/internal/interfaces"

	"go-worker/internal/monitoring"

	"go-worker/pkg/timeutil"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// Счетчики для уменьшения логов в symbol_consumer
var symbolConsumerUpdateErrorCounter uint64
var symbolConsumerMessageCounter uint64   // Счетчик полученных сообщений от бэкенда
var symbolConsumerProcessCounter uint64   // Счетчик обработанных символов
var symbolConsumerCopyCounter uint64      // Счетчик скопированных символов
var symbolConsumerPublishCounter uint64   // Счетчик опубликованных статусов
var symbolConsumerNewSymbolCounter uint64 // Счетчик новых символов

// SymbolData представляет данные о символе и его таймфреймах
type SymbolData struct {
	Symbol     string      `json:"symbol"`
	Timeframes []Timeframe `json:"timeframes"`
}

// SymbolConsumer потребляет данные о символах и таймфреймах из Redis
// и запускает WebSocket соединения для каждого сочетания
type SymbolConsumer struct {
	client *redis.Client
	ctx    context.Context
	cancel context.CancelFunc

	// Активные соединения для отслеживания
	activeConnections map[string]bool // key: "symbol:timeframe"
	connectionsMutex  sync.RWMutex

	// Канал для остановки
	stopChan chan struct{}
	wg       sync.WaitGroup

	// Интервал проверки новых символов
	checkInterval time.Duration

	// Счетчик для ограничения частоты логирования
	startupCounter int
	startupMutex   sync.RWMutex

	// Счетчик обработанных символов
	symbolProcessCounter int64

	// Callback для публикации данных свечей
	candlePublisher interfaces.CandlePublisher

	// Дополнение символов через Binance API
	symbolSupplementer *SymbolSupplementer

	// Multiplexed WebSocket менеджер
	multiplexedManager *MultiplexedManager

	// Whitelist символов из REQUIRED_SYMBOLS (lowercase). Если непустой — принимаем только их.
	requiredSymbols map[string]struct{}
}

var validSymbolRegex = regexp.MustCompile("^[A-Z0-9_\\-]+$")

// NewSymbolConsumer создает новый экземпляр потребителя символов
func NewSymbolConsumer() *SymbolConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	// Строим whitelist из REQUIRED_SYMBOLS (если задан)
	requiredSymbols := make(map[string]struct{})
	if envSymbols := os.Getenv("REQUIRED_SYMBOLS"); envSymbols != "" {
		var invalidSymbols []string
		for _, sym := range strings.Split(envSymbols, ",") {
			s := strings.TrimSpace(sym)
			if s != "" {
				sUpper := strings.ToUpper(s)
				if validSymbolRegex.MatchString(sUpper) {
					requiredSymbols[strings.ToLower(s)] = struct{}{}
				} else {
					invalidSymbols = append(invalidSymbols, s)
				}
			}
		}
		if len(invalidSymbols) > 0 {
			zap.S().Fatalf("❌ SymbolConsumer: обнаружены невалидные символы в REQUIRED_SYMBOLS: %v. Разрешены только [A-Z0-9_\\-].", invalidSymbols)
		}
		zap.S().Infof("🔒 SymbolConsumer: whitelist из %d символов (REQUIRED_SYMBOLS)", len(requiredSymbols))
	}

	sc := &SymbolConsumer{
		client:               redisclient.Client,
		ctx:                  ctx,
		cancel:               cancel,
		activeConnections:    make(map[string]bool),
		connectionsMutex:     sync.RWMutex{},
		stopChan:             make(chan struct{}),
		checkInterval:        30 * time.Second, // Проверяем каждые 30 секунд
		startupCounter:       0,
		startupMutex:         sync.RWMutex{},
		symbolProcessCounter: 0,
		candlePublisher:      nil, // Инициализируем nil, так как Publisher еще не создан
		symbolSupplementer:   NewSymbolSupplementer(redisclient.Client, ctx),
		multiplexedManager:   nil, // Будет создан в SetCandlePublisher
		requiredSymbols:      requiredSymbols,
	}

	// Очищаем map активных подключений при старте
	sc.connectionsMutex.Lock()
	sc.activeConnections = make(map[string]bool)
	sc.connectionsMutex.Unlock()

	// Запускаем мониторинг символов с бэкенда через стрим (6380)
	sc.wg.Add(1)
	go sc.monitorBackendSymbolsStream()

	return sc
}

// SetCandlePublisher устанавливает publisher для данных свечей
func (sc *SymbolConsumer) SetCandlePublisher(publisher interfaces.CandlePublisher) {
	sc.candlePublisher = publisher

	// Также устанавливаем для MultiplexedManager
	if sc.multiplexedManager != nil {
		// Обновляем publisher без пересоздания менеджера и разрыва соединений.
		// Старый вызов Stop() не дожидался завершения горутин, что вызывало утечки.
		sc.multiplexedManager.candlePublisher = publisher
		
		sc.multiplexedManager.connMutex.RLock()
		for _, conn := range sc.multiplexedManager.connections {
			conn.mutex.Lock()
			conn.candlePublisher = publisher
			conn.mutex.Unlock()
		}
		sc.multiplexedManager.connMutex.RUnlock()
	} else {
		// Создаем MultiplexedManager с правильным Redis клиентом
		sc.multiplexedManager = NewMultiplexedManager(sc.client, publisher)
	}

	zap.S().Infof("✅ CandlePublisher установлен для SymbolConsumer и MultiplexedManager")
}

// SetMonitor устанавливает монитор для MultiplexedManager
func (sc *SymbolConsumer) SetMonitor(monitor *monitoring.WebSocketMonitor) {
	if sc.multiplexedManager != nil {
		sc.multiplexedManager.SetMonitor(monitor)
		zap.S().Infof("✅ Монитор установлен для MultiplexedManager")
	}
}

// getStartupCounter возвращает и увеличивает счетчик запусков
func (sc *SymbolConsumer) getStartupCounter() int {
	sc.startupMutex.Lock()
	defer sc.startupMutex.Unlock()

	sc.startupCounter++
	return sc.startupCounter
}

// Start запускает потребление символов из Redis
func (sc *SymbolConsumer) Start() error {
	zap.S().Infof("🚀 Запуск SymbolConsumer для канала symbol:details")

	// Очищаем map активных соединений при старте
	sc.connectionsMutex.Lock()
	sc.activeConnections = make(map[string]bool)
	sc.connectionsMutex.Unlock()
	zap.S().Infof("🧹 Очищен map активных соединений")

	// Запускаем MultiplexedManager для WebSocket соединений
	if sc.multiplexedManager != nil {
		if err := sc.multiplexedManager.Start(); err != nil {
			zap.S().Errorf("❌ Ошибка запуска MultiplexedManager: %v", err)
		} else {
			zap.S().Infof("✅ MultiplexedManager запущен успешно")
		}
	}

	// Запускаем горутину для мониторинга символов
	sc.wg.Add(1)
	go sc.monitorSymbols()

	return nil
}

// Stop останавливает потребитель и закрывает все активные соединения
func (sc *SymbolConsumer) Stop() {
	zap.S().Infof("🛑 Остановка SymbolConsumer")

	// Останавливаем MultiplexedManager
	if sc.multiplexedManager != nil {
		sc.multiplexedManager.Stop()
		zap.S().Infof("✅ MultiplexedManager остановлен")
	}

	// Сигнализируем остановку
	close(sc.stopChan)
	sc.cancel()

	// Ждем завершения горутины
	sc.wg.Wait()

	zap.S().Infof("✅ SymbolConsumer остановлен")
}

// monitorSymbols основной цикл мониторинга символов из Redis
func (sc *SymbolConsumer) monitorSymbols() {
	defer sc.wg.Done()

	zap.S().Infof("🔄 Запуск мониторинга символов из канала symbol:details")

	ticker := time.NewTicker(sc.checkInterval)
	defer ticker.Stop()

	// Обрабатываем существующие символы при запуске
	if err := sc.processExistingSymbols(); err != nil {
		zap.S().Errorf("⚠️ Ошибка обработки существующих символов: %v", err)
	}

	// При запуске проверяем и дополняем символы если нужно
	if sc.symbolSupplementer != nil {
		if err := sc.symbolSupplementer.SupplementSymbolsFromBinanceAPI(); err != nil {
			zap.S().Errorf("⚠️ Ошибка дополнения символов при запуске: %v", err)
		}
	}

	for {
		select {
		case <-sc.ctx.Done():
			zap.S().Infof("🛑 Остановка мониторинга символов")
			return
		case <-sc.stopChan:
			zap.S().Infof("🛑 Получен сигнал остановки")
			return
		case <-ticker.C:
			// Периодически проверяем и дополняем символы если нужно
			if sc.symbolSupplementer != nil {
				if err := sc.symbolSupplementer.SupplementSymbolsFromBinanceAPI(); err != nil {
					zap.S().Errorf("⚠️ Ошибка дополнения символов: %v", err)
				}
			}

			// Обрабатываем новые символы
			if err := sc.processExistingSymbols(); err != nil {
				zap.S().Errorf("⚠️ Ошибка обработки символов: %v", err)
			}
		}
	}
}

// processExistingSymbols обрабатывает все существующие символы из Redis
func (sc *SymbolConsumer) processExistingSymbols() error {
	// Сканируем все ключи symbol:details:*
	var cursor uint64
	var err error

	for {
		var keys []string
		keys, cursor, err = sc.client.Scan(sc.ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			return fmt.Errorf("ошибка сканирования ключей: %v", err)
		}

		// Обрабатываем найденные ключи
		for _, key := range keys {
			if err := sc.processSymbolKey(key); err != nil {
				zap.S().Errorf("⚠️ Ошибка обработки ключа %s: %v", key, err)
				continue
			}
		}

		// Если курсор вернулся к 0, значит все ключи обработаны
		if cursor == 0 {
			break
		}
	}

	return nil
}

// processSymbolKey обрабатывает один ключ символа
func (sc *SymbolConsumer) processSymbolKey(key string) error {
	// Увеличиваем счетчик обработанных символов
	sc.symbolProcessCounter++

	// Выводим каждое 10000-е сообщение
	if sc.symbolProcessCounter%10000 == 0 {
		zap.S().Infof("🔍 Обработка ключа символа: %s (всего: %d)", key, sc.symbolProcessCounter)
	}

	// Получаем данные символа из Redis
	symbolData, err := sc.getSymbolData(key)
	if err != nil {
		return fmt.Errorf("ошибка получения данных символа: %v", err)
	}

	if symbolData == nil {
		zap.S().Warnf("⚠️ Данные символа не найдены для ключа: %s", key)
		return nil
	}

	// Проверяем это новый символ или уже обработанный
	isNewSymbol := false
	for _, tf := range symbolData.Timeframes {
		connectionKey := fmt.Sprintf("%s:%s", strings.ToLower(symbolData.Symbol), tf.String())
		sc.connectionsMutex.RLock()
		exists := sc.activeConnections[connectionKey]
		sc.connectionsMutex.RUnlock()

		if !exists {
			isNewSymbol = true
			break
		}
	}

	// Выводим каждое 10000-е сообщение
	if sc.symbolProcessCounter%10000 == 0 {
		zap.S().Infof("📊 Обработка символа: %s с таймфреймами: %v (всего: %d)",
			symbolData.Symbol, symbolData.Timeframes, sc.symbolProcessCounter)
	}

	// ОТКЛЮЧЕНО: WebSocket соединения теперь управляются через MultiplexedManager
	// for _, timeframe := range symbolData.Timeframes {
	// 	if err := sc.startWebSocketConnection(symbolData.Symbol, timeframe); err != nil {
	// 		zap.S().Errorf("❌ Ошибка запуска WebSocket для %s:%s: %v",
	// 			symbolData.Symbol, timeframe, err)
	// 			continue
	// 	}
	// }

	// Если это новый символ, публикуем информацию на 6380 для бэкенда
	if isNewSymbol {
		sc.publishSymbolStatus(symbolData.Symbol, symbolData.Timeframes, "active")
	}

	// Выводим каждое 10000-е сообщение о передаче в MultiplexedManager
	if sc.symbolProcessCounter%10000 == 0 {
		zap.S().Infof("✅ Символ %s с таймфреймами %v передан в MultiplexedManager (всего: %d)",
			symbolData.Symbol, symbolData.Timeframes, sc.symbolProcessCounter)
	}

	return nil
}

// getSymbolData получает данные символа из Redis по ключу
func (sc *SymbolConsumer) getSymbolData(key string) (*SymbolData, error) {
	// Получаем все поля хеша
	fields, err := sc.client.HGetAll(sc.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("ошибка получения хеша: %v", err)
	}

	if len(fields) == 0 {
		return nil, nil
	}

	// Извлекаем символ из ключа (убираем префикс "symbol:details:")
	symbol := strings.TrimPrefix(key, "symbol:details:")
	if symbol == "" {
		return nil, fmt.Errorf("неверный формат ключа: %s", key)
	}

	// Парсим таймфреймы
	var timeframes []Timeframe
	if timeframesStr, exists := fields["timeframes"]; exists {
		timeframes = sc.parseTimeframesString(timeframesStr)
	}

	// Если таймфреймы не указаны, используем таймфрейм воркера по умолчанию
	if len(timeframes) == 0 {
		defaultTfStr := os.Getenv("BINANCE_WS_TIMEFRAME")
		if defaultTfStr == "" {
			defaultTfStr = "kline_1m"
		}
		if defaultTf, valid := GetTimeframeByString(defaultTfStr); valid {
			timeframes = []Timeframe{defaultTf}
			zap.S().Warnf("⚠️ Таймфреймы не указаны для %s, используем %s по умолчанию", symbol, defaultTfStr)
		} else {
			timeframes = []Timeframe{M1}
			zap.S().Warnf("⚠️ Таймфреймы не указаны для %s, используем kline_1m по умолчанию", symbol)
		}
	}

	return &SymbolData{
		Symbol:     symbol,
		Timeframes: timeframes,
	}, nil
}

// parseTimeframesString парсит строку с таймфреймами
func (sc *SymbolConsumer) parseTimeframesString(s string) []Timeframe {
	var timeframes []Timeframe

	// Пытаемся распарсить как JSON массив
	var tfStrings []string
	if err := json.Unmarshal([]byte(s), &tfStrings); err == nil {
		// Успешно распарсили JSON
		for _, tfStr := range tfStrings {
			if tf, valid := GetTimeframeByString(tfStr); valid {
				timeframes = append(timeframes, tf)
			} else {
				zap.S().Warnf("⚠️ Неизвестный таймфрейм: %s", tfStr)
			}
		}
		return timeframes
	}

	// Если не JSON, пытаемся разбить по запятой
	for _, tfStr := range strings.Split(s, ",") {
		tfStr = strings.TrimSpace(tfStr)
		if tfStr == "" {
			continue
		}

		if tf, valid := GetTimeframeByString(tfStr); valid {
			timeframes = append(timeframes, tf)
		} else {
			zap.S().Warnf("⚠️ Неизвестный таймфрейм: %s", tfStr)
		}
	}

	return timeframes
}

func (sc *SymbolConsumer) parseTimeframesFromJSONString(data string) []Timeframe {
	if strings.TrimSpace(data) == "" {
		return nil
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(data), &payload); err == nil {
		if raw, exists := payload["timeframes"]; exists {
			if extracted := sc.parseTimeframesFromInterfaces(raw); len(extracted) > 0 {
				return extracted
			}
		}
	}

	var tfStrings []string
	if err := json.Unmarshal([]byte(data), &tfStrings); err == nil {
		return sc.parseTimeframesFromStringSlice(tfStrings)
	}

	return nil
}

func (sc *SymbolConsumer) parseTimeframesFromInterfaces(value interface{}) []Timeframe {
	switch v := value.(type) {
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			switch itemVal := item.(type) {
			case string:
				out = append(out, itemVal)
			case []byte:
				out = append(out, string(itemVal))
			}
		}
		return sc.parseTimeframesFromStringSlice(out)
	case []string:
		return sc.parseTimeframesFromStringSlice(v)
	default:
		return nil
	}
}

func (sc *SymbolConsumer) parseTimeframesFromStringSlice(values []string) []Timeframe {
	var timeframes []Timeframe
	for _, tfStr := range values {
		tfStr = strings.TrimSpace(tfStr)
		if tfStr == "" {
			continue
		}
		if tf, valid := GetTimeframeByString(tfStr); valid {
			timeframes = append(timeframes, tf)
		}
	}
	return timeframes
}

// startWebSocketConnection запускает WebSocket соединение для символа и таймфрейма
func (sc *SymbolConsumer) startWebSocketConnection(symbol string, timeframe Timeframe) error {
	connectionKey := fmt.Sprintf("%s:%s", symbol, timeframe)

	// Проверяем, не запущено ли уже соединение
	sc.connectionsMutex.RLock()
	if sc.activeConnections[connectionKey] {
		sc.connectionsMutex.RUnlock()
		zap.S().Infof("ℹ️ WebSocket соединение для %s уже активно", connectionKey)
		return nil
	}
	sc.connectionsMutex.RUnlock()

	// Помечаем соединение как активное
	sc.connectionsMutex.Lock()
	sc.activeConnections[connectionKey] = true
	sc.connectionsMutex.Unlock()

	// Увеличиваем счетчик только при запуске нового соединения
	startupCount := sc.getStartupCounter()

	// Показываем startup сообщения только каждые 1000 раз
	if startupCount%1000 == 0 {
		zap.S().Infof("🔌 Запуск WebSocket соединения для %s:%s (улучшенный клиент)", symbol, timeframe)
	}

	// Запускаем WebSocket соединение в горутине
	go func() {
		defer func() {
			// Помечаем соединение как неактивное при завершении
			sc.connectionsMutex.Lock()
			delete(sc.activeConnections, connectionKey)
			sc.connectionsMutex.Unlock()
			// Показываем completion сообщения только каждые 1000 раз
			// Используем тот же счетчик для консистентности
			if startupCount%1000 == 0 {
				zap.S().Infof("🔌 WebSocket соединение для %s завершено", connectionKey)
			}
		}()

		// Если установлен candlePublisher, логируем это
		if sc.candlePublisher != nil {
			zap.S().Infof("✅ CandlePublisher доступен для %s:%s", symbol, timeframe)
		}

		// ОТКЛЮЧЕНО: WebSocket соединения теперь управляются через MultiplexedManager
		// StartImprovedWS(symbol, timeframe.String())
		zap.S().Infof("📤 Символ %s:%s передан в MultiplexedManager", symbol, timeframe)
	}()

	return nil
}

// GetActiveConnections возвращает список активных соединений
func (sc *SymbolConsumer) GetActiveConnections() map[string]bool {
	sc.connectionsMutex.RLock()
	defer sc.connectionsMutex.RUnlock()

	// Создаем копию для безопасного возврата
	result := make(map[string]bool)
	for k := range sc.activeConnections {
		result[k] = true
	}

	return result
}

// GetActiveConnectionsCount возвращает количество активных соединений
func (sc *SymbolConsumer) GetActiveConnectionsCount() int {
	sc.connectionsMutex.RLock()
	defer sc.connectionsMutex.RUnlock()
	return len(sc.activeConnections)
}

// GetLastFrameAt возвращает время последнего полученного WS-фрейма из MultiplexedManager.
func (sc *SymbolConsumer) GetLastFrameAt() time.Time {
	if sc.multiplexedManager != nil {
		return sc.multiplexedManager.GetLastFrameAt()
	}
	return time.Time{}
}

// monitorBackendSymbolsStream мониторит stream:symbols на redis-worker-1 (6380) для получения символов от бэкенда
func (sc *SymbolConsumer) monitorBackendSymbolsStream() {
	defer sc.wg.Done()

	zap.S().Infof("🔄 Запуск мониторинга stream:symbols на redis-worker-1 (6380)")

	// Создаем уникальные имена для consumer group и consumer name на основе таймфрейма и хоста.
	// Это важно, так как КАЖДЫЙ воркер должен получить сообщение о новом символе.
	// Если группа будет общей, сообщения будут распределяться между ними, и кто-то может пропустить символ.
	hostname, _ := os.Hostname()
	tf := os.Getenv("BINANCE_WS_TIMEFRAME")
	if tf == "" {
		tf = "default"
	}
	streamName := streams.Symbols
	groupName := fmt.Sprintf("go-worker-symbols-group-%s", tf)
	consumerName := fmt.Sprintf("go-worker-%s-%s", tf, hostname)

	// Пытаемся создать consumer group
	err := redisclient.ClientWorker.XGroupCreateMkStream(sc.ctx, streamName, groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		zap.S().Errorf("⚠️ Ошибка создания consumer group '%s' для %s: %v", groupName, streamName, err)
	} else {
		zap.S().Infof("✅ Consumer group '%s' (consumer: %s) готова для %s", groupName, consumerName, streamName)
	}

	// Сначала обрабатываем существующие символы при старте
	zap.S().Infof("🔄 Начальная загрузка символов с бэкенда (6380)...")
	if err := sc.processBackendSymbols(); err != nil {
		zap.S().Errorf("⚠️ Ошибка начальной загрузки символов: %v", err)
	}

	for {
		select {
		case <-sc.ctx.Done():
			zap.S().Infof("🛑 Остановка мониторинга stream:symbols")
			return
		case <-sc.stopChan:
			zap.S().Infof("🛑 Получен сигнал остановки мониторинга stream:symbols")
			return
		default:
			// Читаем новые сообщения из stream:symbols
			streams, err := redisclient.ClientWorker.XReadGroup(sc.ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Count:    10,
				Block:    5 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}

				// Проверяем, не является ли ошибка отменой контекста (graceful shutdown)
				if err == context.Canceled {
					zap.S().Infof("🛑 Остановка чтения из stream:symbols: контекст отменен")
					return
				}

				errMsg := err.Error()

				switch {
				case strings.Contains(errMsg, "NOGROUP"):
					zap.S().Infof("ℹ️ stream:symbols: consumer group '%s' отсутствует, создаём…", groupName)
					if cgErr := redisclient.ClientWorker.XGroupCreateMkStream(sc.ctx, streamName, groupName, "0").Err(); cgErr != nil && !strings.Contains(cgErr.Error(), "BUSYGROUP") {
						zap.S().Errorf("❌ Не удалось пересоздать consumer group '%s' для stream:symbols: %v", groupName, cgErr)
					}
					time.Sleep(2 * time.Second)
				case strings.Contains(errMsg, "circuit breaker"):
					zap.S().Infof("⏳ stream:symbols: circuit breaker открыт, ждём 65 секунд перед повторной попыткой")
					time.Sleep(65 * time.Second)
				default:
					zap.S().Errorf("⚠️ Ошибка чтения из stream:symbols: %v", err)
					time.Sleep(2 * time.Second)
				}

				continue
			}

			// Обрабатываем полученные сообщения
			for _, stream := range streams {
				for _, message := range stream.Messages {
					if err := sc.processBackendSymbolMessage(message); err != nil {
						zap.S().Errorf("⚠️ Ошибка обработки сообщения %s: %v", message.ID, err)
					} else {
						// Подтверждаем обработку
						redisclient.ClientWorker.XAck(sc.ctx, streamName, groupName, message.ID)
					}
				}
			}
		}
	}
}

// processBackendSymbols обрабатывает символы из redis-worker-1 (6380)
func (sc *SymbolConsumer) processBackendSymbols() error {
	// Сканируем все ключи symbol:details:* на redis-worker-1
	var cursor uint64
	var err error
	newSymbolsCount := 0

	for {
		var keys []string
		keys, cursor, err = redisclient.ClientWorker.Scan(sc.ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			return fmt.Errorf("ошибка сканирования ключей на 6380: %v", err)
		}

		// Обрабатываем найденные ключи
		for _, key := range keys {
			// Получаем данные символа с redis-worker-1
			symbolData, err := sc.getSymbolDataFromBackend(key)
			if err != nil {
				zap.S().Errorf("⚠️ Ошибка получения данных символа %s с 6380: %v", key, err)
				continue
			}

			if symbolData == nil {
				continue
			}

			// Проверяем нужно ли добавлять этот символ
			if sc.shouldProcessSymbol(symbolData.Symbol, symbolData.Timeframes) {
				// Сначала копируем символ в основной Redis (6379)
				// чтобы он был учтен в getAllSymbolsFromRedis()
				backendKey := key

				// Получаем данные с redis-worker-1
				fields, err := redisclient.ClientWorker.HGetAll(sc.ctx, backendKey).Result()
				if err == nil && len(fields) > 0 {
					// Копируем в основной Redis
					if err := sc.client.HSet(sc.ctx, backendKey, fields).Err(); err != nil {
						zap.S().Errorf("⚠️ Ошибка копирования символа %s в основной Redis: %v", backendKey, err)
					} else {
						count := atomic.AddUint64(&symbolConsumerCopyCounter, 1)
						// Логируем только каждое 5000-е копирование
						if count%5000 == 0 {
							zap.S().Infof("📋 Символ %s скопирован с 6380 на 6379 (всего скопировано: %d)", backendKey, count)
						}
					}
				}

				// Добавляем символ в multiplexedManager
				if sc.multiplexedManager != nil {
					for _, tf := range symbolData.Timeframes {
						connectionKey := fmt.Sprintf("%s:%s", strings.ToLower(symbolData.Symbol), tf.String())

						sc.connectionsMutex.RLock()
						exists := sc.activeConnections[connectionKey]
						sc.connectionsMutex.RUnlock()

						if !exists {
							count := atomic.AddUint64(&symbolConsumerNewSymbolCounter, 1)
							// Логируем только каждый 5000-й новый символ
							if count%5000 == 0 {
								zap.S().Infof("🆕 Новый символ с бэкенда (6380): %s@%s (всего новых: %d)", symbolData.Symbol, tf.String(), count)
							}

							// Добавляем в активные соединения
							sc.connectionsMutex.Lock()
							sc.activeConnections[connectionKey] = true
							sc.connectionsMutex.Unlock()

							// Добавляем символ в multiplexedManager
							sc.multiplexedManager.AddSymbol(strings.ToLower(symbolData.Symbol), tf)
							newSymbolsCount++
						}
					}
				}
			}
		}

		// Если курсор вернулся к 0, значит все ключи обработаны
		if cursor == 0 {
			break
		}
	}

	if newSymbolsCount > 0 {
		zap.S().Infof("✅ Обработано %d новых символов с бэкенда (6380)", newSymbolsCount)

		// Триггерим обновление соединений в MultiplexedManager
		if sc.multiplexedManager != nil {
			zap.S().Infof("🔄 Триггер обновления соединений для новых символов...")
			go func() {
				// Небольшая задержка перед обновлением
				time.Sleep(2 * time.Second)
				if err := sc.multiplexedManager.UpdateConnectionsNow(); err != nil {
					count := atomic.AddUint64(&symbolConsumerUpdateErrorCounter, 1)
					// Логируем только каждую 5000-ю ошибку
					if count%5000 == 0 {
						zap.S().Errorf("⚠️ Ошибка обновления соединений: %v (всего ошибок: %d)", err, count)
					}
				}
			}()
		}
	}

	return nil
}

// getSymbolDataFromBackend получает данные символа из redis-worker-1 (6380)
func (sc *SymbolConsumer) getSymbolDataFromBackend(key string) (*SymbolData, error) {
	// Получаем все поля хеша
	fields, err := redisclient.ClientWorker.HGetAll(sc.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("ошибка получения хеша с 6380: %v", err)
	}

	if len(fields) == 0 {
		return nil, nil
	}

	// Извлекаем символ из ключа
	symbol := strings.TrimPrefix(key, "symbol:details:")
	if symbol == "" {
		return nil, fmt.Errorf("неверный формат ключа: %s", key)
	}

	// Парсим таймфреймы
	var timeframes []Timeframe
	if timeframesStr, exists := fields["timeframes"]; exists {
		timeframes = sc.parseTimeframesString(timeframesStr)
	}

	// Если таймфреймы не указаны, используем таймфрейм воркера по умолчанию
	if len(timeframes) == 0 {
		defaultTfStr := os.Getenv("BINANCE_WS_TIMEFRAME")
		if defaultTfStr == "" {
			defaultTfStr = "kline_1m"
		}
		if defaultTf, valid := GetTimeframeByString(defaultTfStr); valid {
			timeframes = []Timeframe{defaultTf}
		} else {
			timeframes = []Timeframe{M1}
		}
	}

	return &SymbolData{
		Symbol:     symbol,
		Timeframes: timeframes,
	}, nil
}

// shouldProcessSymbol проверяет нужно ли обрабатывать символ
func (sc *SymbolConsumer) shouldProcessSymbol(symbol string, timeframes []Timeframe) bool {
	// Проверяем whitelist: если REQUIRED_SYMBOLS задан — принимаем только символы из него
	if len(sc.requiredSymbols) > 0 {
		if _, allowed := sc.requiredSymbols[strings.ToLower(symbol)]; !allowed {
			return false // Символ не в whitelist — пропускаем
		}
	}

	// Проверяем хотя бы один таймфрейм
	for _, tf := range timeframes {
		connectionKey := fmt.Sprintf("%s:%s", strings.ToLower(symbol), tf.String())

		sc.connectionsMutex.RLock()
		exists := sc.activeConnections[connectionKey]
		sc.connectionsMutex.RUnlock()

		if !exists {
			return true // Есть хотя бы один новый таймфрейм
		}
	}

	return false
}

func (sc *SymbolConsumer) extractTimeframesFromMessage(message redis.XMessage) []Timeframe {
	var timeframes []Timeframe

	if tfStr, ok := message.Values["timeframes"].(string); ok && tfStr != "" {
		timeframes = sc.parseTimeframesString(tfStr)
		if len(timeframes) > 0 {
			return timeframes
		}
	}

	if dataValue, ok := message.Values["data"]; ok {
		switch v := dataValue.(type) {
		case string:
			if extracted := sc.parseTimeframesFromJSONString(v); len(extracted) > 0 {
				return extracted
			}
		case []byte:
			if extracted := sc.parseTimeframesFromJSONString(string(v)); len(extracted) > 0 {
				return extracted
			}
		case []interface{}:
			if extracted := sc.parseTimeframesFromInterfaces(v); len(extracted) > 0 {
				return extracted
			}
		}
	}

	return timeframes
}

// processBackendSymbolMessage обрабатывает сообщение из stream:symbols от бэкенда
func (sc *SymbolConsumer) processBackendSymbolMessage(message redis.XMessage) error {
	msgCount := atomic.AddUint64(&symbolConsumerMessageCounter, 1)
	// Логируем только каждое 5000-е сообщение
	if msgCount%5000 == 0 {
		zap.S().Infof("📨 Получено сообщение от бэкенда: %s (всего обработано: %d)", message.ID, msgCount)
	}

	// Извлекаем данные из сообщения
	symbolName, ok := message.Values["symbol"].(string)
	if !ok {
		return fmt.Errorf("отсутствует поле symbol")
	}

	action, ok := message.Values["action"].(string)
	if !ok {
		action = "add" // По умолчанию добавление
	}

	procCount := atomic.AddUint64(&symbolConsumerProcessCounter, 1)
	// Логируем только каждую 5000-ю обработку
	if procCount%5000 == 0 {
		zap.S().Infof("🎯 Обработка символа %s (действие: %s) от бэкенда (всего обработано: %d)", symbolName, action, procCount)
	}

	// Получаем данные символа с redis-worker-1
	key := fmt.Sprintf("symbol:details:%s", strings.ToLower(symbolName))
	symbolData, err := sc.getSymbolDataFromBackend(key)
	if err != nil {
		return fmt.Errorf("ошибка получения данных символа: %v", err)
	}

	if symbolData == nil {
		if sc.symbolSupplementer != nil {
			timeframes := sc.extractTimeframesFromMessage(message)
			if err := sc.symbolSupplementer.EnsureSymbolDetails(symbolName, timeframes); err != nil {
				return fmt.Errorf("данные символа не найдены: %s (%v)", symbolName, err)
			}

			symbolData, err = sc.getSymbolDataFromBackend(key)
			if err != nil {
				return fmt.Errorf("ошибка получения данных символа после добавления: %v", err)
			}

			if symbolData == nil {
				return fmt.Errorf("данные символа не найдены: %s (после автоматического добавления)", symbolName)
			}

			zap.S().Infof(
				"🆕 Символ %s автоматически добавлен в Redis (таймфреймы: %s)",
				strings.ToUpper(symbolData.Symbol),
				SerializeTimeframesForRedis(symbolData.Timeframes),
			)
		} else {
			return fmt.Errorf("данные символа не найдены: %s", symbolName)
		}
	}

	// Копируем символ в основной Redis (6379)
	fields, err := redisclient.ClientWorker.HGetAll(sc.ctx, key).Result()
	if err == nil && len(fields) > 0 {
		if err := sc.client.HSet(sc.ctx, key, fields).Err(); err != nil {
			zap.S().Errorf("⚠️ Ошибка копирования символа %s в основной Redis: %v", key, err)
		} else {
			count := atomic.AddUint64(&symbolConsumerCopyCounter, 1)
			// Логируем только каждое 5000-е копирование
			if count%5000 == 0 {
				zap.S().Infof("📋 Символ %s скопирован с 6380 на 6379 (всего скопировано: %d)", symbolName, count)
			}
		}
	}

	// Добавляем символ в активные подключения
	newSymbolsAdded := 0
	if sc.multiplexedManager != nil {
		for _, tf := range symbolData.Timeframes {
			connectionKey := fmt.Sprintf("%s:%s", strings.ToLower(symbolData.Symbol), tf.String())

			sc.connectionsMutex.RLock()
			exists := sc.activeConnections[connectionKey]
			sc.connectionsMutex.RUnlock()

			if !exists {
				count := atomic.AddUint64(&symbolConsumerNewSymbolCounter, 1)
				// Логируем только каждый 5000-й новый символ
				if count%5000 == 0 {
					zap.S().Infof("🆕 Новый символ от бэкенда: %s@%s (всего новых: %d)", symbolData.Symbol, tf.String(), count)
				}

				sc.connectionsMutex.Lock()
				sc.activeConnections[connectionKey] = true
				sc.connectionsMutex.Unlock()

				sc.multiplexedManager.AddSymbol(strings.ToLower(symbolData.Symbol), tf)
				newSymbolsAdded++
			}
		}
	}

	// Если добавили новые символы, триггерим обновление соединений
	if newSymbolsAdded > 0 {
		count := atomic.AddUint64(&symbolConsumerNewSymbolCounter, 1)
		// Логируем только каждое 5000-е добавление
		if count%5000 == 0 {
			zap.S().Infof("✅ Добавлено %d новых подписок для %s (всего добавлений: %d)", newSymbolsAdded, symbolName, count)
		}

		if sc.multiplexedManager != nil {
			go func() {
				time.Sleep(2 * time.Second)
				if err := sc.multiplexedManager.UpdateConnectionsNow(); err != nil {
					count := atomic.AddUint64(&symbolConsumerUpdateErrorCounter, 1)
					// Логируем только каждую 5000-ю ошибку
					if count%5000 == 0 {
						zap.S().Errorf("⚠️ Ошибка обновления соединений: %v (всего ошибок: %d)", err, count)
					}
				}
			}()
		}

		// Публикуем подтверждение обратно в stream:symbols на 6380
		sc.publishSymbolStatus(symbolName, symbolData.Timeframes, "subscribed")
	}

	return nil
}

// publishSymbolStatus публикует статус символа в stream:symbols на 6380 для бэкенда
func (sc *SymbolConsumer) publishSymbolStatus(symbol string, timeframes []Timeframe, status string) {
	// Формируем список таймфреймов
	tfList := make([]string, len(timeframes))
	for i, tf := range timeframes {
		tfList[i] = tf.String()
	}

	tfJSON, _ := json.Marshal(tfList)

	// Публикуем в stream:symbols на 6380
	streamData := map[string]interface{}{
		"symbol":     symbol,
		"action":     status,
		"timeframes": string(tfJSON),
		"timestamp":  timeutil.GetCurrentTimestampMs(),
		"source":     "go-worker",
	}

	if _, err := redisclient.XAddWithRetry(sc.ctx, redisclient.ClientWorker, &redis.XAddArgs{
		Stream: streams.Symbols,
		MaxLen: streams.MaxLenPerSymbol,
		Approx: true,
		ID:     "*",
		Values: streamData,
	}); err != nil {
		zap.S().Errorf("⚠️ Ошибка публикации статуса символа в stream:symbols: %v", err)
	} else {
		count := atomic.AddUint64(&symbolConsumerPublishCounter, 1)
		// Логируем только каждую 5000-ю публикацию
		if count%5000 == 0 {
			zap.S().Infof("📤 Статус символа %s опубликован в stream:symbols (6380): %s (всего опубликовано: %d)", symbol, status, count)
		}
	}
}
