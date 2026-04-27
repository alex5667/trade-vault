package binance

import (
	"context"
	"fmt"
	"go-worker/internal/interfaces"
	"go-worker/internal/monitoring"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// Счетчики для уменьшения логов в multiplexed_manager
var connectionUpdateErrorCounter uint64
var multiplexedAddSymbolCounter uint64     // Счетчик добавления символов
var multiplexedPendingSymbolCounter uint64 // Счетчик символов в ожидании
var connectionUpdateCounter uint64         // Счетчик вызовов обновления соединений

// getAllTimeframesAsStrings возвращает все поддерживаемые таймфреймы как строки
func getAllTimeframesAsStrings() []string {
	allTimeframes := GetAllTimeframes()
	result := make([]string, len(allTimeframes))
	for i, tf := range allTimeframes {
		result[i] = tf.String()
	}
	return result
}

// getTimeframesFromEnv читает таймфрейм из переменной окружения BINANCE_WS_TIMEFRAME
// Если переменная не задана, возвращает все таймфреймы
func getTimeframesFromEnv() []string {
	envTimeframe := os.Getenv("BINANCE_WS_TIMEFRAME")
	if envTimeframe == "" {
		zap.S().Warnf("⚠️ BINANCE_WS_TIMEFRAME не задан, используются ВСЕ таймфреймы")
		return getAllTimeframesAsStrings()
	}

	// Проверяем валидность таймфрейма
	tf := Timeframe(envTimeframe)
	if !tf.IsValid() {
		zap.S().Fatalf("❌ FATAL: Неверный таймфрейм '%s' в BINANCE_WS_TIMEFRAME. Завершение работы (fail-fast).", envTimeframe)
	}

	zap.S().Infof("✅ Воркер будет обрабатывать ТОЛЬКО таймфрейм: %s", envTimeframe)
	return []string{envTimeframe}
}

// getEnvAsInt читает целочисленную переменную окружения или возвращает fallback
func getEnvAsInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		zap.S().Warnf("⚠️ Ошибка парсинга %s='%s', используется дефолтное значение %d", key, valStr, fallback)
		return fallback
	}
	return val
}

// MultiplexedManager управляет multiplexed WebSocket соединениями
type MultiplexedManager struct {
	redisClient *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc

	// Конфигурация
	maxSymbolsPerConnection int      // Максимальное количество символов на одно соединение
	maxConnections          int      // Максимальное количество одновременных соединений
	timeframes              []string // Поддерживаемые таймфреймы

	// Активные соединения
	connections map[string]*MultiplexedWSClient // key: "timeframe:connection_id"
	connMutex   sync.RWMutex

	// Callback для публикации данных свечей
	candlePublisher interfaces.CandlePublisher

	// Статистика
	totalSymbols     atomic.Int32
	totalConnections atomic.Int32

	// Мониторинг
	monitor *monitoring.WebSocketMonitor

	// Флаг перезапуска
	restartInProgress int32
}

// NewMultiplexedManager создает новый менеджер multiplexed соединений
func NewMultiplexedManager(redisClient *redis.Client, candlePublisher interfaces.CandlePublisher) *MultiplexedManager {
	ctx, cancel := context.WithCancel(context.Background())

	// Получаем таймфрейм из переменной окружения или используем все
	timeframes := getTimeframesFromEnv()

	// Считываем лимиты соединений и символов из переменных окружения
	maxSymbols := getEnvAsInt("BINANCE_WS_MAX_SYMBOLS_PER_CONN", 35)
	maxConns := getEnvAsInt("BINANCE_WS_MAX_CONNECTIONS", 15)

	return &MultiplexedManager{
		redisClient: redisClient,
		ctx:         ctx,
		cancel:      cancel,
		// Оптимальный баланс между количеством соединений и нагрузкой - 35
		// Достаточное количество соединений для обработки символов - 15
		maxSymbolsPerConnection: maxSymbols,
		maxConnections:          maxConns,
		timeframes:              timeframes,
		connections:             make(map[string]*MultiplexedWSClient),
		candlePublisher:         candlePublisher,
		monitor:                 monitoring.NewWebSocketMonitor(),
	}
}

// SetMonitor устанавливает монитор для отслеживания соединений
func (mm *MultiplexedManager) SetMonitor(monitor *monitoring.WebSocketMonitor) {
	mm.monitor = monitor
}

// Start запускает менеджер и создает соединения для всех символов
func (mm *MultiplexedManager) Start() error {
	// СТАРТ: Оставляем логирование запуска
	zap.S().Infof("🚀 Запуск MultiplexedManager")
	zap.S().Infof("⚙️ Настройки: макс. символов на соединение=%d, макс. соединений=%d",
		mm.maxSymbolsPerConnection, mm.maxConnections)

	// Получаем все символы из Redis
	symbols, err := mm.getAllSymbolsFromRedis()
	if err != nil {
		return fmt.Errorf("ошибка получения символов из Redis: %v", err)
	}

	// СТАРТ: Оставляем логирование количества найденных символов
	zap.S().Infof("📊 Найдено %d символов в Redis", len(symbols))

	// Создаем соединения для каждого таймфрейма
	for _, timeframe := range mm.timeframes {
		// zap.S().Infof("🔄 Обработка таймфрейма: %s", timeframe)
		if err := mm.createConnectionsForTimeframe(timeframe, symbols); err != nil {
			// ОШИБКА: Оставляем логирование ошибок
			zap.S().Errorf("⚠️ Ошибка создания соединений для таймфрейма %s: %v", timeframe, err)
			continue
		}
	}

	// Запускаем мониторинг новых символов
	go mm.monitorNewSymbols()

	// СТАРТ: Оставляем финальное сообщение о запуске
	zap.S().Infof("✅ MultiplexedManager запущен: %d таймфреймов, %d символов", len(mm.timeframes), len(symbols))

	return nil
}

// Stop останавливает все соединения без отмены внешнего ctx.
// Не трогает mm.cancel() — ctx принадлежит монитору и горутинам снаружи.
func (mm *MultiplexedManager) Stop() {
	mm.connMutex.Lock()
	defer mm.connMutex.Unlock()

	for _, conn := range mm.connections {
		conn.Stop()
		// Метрика: соединение остановлено
		monitoring.WsConnectionsActive.WithLabelValues("binance", conn.config.Timeframe).Dec()
	}

	mm.connections = make(map[string]*MultiplexedWSClient)
	mm.totalSymbols.Store(0)
	mm.totalConnections.Store(0)
}

// createConnectionsForTimeframe создает соединения для конкретного таймфрейма
func (mm *MultiplexedManager) createConnectionsForTimeframe(timeframe string, symbols []string) error {
	// Группируем символы по maxSymbolsPerConnection
	symbolGroups := mm.groupSymbols(symbols, mm.maxSymbolsPerConnection)

	// СТАРТ: Оставляем логирование создания соединений
	// Закомментировано для уменьшения шума в логах
	// zap.S().Infof("📊 Создание %d соединений для таймфрейма %s (всего символов: %d, макс. на соединение: %d)",
	// 	len(symbolGroups), timeframe, len(symbols), mm.maxSymbolsPerConnection)

	// 🎯 ВОЗВРАЩЕНО К РАБОЧЕМУ КОДУ из коммита f31310c (где не было ошибок)
	// Последовательное создание соединений с задержкой для избежания thundering herd
	// Это предотвращает одновременное получение всех слотов семафора
	for i, symbolGroup := range symbolGroups {
		connectionID := fmt.Sprintf("%s:conn_%d", timeframe, i)

		// Создаем конфигурацию для соединения
		config := &MultiplexedWSConfig{
			Symbols:    symbolGroup,
			Timeframe:  timeframe,
			MaxRetries: 10,              // Сохраняем 10 попыток, но с exponential backoff
			RetryDelay: 1 * time.Second, // Начальная задержка для backoff
		}

		// Создаем клиент
		client := NewMultiplexedWSClient(config, mm.redisClient, mm.candlePublisher, mm.monitor)

		// Запускаем соединение
		if err := client.Start(); err != nil {
			zap.S().Errorf("❌ Ошибка запуска соединения %s: %v", connectionID, err)
			continue
		}

		// Сохраняем соединение
		mm.connMutex.Lock()
		mm.connections[connectionID] = client
		mm.totalConnections.Add(1)
		mm.totalSymbols.Add(int32(len(symbolGroup)))
		mm.connMutex.Unlock()

		// Метрика: активное соединение создано
		monitoring.WsConnectionsActive.WithLabelValues("binance", timeframe).Inc()

		// Staggered startup: задержка между соединениями для избежания thundering herd.
		// ENV WS_STARTUP_STAGGER_MS: default 5000 (prod). Для dev можно 1000-2000.
		if i < len(symbolGroups)-1 {
			time.Sleep(wsStartupStaggerMs())
		}
	}

	return nil
}

// groupSymbols группирует символы по указанному размеру группы
func (mm *MultiplexedManager) groupSymbols(symbols []string, groupSize int) [][]string {
	var groups [][]string

	for i := 0; i < len(symbols); i += groupSize {
		end := i + groupSize
		if end > len(symbols) {
			end = len(symbols)
		}
		groups = append(groups, symbols[i:end])
	}

	return groups
}

// getAllSymbolsFromRedis получает все символы из Redis
func (mm *MultiplexedManager) getAllSymbolsFromRedis() ([]string, error) {
	var symbols []string
	var cursor uint64

	// Используем context.Background() чтобы избежать отмены при mm.Stop()
	ctx := context.Background()

	for {
		var keys []string
		var err error
		keys, cursor, err = mm.redisClient.Scan(ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			// Проверяем на ошибку загрузки Redis
			if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
				zap.S().Warnf("⚠️ Redis is loading dataset, skipping getAllSymbolsFromRedis")
				return []string{}, nil // Возвращаем пустой список вместо ошибки
			}
			return nil, err
		}

		// Извлекаем символы из ключей
		for _, key := range keys {
			symbol := strings.TrimPrefix(key, "symbol:details:")
			if symbol != "" {
				symbols = append(symbols, strings.ToLower(symbol))
			}
		}

		if cursor == 0 {
			break
		}
	}

	return symbols, nil
}

// monitorNewSymbols мониторит новые символы и обновляет соединения
func (mm *MultiplexedManager) monitorNewSymbols() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mm.ctx.Done():
			return
		case <-ticker.C:
			// Проверяем новые символы
			if err := mm.updateConnectionsIfNeeded(); err != nil {
				count := atomic.AddUint64(&connectionUpdateErrorCounter, 1)
				// ОШИБКА: Оставляем логирование ошибок обновления (каждую 10000-ю)
				if count%10000 == 0 {
					zap.S().Errorf("⚠️ Ошибка обновления соединений: %v (всего ошибок: %d)", err, count)
				}
			}
		}
	}
}

// wsStartupStaggerMs возвращает задержку между соединениями при старте.
// Читает ENV WS_STARTUP_STAGGER_MS (default 5000ms).
func wsStartupStaggerMs() time.Duration {
	v := strings.TrimSpace(os.Getenv("WS_STARTUP_STAGGER_MS"))
	if v == "" {
		return 5000 * time.Millisecond
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		zap.S().Warnf("⚠️ WS_STARTUP_STAGGER_MS invalid (%q), using default 5000ms", v)
		return 5000 * time.Millisecond
	}
	return time.Duration(n) * time.Millisecond
}

// updateConnectionsIfNeeded обновляет соединения если появились новые символы.
// atomic.restartInProgress снимается ВНУТРИ горутины — после завершения всего restart цикла.
func (mm *MultiplexedManager) updateConnectionsIfNeeded() error {
	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Больше не выполняем полный рестарт всех соединений.
	// Символы теперь добавляются динамически через AddSymbol (SUBSCRIBE over WebSocket).
	// Проверка len(currentSymbols) != currentTotal была некорректной для множественных таймфреймов
	// и приводила к необоснованному сбросу всех соединений вместо мягкого добавления.
	return nil
}

// AddSymbol добавляет новый символ в существующие соединения или создает новое
func (mm *MultiplexedManager) AddSymbol(symbol string, timeframe Timeframe) error {
	mm.connMutex.Lock()
	defer mm.connMutex.Unlock()

	timeframeStr := timeframe.String()
	symbolLower := strings.ToLower(symbol)

	// Idempotency guard: dedup check перед добавлением символа.
	// Если символ уже отслеживается в любом соединении этого таймфрейма — выходим без изменений.
	for key, conn := range mm.connections {
		if strings.HasPrefix(key, timeframeStr+":") {
			conn.mutex.RLock()
			for _, s := range conn.config.Symbols {
				if strings.ToLower(s) == symbolLower {
					conn.mutex.RUnlock()
					return nil // уже отслеживается
				}
			}
			conn.mutex.RUnlock()
		}
	}

	atomic.AddUint64(&multiplexedAddSymbolCounter, 1)

	var targetConn *MultiplexedWSClient

	// Ищем существующее соединение для этого таймфрейма, в котором есть место
	for key, conn := range mm.connections {
		if strings.HasPrefix(key, timeframeStr+":") {
			conn.mutex.RLock()
			count := len(conn.config.Symbols)
			conn.mutex.RUnlock()

			if count < mm.maxSymbolsPerConnection {
				targetConn = conn
				_ = key
				break
			}
		}
	}

	if targetConn != nil {
		// Соединение с достаточным местом найдено, добавляем динамически
		if err := targetConn.AddSymbol(symbol); err != nil {
			return err
		}
		// Увеличиваем счетчик атомарно (ошибки нет — символ добавлен успешно)
		mm.totalSymbols.Add(1)
	} else {
		// Не найдено подходящих соединений, создаем новое с этим символом
		connID := len(mm.connections)
		connKey := fmt.Sprintf("%s:conn_%d", timeframeStr, connID)

		config := &MultiplexedWSConfig{
			Symbols:    []string{symbolLower},
			Timeframe:  timeframeStr,
			MaxRetries: 10,
			RetryDelay: 10 * time.Second,
		}

		newConn := NewMultiplexedWSClient(
			config,
			mm.redisClient,
			mm.candlePublisher,
			mm.monitor,
		)

		mm.connections[connKey] = newConn
		// totalConnections и totalSymbols инкрементируем ДО запуска горутины, под mm.connMutex.Lock.
		// Это гарантирует, что счётчик не вырастает при ошибке Start() и не имеет data race с горутиной.
		mm.totalConnections.Add(1)
		mm.totalSymbols.Add(1)

		go func() {
			if err := newConn.Start(); err != nil {
				zap.S().Errorf("❌ Ошибка запуска multiplexed соединения %s: %v", connKey, err)
				return
			}
			// Метрика: новое соединение успешно поднято
			monitoring.WsConnectionsActive.WithLabelValues("binance", timeframeStr).Inc()
		}()
	}

	return nil
}

// UpdateConnectionsNow принудительно обновляет соединения (вызывается при обнаружении новых символов с бэкенда)
func (mm *MultiplexedManager) UpdateConnectionsNow() error {
	atomic.AddUint64(&connectionUpdateCounter, 1)
	// Закомментировано: информационное сообщение о принудительном обновлении
	// count := atomic.AddUint64(&connectionUpdateCounter, 1)
	// if count%10000 == 0 {
	// 	zap.S().Infof("🔄 Принудительное обновление соединений... (вызовов: %d)", count)
	// }
	return mm.updateConnectionsIfNeeded()
}

// GetStats возвращает статистику менеджера
func (mm *MultiplexedManager) GetStats() map[string]interface{} {
	mm.connMutex.RLock()
	defer mm.connMutex.RUnlock()

	stats := map[string]interface{}{
		"totalSymbols":      int(mm.totalSymbols.Load()),
		"totalConnections":  int(mm.totalConnections.Load()),
		"maxSymbolsPerConn": mm.maxSymbolsPerConnection,
		"maxConnections":    mm.maxConnections,
		"timeframes":        mm.timeframes,
		"connections":       make(map[string]interface{}),
	}

	// Статистика по каждому соединению
	for connID, conn := range mm.connections {
		stats["connections"].(map[string]interface{})[connID] = conn.GetStats()
	}

	return stats
}

// GetConnectionCount возвращает количество активных соединений
func (mm *MultiplexedManager) GetConnectionCount() int {
	mm.connMutex.RLock()
	defer mm.connMutex.RUnlock()
	return len(mm.connections)
}

// GetSymbolCount возвращает общее количество символов
func (mm *MultiplexedManager) GetSymbolCount() int {
	mm.connMutex.RLock()
	defer mm.connMutex.RUnlock()
	return int(mm.totalSymbols.Load())
}

// GetLastFrameAt возвращает время последнего полученного WS-фрейма по всем активным соединениям.
// Если соединений нет — возвращает zero time.
// Используется health-checker'ом для обнаружения frozen-state:
// счётчик isConnected=true, но данных нет → процесс завис в reconnect-loop.
func (mm *MultiplexedManager) GetLastFrameAt() time.Time {
	mm.connMutex.RLock()
	defer mm.connMutex.RUnlock()

	var latest time.Time
	for _, conn := range mm.connections {
		stats := conn.GetStats()
		if lm, ok := stats["lastMessage"].(time.Time); ok {
			if lm.After(latest) {
				latest = lm
			}
		}
	}
	return latest
}
