package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"go-worker/internal/interfaces"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-worker/internal/monitoring"

	"io"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// Global connection limiter - limits concurrent TLS handshakes across all clients
var (
	connectionSemaphore   chan struct{}
	connectionLimiterOnce sync.Once
)

// Global reconnection coordinator - prevents simultaneous reconnections after cascade failures
var (
	reconnectionSemaphore       chan struct{}
	reconnectionCoordinatorOnce sync.Once
	globalReconnectionCounter   int64 // Глобальный счетчик переподключений для распределения во времени
	reconnectionCounterMutex    sync.Mutex
)

func initConnectionLimiter() {
	connectionLimiterOnce.Do(func() {
		// 🎯 КРИТИЧЕСКАЯ ОПТИМИЗАЦИЯ: Увеличено до 1500 для учета каскадных переподключений
		// Расчёт: 10 воркеров × до 15 соединений на воркер = до 150 одновременных попыток
		// Учитываем:
		// - Одновременный старт всех воркеров (несмотря на задержки в docker-compose)
		// - Переподключения при ошибках (до 10 попыток на соединение)
		// - **КАСКАДНЫЕ ПЕРЕПОДКЛЮЧЕНИЯ**: После i/o timeout или закрытия соединения,
		//   все соединения пытаются переподключиться одновременно, создавая пиковую нагрузку
		// - Таймауты TLS handshake (до 15 секунд) - семафор держится все это время
		// - TCP dial timeout (до 10 секунд) - семафор держится все это время
		// - Пиковые нагрузки при переподключениях всех воркеров одновременно
		// С запасом 10x (150 × 10 = 1500) для гарантированного избежания блокировок даже при каскадных переподключениях
		connectionSemaphore = make(chan struct{}, 1500)
	})
}

// initReconnectionCoordinator инициализирует глобальный координатор переподключений
// Это предотвращает одновременные переподключения после каскадных ошибок
func initReconnectionCoordinator() {
	reconnectionCoordinatorOnce.Do(func() {
		// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Ограничиваем количество одновременных переподключений
		// Проблема: После i/o timeout или закрытия соединения, все соединения пытаются переподключиться одновременно
		// Решение: Глобальный семафор ограничивает количество одновременных попыток переподключения
		// Размер 50 означает, что одновременно может переподключаться максимум 50 соединений
		// Остальные будут ждать в очереди, предотвращая каскадные ошибки
		reconnectionSemaphore = make(chan struct{}, 50)
	})
}

// MultiplexedWSConfig конфигурация для multiplexed WebSocket соединения
type MultiplexedWSConfig struct {
	Symbols    []string      // Список символов для подписки
	Timeframe  string        // Таймфрейм для всех символов
	MaxRetries int           // Максимальное количество попыток переподключения
	RetryDelay time.Duration // Задержка между попытками
}

// MultiplexedWSClient клиент для multiplexed WebSocket соединений
type MultiplexedWSClient struct {
	config    *MultiplexedWSConfig
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	isRunning bool
	mutex     sync.RWMutex

	// Redis клиент для публикации данных
	redisClient *redis.Client

	// Статистика
	messageCount        int64
	lastMessage         time.Time
	lastPingTime        time.Time // Время последнего отправленного ping
	pingCount           int64     // Счетчик ping сообщений
	errorCount          int64     // Счетчик ошибок для фильтрации логов
	connectionCount     int64     // Счетчик подключений для фильтрации логов
	apiCheckCount       int64     // Счетчик проверок API для фильтрации логов
	connectAttemptCount int64     // Счетчик попыток подключения для фильтрации логов
	closeCount          int64     // Счетчик закрытий соединения для фильтрации логов
	pingSendCount       int64     // Счетчик отправки ping для фильтрации логов

	// Трекинг последовательных неудач
	consecutiveFailures int64
	lastSuccessTime     time.Time

	// Callback для публикации данных свечей
	candlePublisher interfaces.CandlePublisher

	// Мониторинг
	monitor *monitoring.WebSocketMonitor
}

// getReadTimeoutForTimeframe возвращает таймаут чтения в зависимости от таймфрейма
func getReadTimeoutForTimeframe(timeframe string) time.Duration {
	// Базовый таймаут из ENV (по умолчанию 300с)
	baseReadTimeout := getEnvDuration("FUTURES_WS_READ_TIMEOUT", 300*time.Second)

	// 🎯 ИСПРАВЛЕНИЕ: case-sensitive проверки для месячных таймфреймов Binance.
	//
	// Проблема (P3 bug): strings.ToLower() создаёт коллизию:
	//   "kline_3M" (3 months) → "kline_3m" → совпадает с паттерном "3m" (3 minutes!)
	//   "kline_1M" (1 month)  → "kline_1m" → совпадает с паттерном "1m" (1 minute!)
	//
	// Решение: проверяем регистрозависимые суффиксы ЗАГЛАВНЫМИ БУКВАМИ до ToLower.
	// Порядок важен: более длинные суффиксы проверяем первыми.
	//
	// Binance кейсы (заглавные суффиксы = долгосрочные таймфреймы):
	//   1y  = 1 year   (~1200s readTimeout)
	//   3M  = 3 months (~900s)
	//   1M  = 1 month  (~600s)
	//   1w  = 1 week   (~450s) — строчная 'w', нет коллизии
	//   1d  = 1 day    (~400s) — строчная 'd', нет коллизии
	//   1h  = 1 hour   — base timeout (нет коллизии)

	// --- Регистрозависимые проверки (заглавные суффиксы = долгосрочные TF) ---
	if strings.Contains(timeframe, "1y") || strings.Contains(timeframe, "1Y") {
		return max(baseReadTimeout, 1200*time.Second)
	}

	// "3M" = 3 months. ВАЖНО: проверяем до ToLower чтобы не спутать с "3m" (3 minutes).
	if strings.Contains(timeframe, "3M") {
		return max(baseReadTimeout, 900*time.Second)
	}

	// "1M" = 1 month. ВАЖНО: проверяем до ToLower чтобы не спутать с "1m" (1 minute).
	if strings.Contains(timeframe, "1M") {
		return max(baseReadTimeout, 600*time.Second)
	}

	// --- Регистронезависимые проверки для строчных суффиксов (нет коллизий) ---
	tfLower := strings.ToLower(timeframe)

	if strings.Contains(tfLower, "1w") || strings.Contains(tfLower, "_1w") {
		return max(baseReadTimeout, 450*time.Second)
	}

	if strings.Contains(tfLower, "1d") || strings.Contains(tfLower, "_1d") {
		return max(baseReadTimeout, 400*time.Second)
	}

	return baseReadTimeout
}

func getMultiplexedWriteWait() time.Duration {
	return getEnvDuration("FUTURES_WS_WRITE_WAIT", 10*time.Second)
}

func getMultiplexedPingPeriod() time.Duration {
	return getEnvDuration("FUTURES_WS_PING_PERIOD", 20*time.Second)
}

// NewMultiplexedWSClient создает новый клиент для multiplexed WebSocket
func NewMultiplexedWSClient(config *MultiplexedWSConfig, redisClient *redis.Client, candlePublisher interfaces.CandlePublisher, monitor *monitoring.WebSocketMonitor) *MultiplexedWSClient {
	// 🎯 ОПТИМИЗАЦИЯ: Используем context.Background() с таймаутом для более гибкого управления
	// Контекст отменяется только при явном вызове Stop(), а не при временных ошибках
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize connection limiter
	initConnectionLimiter()

	return &MultiplexedWSClient{
		config:              config,
		ctx:                 ctx,
		cancel:              cancel,
		redisClient:         redisClient,
		candlePublisher:     candlePublisher,
		monitor:             monitor,
		isRunning:           false,
		messageCount:        0,
		lastMessage:         time.Now(),
		lastPingTime:        time.Now(), // Инициализируем время последнего ping
		pingCount:           0,
		errorCount:          0,
		connectionCount:     0,
		apiCheckCount:       0,
		connectAttemptCount: 0,
		closeCount:          0,
		pingSendCount:       0,
		consecutiveFailures: 0,
		lastSuccessTime:     time.Now(),
	}
}

// Start запускает multiplexed WebSocket соединение
func (mwc *MultiplexedWSClient) Start() error {
	mwc.mutex.Lock()
	if mwc.isRunning {
		mwc.mutex.Unlock()
		return fmt.Errorf("клиент уже запущен")
	}
	mwc.isRunning = true
	mwc.mutex.Unlock()

	// СТАРТ: Оставляем логирование запуска
	// Закомментировано для уменьшения шума в логах
	// zap.S().Infof("🚀 Запуск multiplexed WebSocket для %d символов с таймфреймом %s",
	// 	len(mwc.config.Symbols), mwc.config.Timeframe)

	// Запускаем в горутине
	go mwc.run()

	return nil
}

// Stop останавливает клиент
func (mwc *MultiplexedWSClient) Stop() {
	mwc.mutex.Lock()
	defer mwc.mutex.Unlock()

	if !mwc.isRunning {
		return
	}

	// zap.S().Infof("🛑 Остановка multiplexed WebSocket клиента")

	mwc.isRunning = false
	mwc.cancel()

	if mwc.conn != nil {
		mwc.conn.Close()
	}
}

// run основной цикл работы клиента
func (mwc *MultiplexedWSClient) run() {
	retryCount := 0

	// 🎯 ВОЗВРАЩЕНО К РАБОЧЕМУ КОДУ из коммита f31310c (где не было ошибок)
	for mwc.isRunning {
		// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Координация переподключений через глобальный семафор
		// Это предотвращает одновременные переподключения после каскадных ошибок
		initReconnectionCoordinator()

		// Получаем слот для переподключения (ограничивает количество одновременных попыток)
		select {
		case reconnectionSemaphore <- struct{}{}:
			shouldReturn := func() bool {
				// Гарантируем освобождение слота в конце каждой итерации
				defer func() { <-reconnectionSemaphore }()

				// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Добавляем глобальную задержку перед переподключением
				// Это распределяет переподключения во времени, предотвращая одновременные попытки
				// Задержка зависит от глобального счетчика переподключений
				reconnectionCounterMutex.Lock()
				globalReconnectionCounter++
				mySlot := globalReconnectionCounter
				reconnectionCounterMutex.Unlock()

				// Задержка зависит от номера слота: каждое переподключение ждет дольше
				// Это создает распределение во времени: 0s, 2s, 4s, 6s, ...
				slotDelay := time.Duration((mySlot-1)*2) * time.Second
				// Добавляем случайную задержку от 0 до 10 секунд для дополнительного распределения
				randomDelay := time.Duration(rand.Int63n(11)) * time.Second
				totalPreReconnectDelay := slotDelay + randomDelay

				// Максимальная задержка - 30 секунд, чтобы не ждать слишком долго
				if totalPreReconnectDelay > 30*time.Second {
					totalPreReconnectDelay = 30 * time.Second
				}

				// Ждем перед попыткой переподключения для распределения нагрузки
				if totalPreReconnectDelay > 0 {
					select {
					case <-time.After(totalPreReconnectDelay):
						// Continue to reconnection attempt
					case <-mwc.ctx.Done():
						return true
					}
				}

				// Теперь пытаемся переподключиться
				err := mwc.connectAndHandle()

				if err != nil {
					errStr := err.Error()

					// 🎯 ИСПРАВЛЕНИЕ: Игнорируем ошибки остановки клиента - это нормальная ситуация
					if strings.Contains(errStr, "use of closed network connection") ||
						strings.Contains(errStr, "client is not running") ||
						strings.Contains(errStr, "client stopped while acquiring connection slot") {
						// Просто выходим, это нормальное завершение работы
						mwc.mutex.RLock()
						if !mwc.isRunning {
							mwc.mutex.RUnlock()
							return true
						}
						mwc.mutex.RUnlock()
						return false
					}

					// 🎯 ИСПРАВЛЕНИЕ: Проверяем контекст перед обработкой ошибки
					// Если контекст отменен и клиент остановлен, это не ошибка
					select {
					case <-mwc.ctx.Done():
						mwc.mutex.RLock()
						if !mwc.isRunning {
							mwc.mutex.RUnlock()
							return true // Нормальное завершение работы
						}
						mwc.mutex.RUnlock()
						// Контекст отменен, но клиент еще работает - продолжаем
					default:
						// Контекст не отменен, обрабатываем ошибку
					}

					// Для всех остальных ошибок увеличиваем счетчики
					retryCount++
					currentErrorCount := atomic.AddInt64(&mwc.errorCount, 1)
					failures := atomic.AddInt64(&mwc.consecutiveFailures, 1)

					// После 5 подряд идущих неудач отправляем алерт в Prometheus + Telegram
					if failures == 5 {
						if mwc.redisClient != nil {
							mwc.redisClient.XAdd(context.Background(), &redis.XAddArgs{
								Stream: "notify:telegram:crit",
								Values: map[string]interface{}{
									"message": fmt.Sprintf("🚨 Binance WS Alert: 5 consecutive reconnect failures for %d symbols, timeframe: %s", len(mwc.config.Symbols), mwc.config.Timeframe),
								},
							})
						}
						// Если есть мониторинг, можно записать критическую ошибку
						if mwc.monitor != nil {
							for _, symbol := range mwc.config.Symbols {
								mwc.monitor.RecordError(symbol, mwc.config.Timeframe, "critical_reconnect_failure")
							}
						}
					}

					// 🎯 УЛУЧШЕНИЕ: Умное логирование в зависимости от типа ошибки
					// Классификация ошибок для лучшей наблюдаемости
					var errorType string
					var errorCategory string

					isConnectionReset := strings.Contains(errStr, "connection reset by peer")
					isAbnormalClosure := strings.Contains(errStr, "1006") ||
						strings.Contains(errStr, "abnormal closure") ||
						strings.Contains(errStr, "unexpected EOF")
					isConnectionClosed := strings.Contains(errStr, "connection closed") && !isConnectionReset
					isTimeout := strings.Contains(errStr, "i/o timeout") ||
						strings.Contains(errStr, "timeout")
					isContextCancelled := strings.Contains(errStr, "context cancelled")

					// Определяем тип ошибки для метрик
					if isContextCancelled {
						// Не логируем и не увеличиваем счетчики - это нормальное завершение
						return true
					} else if isConnectionReset {
						errorType = "connection_reset"
						errorCategory = "connection reset"
					} else if isAbnormalClosure {
						errorType = "abnormal_closure"
						errorCategory = "abnormal closure"
					} else if isConnectionClosed {
						errorType = "connection_closed"
						errorCategory = "connection closed"
					} else if isTimeout {
						errorType = "timeout"
						errorCategory = "timeout"
					} else {
						errorType = "other_error"
						errorCategory = "other"
					}

					// Для connection reset, abnormal closure и timeout логируем реже (каждую 1000-ю), так как это нормальная ситуация
					// Для других ошибок логируем чаще (каждую 100-ю)
					isNormalNetworkError := isConnectionReset || isAbnormalClosure || isConnectionClosed || isTimeout
					if isNormalNetworkError {
						// Connection reset, abnormal closure и timeout - это нормальные сетевые ошибки, Binance может закрывать соединения
						// Логируем реже, чтобы не засорять логи (каждую 1000-ю ошибку или первые 5)
						if currentErrorCount <= 5 || currentErrorCount%1000 == 0 {
							zap.S().Errorf("⚠️ Ошибка multiplexed WebSocket (%s, попытка %d/%d, всего ошибок: %d, подряд: %d): %v",
								errorCategory, retryCount, mwc.config.MaxRetries, currentErrorCount, failures, err)
						}
					} else {
						// Для других ошибок логируем чаще (каждую 100-ю)
						if currentErrorCount <= 5 || currentErrorCount%100 == 0 {
							zap.S().Errorf("❌ Ошибка multiplexed WebSocket (%s, попытка %d/%d, всего ошибок: %d, подряд: %d): %v",
								errorCategory, retryCount, mwc.config.MaxRetries, currentErrorCount, failures, err)
						}
					}

					// Записываем ошибку в мониторинг для всех символов с конкретным типом ошибки
					if mwc.monitor != nil {
						for _, symbol := range mwc.config.Symbols {
							mwc.monitor.RecordError(symbol, mwc.config.Timeframe, errorType)
							mwc.monitor.RecordReconnection(symbol, mwc.config.Timeframe, "connection_failed")
						}
					}

					// Метрика: агрегированный счётчик переподключений (ws_reconnects_total)
					monitoring.WsReconnectsTotal.WithLabelValues("binance", mwc.config.Timeframe, errorType).Inc()
					monitoring.RecordWSDisconnect("binance", errorType)

					if retryCount >= mwc.config.MaxRetries {
						// ОШИБКА: Оставляем логирование достижения максимума попыток (только каждое 10000-е)
						if currentErrorCount%10000 == 0 {
							zap.S().Errorf("⚠️ Достигнут максимум попыток (%d), но продолжаем работу... (всего ошибок: %d)",
								mwc.config.MaxRetries, currentErrorCount)
						}
						// Сбрасываем счетчик и продолжаем работу
						retryCount = 0
					}

					// Exponential backoff
					baseDelay := mwc.config.RetryDelay * time.Duration(1<<uint(retryCount))
					if baseDelay > 60*time.Second {
						baseDelay = 60 * time.Second
					}

					// ±25% jitter для избежания thundering herd
					jitter := time.Duration(float64(baseDelay) * 0.25)
					delay := baseDelay
					jitterRange := int64(jitter * 2)
					if jitterRange > 0 {
						delay = baseDelay + time.Duration(rand.Int63n(jitterRange)-int64(jitter))
					}
					totalDelay := delay

					// Check for context cancellation during sleep
					select {
					case <-time.After(totalDelay):
						// Continue to next retry
					case <-mwc.ctx.Done():
						return true
					}
				} else {
					// Сбрасываем счетчик при успешном подключении
					retryCount = 0
					atomic.StoreInt64(&mwc.consecutiveFailures, 0)
					mwc.lastSuccessTime = time.Now()
				}

				return false
			}()

			if shouldReturn {
				return
			}
		case <-mwc.ctx.Done():
			return
		}
	}

	mwc.mutex.Lock()
	mwc.isRunning = false
	mwc.mutex.Unlock()
}

// connectAndHandle устанавливает соединение и обрабатывает сообщения
func (mwc *MultiplexedWSClient) connectAndHandle() error {
	// 🎯 ВОЗВРАЩЕНО К РАБОЧЕМУ КОДУ из коммита f31310c (где не было ошибок)
	// Acquire semaphore to limit concurrent connections
	// 🎯 ИСПРАВЛЕНИЕ: Сначала проверяем isRunning, затем пытаемся получить семафор
	// Это предотвращает попытки подключения если клиент уже остановлен
	mwc.mutex.RLock()
	isRunning := mwc.isRunning
	mwc.mutex.RUnlock()

	if !isRunning {
		return fmt.Errorf("client is not running")
	}

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Проверка API health ПЕРЕД получением семафора
	// Это предотвращает удержание семафора во время проверки (до 10 секунд)
	// Делаем быструю проверку (2 секунды) и продолжаем даже при ошибке
	apiCheckCounter := atomic.AddInt64(&mwc.apiCheckCount, 1)
	if err := mwc.checkBinanceAPIHealthFast(); err != nil {
		// Логируем только каждое 10000-е, но продолжаем попытку подключения
		if apiCheckCounter%10000 == 0 {
			zap.S().Errorf("⚠️ Binance API недоступен (быстрая проверка): %v (всего проверок: %d)", err, apiCheckCounter)
		}
		// Продолжаем попытку подключения - проверка не критична
	}

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Добавляем задержку ПЕРЕД попыткой получить семафор при переподключении
	// Это предотвращает одновременные попытки получения семафора после каскадных ошибок
	// Задержка зависит от количества последовательных ошибок
	consecutiveFailures := atomic.LoadInt64(&mwc.consecutiveFailures)
	if consecutiveFailures > 0 {
		// Добавляем дополнительную задержку от 1 до 5 секунд в зависимости от количества ошибок
		// Больше ошибок = больше задержка для распределения нагрузки
		failuresInt := int(consecutiveFailures)
		if failuresInt > 5 {
			failuresInt = 5 // Максимум 5 секунд
		}
		additionalDelay := time.Duration(failuresInt) * time.Second
		additionalDelay += time.Duration(rand.Intn(1000)) * time.Millisecond // ±1s случайная добавка

		select {
		case <-time.After(additionalDelay):
			// Continue to acquire semaphore
		case <-mwc.ctx.Done():
			return fmt.Errorf("context cancelled before acquiring connection slot")
		}
	}

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Улучшенная стратегия получения семафора
	// Проблема: Семафор держится во время TLS handshake (до 15s) и TCP dial (до 10s)
	// Решение: Сначала проверяем доступность немедленно, затем блокируем с короткими таймаутами
	maxTimeout := 240 * time.Second // Увеличено до 4 минут для учета времени на освобождение слотов
	startTime := time.Now()
	attempt := 0

	for {
		// Проверяем общий таймаут
		elapsed := time.Since(startTime)
		if elapsed > maxTimeout {
			return fmt.Errorf("timeout waiting for connection slot (too many concurrent connections) after %v", maxTimeout)
		}

		// Немедленная попытка (неблокирующая) - семафор мог освободиться
		select {
		case connectionSemaphore <- struct{}{}:
			// Got semaphore immediately, proceed with connection
			defer func() { <-connectionSemaphore }()

			// Проверяем isRunning после получения семафора
			mwc.mutex.RLock()
			isRunning = mwc.isRunning
			mwc.mutex.RUnlock()

			if !isRunning {
				return fmt.Errorf("client stopped while acquiring connection slot")
			}

			// Успешно получили семафор, выходим из цикла retry
			goto semaphoreAcquired

		case <-mwc.ctx.Done():
			return fmt.Errorf("context cancelled before acquiring connection slot")

		default:
			// Семафор занят, используем блокирующую попытку с адаптивным таймаутом
			attempt++

			// Адаптивный таймаут: начинаем с 1 секунды, увеличиваем до 5 секунд
			timeout := 1 * time.Second
			if elapsed > 30*time.Second {
				timeout = 3 * time.Second
			}
			if elapsed > 60*time.Second {
				timeout = 5 * time.Second
			}
			if elapsed > 120*time.Second {
				timeout = 10 * time.Second // Дольше после 2 минут ожидания
			}

			select {
			case connectionSemaphore <- struct{}{}:
				// Got semaphore after waiting, proceed with connection
				defer func() { <-connectionSemaphore }()

				// Проверяем isRunning после получения семафора
				mwc.mutex.RLock()
				isRunning = mwc.isRunning
				mwc.mutex.RUnlock()

				if !isRunning {
					return fmt.Errorf("client stopped while acquiring connection slot")
				}

				// Успешно получили семафор, выходим из цикла retry
				goto semaphoreAcquired

			case <-mwc.ctx.Done():
				return fmt.Errorf("context cancelled before acquiring connection slot")

			case <-time.After(timeout):
				// Таймаут попытки, проверяем isRunning и продолжаем
				mwc.mutex.RLock()
				isRunning = mwc.isRunning
				mwc.mutex.RUnlock()

				if !isRunning {
					return fmt.Errorf("client stopped while acquiring connection slot")
				}

				// Добавляем jitter задержку перед следующей попыткой для распределения нагрузки
				// Задержка зависит от номера попытки: 200ms, 400ms, 600ms, max 1s
				jitter := time.Duration(min(attempt*200, 1000)) * time.Millisecond
				jitter += time.Duration(rand.Intn(100)) * time.Millisecond // ±100ms случайная добавка

				select {
				case <-time.After(jitter):
					// Continue to next attempt
				case <-mwc.ctx.Done():
					return fmt.Errorf("context cancelled before acquiring connection slot")
				}
			}
		}
	}

semaphoreAcquired:

	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Проверка API health уже выполнена ПЕРЕД получением семафора
	// Теперь семафор используется только для TLS handshake и установки соединения
	// Это значительно сокращает время удержания семафора

	// Формируем URL для multiplexed stream
	streams := make([]string, len(mwc.config.Symbols))
	for i, symbol := range mwc.config.Symbols {
		streams[i] = fmt.Sprintf("%s@%s", strings.ToLower(symbol), mwc.config.Timeframe)
	}

	streamParam := strings.Join(streams, "/")
	url := fmt.Sprintf("wss://fstream.binance.com/stream?streams=%s", streamParam)

	// Закомментировано: информационное сообщение о подключении
	// connectCounter := atomic.AddInt64(&mwc.connectAttemptCount, 1)
	// if connectCounter == 1 || connectCounter%10000 == 0 {
	// 	zap.S().Infof("🔌 Подключение к multiplexed stream: %s (попытка #%d)", url, connectCounter)
	// }

	// Создаем настраиваемый dialer с настроенными таймаутами
	// 🎯 УВЕЛИЧЕНО для предотвращения i/o timeout при высокой нагрузке сети
	handshakeTimeout := getEnvDuration("WS_HANDSHAKE_TIMEOUT", 45*time.Second)
	dialTimeout := getEnvDuration("WS_DIAL_TIMEOUT", 30*time.Second)
	keepAlive := getEnvDuration("WS_TCP_KEEPALIVE", 60*time.Second)

	dialer := websocket.Dialer{
		HandshakeTimeout: handshakeTimeout,
		Proxy:            http.ProxyFromEnvironment,
		// Отключаем сжатие для лучшей стабильности
		EnableCompression: false,
		// Улучшаем настройки TCP соединения
		NetDialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: keepAlive,
			DualStack: true, // Поддержка IPv4 и IPv6
		}).DialContext,
	}

	// Устанавливаем WebSocket соединение с заголовками для лучшей совместимости
	headers := http.Header{
		"User-Agent": []string{"Mozilla/5.0 (compatible; BinanceWebSocket/1.0)"},
		"Accept":     []string{"*/*"},
		"Origin":     []string{"https://www.binance.com"},
	}

	conn, resp, err := dialer.Dial(url, headers)
	if err != nil {
		// ОШИБКА: Оставляем детальное логирование ошибки подключения
		// 🎯 УПРОЩЕНО: возвращено к простой обработке ошибок из рабочей версии
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("ошибка подключения к WebSocket (HTTP %d): %v, ответ: %s",
				resp.StatusCode, err, string(body))
		}
		return fmt.Errorf("ошибка подключения к WebSocket: %v", err)
	}

	// Настраиваем дополнительные параметры соединения
	conn.SetReadLimit(1024 * 1024) // Увеличиваем лимит до 1MB

	// Настраиваем TCP keepalive если возможно
	if tcpConn, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(keepAlive)
		tcpConn.SetLinger(0)
		// Добавляем настройки для лучшей стабильности
		tcpConn.SetNoDelay(true)
		tcpConn.SetWriteBuffer(64 * 1024) // 64KB буфер записи
		tcpConn.SetReadBuffer(64 * 1024)  // 64KB буфер чтения
	}

	mwc.mutex.Lock()
	mwc.conn = conn
	mwc.lastPingTime = time.Now() // Инициализируем время последнего ping при подключении
	mwc.mutex.Unlock()

	defer func() {
		mwc.mutex.Lock()
		if mwc.conn != nil {
			mwc.conn.Close()
			mwc.conn = nil
		}
		mwc.mutex.Unlock()
	}()

	// Настраиваем ping/pong для поддержания соединения
	conn.SetPingHandler(func(appData string) error {
		// Увеличиваем счетчик ping
		atomic.AddInt64(&mwc.pingCount, 1)

		// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Сбрасываем ReadDeadline при получении ping от Binance
		// Это предотвращает таймауты чтения, даже если нет данных в течение долгого времени
		// Ping/pong показывают, что соединение активно, поэтому таймаут должен сбрасываться
		// Используем адаптивный таймаут в зависимости от таймфрейма
		readTimeout := getReadTimeoutForTimeframe(mwc.config.Timeframe)
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		// Записываем активность для всех символов данного мультиплекса
		if mwc.monitor != nil {
			for _, symbol := range mwc.config.Symbols {
				mwc.monitor.RecordActivity(symbol, mwc.config.Timeframe)
			}
		}

		// Отправляем pong с увеличенным таймаутом (используем WriteControl для надежности)
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(getMultiplexedWriteWait()))
	})

	conn.SetPongHandler(func(appData string) error {
		// Увеличиваем счетчик pong
		atomic.AddInt64(&mwc.pingCount, 1)

		// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Сбрасываем ReadDeadline при получении pong от Binance
		// Это подтверждает, что соединение активно и Binance получает наши ping сообщения
		// Без сброса ReadDeadline, соединение будет разорвано даже при активном ping/pong обмене
		// Используем адаптивный таймаут в зависимости от таймфрейма
		readTimeout := getReadTimeoutForTimeframe(mwc.config.Timeframe)
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		// Обновляем время последнего ping при получении pong (подтверждение активности)
		mwc.mutex.Lock()
		mwc.lastPingTime = time.Now()
		mwc.mutex.Unlock()

		// Записываем активность для всех символов данного мультиплекса
		if mwc.monitor != nil {
			for _, symbol := range mwc.config.Symbols {
				mwc.monitor.RecordActivity(symbol, mwc.config.Timeframe)
			}
		}

		// Закомментировано: информационное сообщение о pong
		// pongCounter := atomic.AddInt64(&mwc.pingCount, 1)
		// if pongCounter%10000 == 0 {
		// 	zap.S().Infof("🏓 Получен pong #%d", pongCounter)
		// }

		return nil
	})

	// Добавляем обработчик закрытия соединения
	conn.SetCloseHandler(func(code int, text string) error {
		closeCounter := atomic.AddInt64(&mwc.closeCount, 1)
		// ОШИБКА: Оставляем логирование закрытия соединения
		if closeCounter == 1 || closeCounter%10000 == 0 {
			zap.S().Infof("🔌 WebSocket соединение закрыто: код=%d, причина=%s (всего закрытий: %d)", code, text, closeCounter)
		}
		return nil
	})

	// Увеличиваем счетчик подключений
	_ = atomic.AddInt64(&mwc.connectionCount, 1)

	// СТАРТ: Оставляем логирование успешного подключения (только первое и каждое 10000-е)
	// Закомментировано для уменьшения шума в логах
	// if currentConnectionCount == 1 || currentConnectionCount%10000 == 0 {
	// 	zap.S().Infof("✅ Multiplexed WebSocket соединение установлено для %d символов (подключение #%d)",
	// 		len(mwc.config.Symbols), currentConnectionCount)
	// 	// log.Printf("📊 Ping логирование: каждое 10000-е сообщение")
	// 	// log.Printf("🔧 Настройки соединения: ReadTimeout=180s, PingInterval=20s, MaxRetries=%d", mwc.config.MaxRetries)
	// }

	// Записываем подключение в мониторинг для каждого символа
	if mwc.monitor != nil {
		for _, symbol := range mwc.config.Symbols {
			mwc.monitor.RecordConnection(symbol, mwc.config.Timeframe)
		}
	}

	// Обрабатываем сообщения
	return mwc.handleMessages()
}

// handleMessages обрабатывает входящие сообщения
func (mwc *MultiplexedWSClient) handleMessages() (retErr error) {
	// Panic guard: любая паника внутри (gorilla readErrCount overflow, json-parser,
	// processMessage и др.) конвертируется в error, чтобы connectAndHandle мог
	// выполнить нормальный reconnect вместо падения всей горутины.
	defer func() {
		if r := recover(); r != nil {
			zap.S().Errorf("🔥 handleMessages panic recovered (reconnect triggered): %v", r)
			retErr = fmt.Errorf("panic in handleMessages: %v", r)
		}
	}()

	// Запускаем периодическую отправку ping в отдельной горутине,
	// чтобы блокирующий вызов ReadMessage() не мешал отправлять ping'и вовремя.
	pingDone := make(chan struct{})
	defer close(pingDone)

	go func() {
		pingTicker := time.NewTicker(getMultiplexedPingPeriod())
		defer pingTicker.Stop()

		for {
			select {
			case <-pingDone:
				return
			case <-mwc.ctx.Done():
				return
			case <-pingTicker.C:
				mwc.mutex.RLock()
				conn := mwc.conn
				mwc.mutex.RUnlock()

				if conn == nil {
					continue
				}

				pingSendCounter := atomic.AddInt64(&mwc.pingSendCount, 1)
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(getMultiplexedWriteWait())); err != nil {
					if pingSendCounter == 1 || pingSendCounter%10000 == 0 {
						zap.S().Errorf("⚠️ Ошибка отправки ping: %v (всего попыток: %d)", err, pingSendCounter)
					}
					// Не прерываем чтение, цикл ReadMessage обработает разорванное соединение
				} else {
					// SetReadDeadline обновляется в PongHandler при получении ответа
					atomic.AddInt64(&mwc.pingCount, 1)
					mwc.mutex.Lock()
					mwc.lastPingTime = time.Now()
					mwc.mutex.Unlock()
				}
			}
		}
	}()

	// Читаем isRunning под блокировкой, чтобы избежать data race.
	running := func() bool {
		mwc.mutex.RLock()
		defer mwc.mutex.RUnlock()
		return mwc.isRunning
	}

	for running() {
		// Устанавливаем адаптивный таймаут для чтения в зависимости от таймфрейма
		// 🎯 УЛУЧШЕНИЕ: Адаптивный таймаут для долгосрочных таймфреймов (3M, 1y)
		// Для долгосрочных таймфреймов свечи приходят редко, поэтому нужен больший таймаут
		readTimeout := getReadTimeoutForTimeframe(mwc.config.Timeframe)
		if err := mwc.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("ошибка установки таймаута: %v", err)
		}

		// NOTE: time.Sleep(1ms) removed by latency audit — ReadMessage() blocks
		// on I/O natively; the sleep added 1ms floor latency per message,
		// making >1000 msg/s throughput impossible.

		_, message, err := mwc.conn.ReadMessage()
		if err != nil {
			// ОШИБКА: Проверяем тип ошибки для лучшей диагностики
			// 🎯 УПРОЩЕНО: возвращено к простой обработке ошибок из рабочей версии
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				return fmt.Errorf("неожиданное закрытие WebSocket: %v", err)
			}

			// 🎯 ИСПРАВЛЕНИЕ ПАНИКИ: После любой ошибки ReadMessage (включая Timeout)
			// НЕЛЬЗЯ делать continue — gorilla/websocket кеширует readErr и при повторных
			// вызовах ReadMessage инкрементирует readErrCount; при >=1000 вызывает panic.
			// Возвращаем ошибку для нормального реконнекта.
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				currentErrorCount := atomic.AddInt64(&mwc.errorCount, 1)
				if currentErrorCount <= 5 || currentErrorCount%1000 == 0 {
					mwc.mutex.RLock()
					timeSinceLastMsg := time.Since(mwc.lastMessage)
					mwc.mutex.RUnlock()
					zap.S().Errorf("⚠️ Таймаут чтения WebSocket (последнее сообщение: %v назад, всего ошибок: %d): %v",
						timeSinceLastMsg, currentErrorCount, err)
				}
				return fmt.Errorf("таймаут чтения WebSocket: %v", err)
			}

			return fmt.Errorf("ошибка чтения сообщения: %v", err)
		}

		// Обрабатываем сообщение
		if err := mwc.processMessage(message); err != nil {
			// ОШИБКА: Оставляем логирование ошибок обработки
			zap.S().Errorf("⚠️ Ошибка обработки сообщения: %v", err)
			continue
		}

		mwc.messageCount++
		mwc.mutex.Lock()
		mwc.lastMessage = time.Now()
		mwc.mutex.Unlock()

		// Метрика: суммарный счётчик входящих WS-сообщений
		monitoring.WsMessagesTotal.WithLabelValues("binance", mwc.config.Timeframe).Inc()

		// NOTE: duplicate SetReadDeadline removed by latency audit.
		// The deadline is already set at the top of the loop (line 842);
		// resetting it here was redundant and cost ~2000 extra syscalls/s.

		// Записываем сообщение в мониторинг
		if mwc.monitor != nil {
			// Извлекаем символ из stream (например, "btcusdt@kline_1m" -> "BTCUSDT")
			stream := mwc.extractStreamFromMessage(message)
			if stream != "" {
				symbol := mwc.extractSymbolFromStream(stream)
				if symbol != "" {
					mwc.monitor.RecordMessageReceived(symbol, mwc.config.Timeframe)
				}
			}
		}

		// Закомментировано: информационное сообщение о количестве обработанных сообщений
		// if mwc.messageCount%5000 == 0 {
		// 	zap.S().Infof("📨 Multiplexed WS: обработано %d сообщений", mwc.messageCount)
		// 	zap.S().Infof("📊 Статус соединения: сообщений=%d, последнее=%v, ping=%d",
		// 		mwc.messageCount, mwc.lastMessage.Format("15:04:05"), atomic.LoadInt64(&mwc.pingCount))
		// }
	}

	return nil
}

// processMessage обрабатывает одно сообщение
func (mwc *MultiplexedWSClient) processMessage(message []byte) error {
	// Парсим JSON
	var payload map[string]interface{}
	if err := json.Unmarshal(message, &payload); err != nil {
		return fmt.Errorf("ошибка парсинга JSON: %v", err)
	}

	// Проверяем, является ли это ответом на команду или системным сообщением
	if _, hasID := payload["id"]; hasID {
		// Это ответ от сервера на команду (SUBSCRIBE/UNSUBSCRIBE)
		return nil
	}

	// Извлекаем stream и data
	stream, ok := payload["stream"].(string)
	if !ok {
		return fmt.Errorf("отсутствует поле 'stream' в сообщении (длина: %d байт)", len(message))
	}

	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("отсутствует поле 'data' в сообщении")
	}

	// Извлекаем kline данные
	klineData, hasKline := data["k"].(map[string]interface{})
	if !hasKline {
		return nil // Не kline сообщение, пропускаем
	}

	// Проверяем, закрыта ли свеча
	isClosed, hasX := klineData["x"].(bool)
	if !hasX || !isClosed {
		return nil // Свеча не закрыта, пропускаем
	}

	// Извлекаем символ из stream (например, "btcusdt@kline_1m" -> "btcusdt")
	symbol := strings.Split(stream, "@")[0]

	// Публикуем в Redis
	return mwc.publishToRedis(symbol, mwc.config.Timeframe, message, klineData)
}

// publishToRedis публикует данные в единый Redis Stream candles:data
func (mwc *MultiplexedWSClient) publishToRedis(symbol, timeframe string, message []byte, klineData map[string]interface{}) error {
	// Записываем опубликованное сообщение в мониторинг
	if mwc.monitor != nil {
		mwc.monitor.RecordPublishedMessage(symbol, timeframe)
	}

	// Публикуем только через CandlePublisher в единый stream candles:data
	if mwc.candlePublisher != nil {
		// Создаем данные свечи для единого stream
		candleData := map[string]interface{}{
			"openTime":            mwc.convertTimestamp(klineData["t"]), // Время открытия свечи
			"closeTime":           mwc.convertTimestamp(klineData["T"]), // Время закрытия свечи
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
		tf := strings.TrimPrefix(timeframe, "kline_")

		// Метрика: измеряем задержку публикации через CandlePublisher
		publishStart := time.Now()

		// Публикуем через CandlePublisher в единый stream candles:data
		if err := mwc.candlePublisher.PublishCandleData(mwc.ctx, strings.ToUpper(symbol), tf, candleData); err != nil {
			// ОШИБКА: Оставляем логирование ошибки публикации
			zap.S().Errorf("⚠️ Ошибка публикации через CandlePublisher в candles:data: %v", err)
			return fmt.Errorf("ошибка публикации в candles:data: %v", err)
		}

		// Метрика: фиксируем задержку Redis-публикации
		monitoring.RedisPublishDurationSeconds.
			WithLabelValues("binance", tf).
			Observe(time.Since(publishStart).Seconds())
	} else {
		return fmt.Errorf("CandlePublisher не инициализирован")
	}

	return nil
}

// convertTimestamp безопасно преобразует timestamp из различных типов в int64
func (mwc *MultiplexedWSClient) convertTimestamp(timestamp interface{}) int64 {
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

// checkBinanceAPIHealth проверяет доступность Binance API (полная проверка)
func (mwc *MultiplexedWSClient) checkBinanceAPIHealth() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Проверяем REST API endpoint
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.binance.com/api/v3/ping", nil)
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %v", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка HTTP запроса: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("неверный статус ответа: %d", resp.StatusCode)
	}

	return nil
}

// checkBinanceAPIHealthFast выполняет быструю проверку API (2 секунды) ПЕРЕД получением семафора
// Это предотвращает удержание семафора во время проверки
func (mwc *MultiplexedWSClient) checkBinanceAPIHealthFast() error {
	// 🎯 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Быстрая проверка (2 секунды) выполняется ДО получения семафора
	// Это предотвращает удержание семафора во время проверки API
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Проверяем REST API endpoint с коротким таймаутом
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.binance.com/api/v3/ping", nil)
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %v", err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка HTTP запроса: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("неверный статус ответа: %d", resp.StatusCode)
	}

	return nil
}

// AddSymbol dynamically adds a symbol to the existing connection
func (mwc *MultiplexedWSClient) AddSymbol(symbol string) error {
	mwc.mutex.Lock()
	defer mwc.mutex.Unlock()

	symbolLower := strings.ToLower(symbol)

	for _, s := range mwc.config.Symbols {
		if strings.ToLower(s) == symbolLower {
			return nil // Уже отслеживается
		}
	}

	// Обновляем конфиг, чтобы при переподключении символ учитывался
	mwc.config.Symbols = append(mwc.config.Symbols, symbolLower)

	// Если соединение активно, отправляем команду SUBSCRIBE
	if mwc.conn != nil && mwc.isRunning {
		streamName := fmt.Sprintf("%s@%s", symbolLower, strings.ToLower(mwc.config.Timeframe))

		payload := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": []string{streamName},
			"id":     time.Now().UnixNano() % 1000000,
		}

		// WriteJSON безопасно использовать вместе с WriteControl
		if err := mwc.conn.WriteJSON(payload); err != nil {
			zap.S().Errorf("⚠️ Ошибка отправки SUBSCRIBE для %s: %v", streamName, err)
			return err
		}
	}

	return nil
}

// GetStats возвращает статистику клиента
func (mwc *MultiplexedWSClient) GetStats() map[string]interface{} {
	mwc.mutex.RLock()
	defer mwc.mutex.RUnlock()

	return map[string]interface{}{
		"isRunning":    mwc.isRunning,
		"messageCount": mwc.messageCount,
		"lastMessage":  mwc.lastMessage,
		"symbolsCount": len(mwc.config.Symbols),
		"timeframe":    mwc.config.Timeframe,
	}
}

// IsRunning проверяет, запущен ли клиент
func (mwc *MultiplexedWSClient) IsRunning() bool {
	mwc.mutex.RLock()
	defer mwc.mutex.RUnlock()
	return mwc.isRunning
}

// extractStreamFromMessage извлекает stream из JSON сообщения
func (mwc *MultiplexedWSClient) extractStreamFromMessage(message []byte) string {
	var payload map[string]interface{}
	if err := json.Unmarshal(message, &payload); err != nil {
		return ""
	}

	if stream, ok := payload["stream"].(string); ok {
		return stream
	}
	return ""
}

// extractSymbolFromStream извлекает символ из stream (например, "btcusdt@kline_1m" -> "BTCUSDT")
func (mwc *MultiplexedWSClient) extractSymbolFromStream(stream string) string {
	parts := strings.Split(stream, "@")
	if len(parts) > 0 {
		return strings.ToUpper(parts[0])
	}
	return ""
}
