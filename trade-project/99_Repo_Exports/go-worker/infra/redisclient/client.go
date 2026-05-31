// Пакет redisclient с улучшенными настройками стабильности и retry логикой
package redisclient

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// Ctx - контекст для операций с Redis
var Ctx = context.Background()

// Client - экземпляр клиента Redis (основной Redis 6379)
var Client *redis.Client

// ClientWorker - клиент для redis-worker-1 (порт 6380 снаружи, 6379 внутри контейнера)
var ClientWorker *redis.Client

// ClientTicks - клиент для redis-ticks (отдельное хранилище тиков)
var ClientTicks *redis.Client

// Счетчики подключений для уменьшения количества логов
var (
	connectionCounter       uint64
	connectionWorkerCounter uint64
	connectionTicksCounter  uint64
)

const (
	redisErrorLogInterval = 1000
)

// getEnvInt parses an integer environment variable, falling back to defaultVal on error.
func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultVal
}

type errorLogState struct {
	total      atomic.Uint64
	lastLogged atomic.Uint64
}

var redisErrorLogCounters sync.Map

func shouldLogRedisError(key string) (bool, uint64) {
	stateValue, loaded := redisErrorLogCounters.Load(key)
	if !loaded {
		newState := &errorLogState{}
		stateValue, loaded = redisErrorLogCounters.LoadOrStore(key, newState)
		if !loaded {
			stateValue = newState
		}
	}

	state := stateValue.(*errorLogState)
	count := state.total.Add(1)

	if count == 1 {
		state.lastLogged.Store(count)
		return true, 0
	}

	if count%redisErrorLogInterval != 0 {
		return false, 0
	}

	prev := state.lastLogged.Swap(count)
	if prev >= count {
		return true, 0
	}

	suppressed := count - prev - 1
	return true, suppressed
}

// Circuit breaker для защиты от каскадных сбоев
type CircuitBreaker struct {
	mu           sync.RWMutex
	failureCount int
	lastFailTime time.Time
	state        string // "closed", "open", "half-open"
	threshold    int
	timeout      time.Duration
	inProbe      atomic.Bool // guards exactly one probe in half-open
}

const (
	StateClosed   = "closed"
	StateOpen     = "open"
	StateHalfOpen = "half-open"
)

// NewCircuitBreaker создает новый circuit breaker
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
		state:     StateClosed,
	}
}

// CanExecute проверяет, можно ли выполнить операцию и управляет переходом в HalfOpen.
// В состоянии HalfOpen ровно один запрос-зонд пропускается через атомарный флаг;
// остальные блокируются до разрешения зонда (RecordSuccess/RecordFailure сбрасывают флаг).
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastFailTime) > cb.timeout {
			cb.state = StateHalfOpen
			cb.inProbe.Store(false) // сбрасываем перед первым зондом
			// fall through to HalfOpen handling below
			if cb.inProbe.CompareAndSwap(false, true) {
				zap.S().Infof("🟡 Circuit breaker → HalfOpen: отправляем пробный запрос")
				return true
			}
			return false
		}
		return false
	case StateHalfOpen:
		// Только один зонд за раз; остальные отклоняются
		if cb.inProbe.CompareAndSwap(false, true) {
			zap.S().Infof("🟡 Circuit breaker HalfOpen: отправляем пробный запрос")
			return true
		}
		return false
	default:
		return false
	}
}

// RecordSuccess записывает успешную операцию и закрывает breaker
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Только если мы были НЕ в StateClosed, логируем восстановление
	if cb.state != StateClosed {
		zap.S().Infof("✅ Circuit breaker → Closed (восстановлен)")
	}
	cb.failureCount = 0
	cb.state = StateClosed
	cb.inProbe.Store(false)
}

// RecordFailure записывает неудачную операцию
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailTime = time.Now()
	cb.inProbe.Store(false) // разрешаем следующий зонд после таймаута

	if cb.failureCount >= cb.threshold {
		if cb.state != StateOpen {
			zap.S().Errorf("🔴 Circuit breaker → Open после %d неудач", cb.failureCount)
		}
		cb.state = StateOpen
	}
}

// init инициализирует клиент Redis с улучшенными настройками стабильности
func init() {
	// Получаем хост, порт, пользователь и пароль Redis из переменных окружения или используем значения по умолчанию
	redisHost := getEnv("REDIS_HOST", "scanner-redis-worker-1")
	redisPort := getEnv("REDIS_PORT", "6379")
	redisUser := getEnv("REDIS_USERNAME", "") // ACL-пользователь: пусто = использует default учетную запись
	redisPass := getEnv("REDIS_PASSWORD", "") // ACL-пароль: пусто = без аутентификации (обратная совместимость)
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)

	// Тюнинг пула: ENV-overridable — меньшие значения экономят соединения при сохранении запаса
	// Formula: ~30-50 concurrent goroutines per go-worker instance at peak load
	mainPoolSize := getEnvInt("REDIS_POOL_SIZE", 60)
	mainMinIdle := getEnvInt("REDIS_MIN_IDLE_CONNS", 10)
	mainMaxRetries := getEnvInt("REDIS_MAX_RETRIES", 5)

	// Инициализируем клиента Redis с улучшенными настройками стабильности
	Client = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Username: redisUser,
		Password: redisPass,
		DB:       0,

		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  6 * time.Second,

		PoolSize:     mainPoolSize,   // default 60; was 500
		MinIdleConns: mainMinIdle,    // default 10; was 50
		MaxRetries:   mainMaxRetries, // default 5; was 50

		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 3 * time.Second,
		ConnMaxIdleTime: 10 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,

		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			count := atomic.AddUint64(&connectionCounter, 1)
			if count%1000 == 0 {
				zap.S().Infof("✅ Redis (main) подключение #%d", count)
			}
			return nil
		},
	})

	// Устанавливаем улучшенный обработчик ошибок
	Client.AddHook(&improvedRedisHook{})

	// Инициализируем клиента для redis-worker-1 (бэкенд, порт 6380)
	redisWorkerHost := getEnv("REDIS_WORKER_HOST", "redis-worker-1")
	redisWorkerPort := getEnv("REDIS_WORKER_PORT", "6379")        // внутри контейнера 6379, снаружи 6380
	redisWorkerUser := getEnv("REDIS_WORKER_USERNAME", redisUser) // по умолчанию использует REDIS_USERNAME
	redisWorkerPass := getEnv("REDIS_WORKER_PASSWORD", redisPass) // по умолчанию использует REDIS_PASSWORD
	redisWorkerAddr := fmt.Sprintf("%s:%s", redisWorkerHost, redisWorkerPort)

	ClientWorker = redis.NewClient(&redis.Options{
		Addr:     redisWorkerAddr,
		Username: redisWorkerUser,
		Password: redisWorkerPass,
		DB:       0,

		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  6 * time.Second,

		PoolSize:     mainPoolSize, // shared with Client (same ENV)
		MinIdleConns: mainMinIdle,
		MaxRetries:   mainMaxRetries,

		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 3 * time.Second,
		ConnMaxIdleTime: 10 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,

		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			count := atomic.AddUint64(&connectionWorkerCounter, 1)
			if count%1000 == 0 {
				zap.S().Infof("✅ Redis (worker) подключение #%d", count)
			}
			return nil
		},
	})

	ClientWorker.AddHook(&improvedRedisHook{})

	// Инициализируем клиента для redis-ticks (стримы тиков)
	redisTicksHost := getEnv("REDIS_TICKS_HOST", "redis-ticks")
	redisTicksPort := getEnv("REDIS_TICKS_PORT", "6379")
	redisTicksUser := getEnv("REDIS_TICKS_USERNAME", redisUser) // по умолчанию использует REDIS_USERNAME
	redisTicksPass := getEnv("REDIS_TICKS_PASSWORD", redisPass) // по умолчанию использует REDIS_PASSWORD
	redisTicksAddr := fmt.Sprintf("%s:%s", redisTicksHost, redisTicksPort)

	switch redisTicksAddr {
	case redisAddr:
		ClientTicks = Client
	case redisWorkerAddr:
		ClientTicks = ClientWorker
	default:
		// Ticks gets a smaller pool: writes are batched by timeframe workers, not one-per-goroutine
		icksPoolSize := getEnvInt("REDIS_TICKS_POOL_SIZE", 40)
		icksMinIdle := getEnvInt("REDIS_TICKS_MIN_IDLE_CONNS", 5)
		ClientTicks = redis.NewClient(&redis.Options{
			Addr:     redisTicksAddr,
			Username: redisTicksUser,
			Password: redisTicksPass,
			DB:       0,

			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolTimeout:  6 * time.Second,

			PoolSize:     icksPoolSize, // default 40; was 500
			MinIdleConns: icksMinIdle,  // default 5; was 50
			MaxRetries:   mainMaxRetries,

			MinRetryBackoff: 100 * time.Millisecond,
			MaxRetryBackoff: 3 * time.Second,
			ConnMaxIdleTime: 10 * time.Minute,
			ConnMaxLifetime: 30 * time.Minute,

			OnConnect: func(ctx context.Context, cn *redis.Conn) error {
				count := atomic.AddUint64(&connectionTicksCounter, 1)
				if count%1000 == 0 {
					zap.S().Infof("✅ Redis (ticks) подключение #%d", count)
				}
				return nil
			},
		})
		ClientTicks.AddHook(&improvedRedisHook{})
	}
}

// improvedRedisHook - улучшенный хук для обработки ошибок Redis
type improvedRedisHook struct {
	circuitBreaker *CircuitBreaker
}

func (h *improvedRedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *improvedRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.circuitBreaker == nil {
			h.circuitBreaker = NewCircuitBreaker(100, 60*time.Second)
		}

		if !h.circuitBreaker.CanExecute() {
			cmd.SetErr(fmt.Errorf("circuit breaker открыт, операция заблокирована"))
			return cmd.Err()
		}

		err := next(ctx, cmd)
		if cmd.Err() != nil && cmd.Err() != redis.Nil {
			errStr := cmd.Err().Error()

			if strings.Contains(errStr, "BUSYGROUP") || strings.Contains(errStr, "Consumer Group name already exists") {
				h.circuitBreaker.RecordSuccess()
				return nil
			}

			if strings.Contains(errStr, "NOGROUP") {
				h.circuitBreaker.RecordSuccess()
				return cmd.Err()
			}

			if errStr == "context canceled" || errStr == "context deadline exceeded" {
				h.circuitBreaker.RecordSuccess()
				return cmd.Err()
			}

			if strings.Contains(errStr, "circuit breaker") {
				return cmd.Err()
			}

			if strings.Contains(errStr, "WRONGPASS") || strings.Contains(errStr, "NOAUTH") {
				zap.S().Errorf("🔑 Redis AUTH ошибка (проверьте ACL/пароль): %v", cmd.Err())
				return cmd.Err()
			}

			key := fmt.Sprintf("cmd|%s|%v", cmd.Name(), cmd.Err())
			if shouldLog, suppressed := shouldLogRedisError(key); shouldLog {
				if suppressed > 0 {
					zap.S().Errorf("❌ Redis команда %s завершилась с ошибкой: %v (подавлено %d повторов)", cmd.Name(), cmd.Err(), suppressed)
				} else {
					zap.S().Errorf("❌ Redis команда %s завершилась с ошибкой: %v", cmd.Name(), cmd.Err())
				}
			}

			h.circuitBreaker.RecordFailure()
			return err
		}

		h.circuitBreaker.RecordSuccess()
		return err
	}
}

func (h *improvedRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.circuitBreaker == nil {
			h.circuitBreaker = NewCircuitBreaker(100, 60*time.Second)
		}

		if !h.circuitBreaker.CanExecute() {
			return fmt.Errorf("circuit breaker открыт, pipeline операция заблокирована")
		}

		err := next(ctx, cmds)

		hasCircuitBreakerBlock := false
		for _, cmd := range cmds {
			if cmd.Err() != nil && cmd.Err() != redis.Nil {
				errStr := cmd.Err().Error()

				if strings.Contains(errStr, "circuit breaker") {
					hasCircuitBreakerBlock = true
					continue
				}

				if strings.Contains(errStr, "WRONGPASS") || strings.Contains(errStr, "NOAUTH") {
					zap.S().Errorf("🔑 Redis AUTH ошибка в pipeline (проверьте ACL/пароль): %v", cmd.Err())
					continue
				}

				if strings.Contains(errStr, "BUSYGROUP") || strings.Contains(errStr, "Consumer Group name already exists") {
					continue
				}

				if strings.Contains(errStr, "NOGROUP") {
					continue
				}

				if errStr == "context canceled" || errStr == "context deadline exceeded" {
					continue
				}

				key := fmt.Sprintf("pipeline|%s|%v", cmd.Name(), cmd.Err())
				if shouldLog, suppressed := shouldLogRedisError(key); shouldLog {
					if suppressed > 0 {
						zap.S().Errorf("❌ Redis pipeline команда %s завершилась с ошибкой: %v (подавлено %d повторов)", cmd.Name(), cmd.Err(), suppressed)
					} else {
						zap.S().Errorf("❌ Redis pipeline команда %s завершилась с ошибкой: %v", cmd.Name(), cmd.Err())
					}
				}
			}
		}

		if hasCircuitBreakerBlock || err != nil {
			h.circuitBreaker.RecordFailure()
			return err
		}

		h.circuitBreaker.RecordSuccess()
		return nil
	}
}

// XAddArgs - псевдоним для redis.XAddArgs для использования в пакете tasks
type XAddArgs = redis.XAddArgs

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// PingWithRetry выполняет ping с повторными попытками и exponential backoff
func PingWithRetry(maxRetries int, baseDelay time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := Client.Ping(ctx).Err()
		cancel()

		if err == nil {
			return nil
		}

		if i < maxRetries-1 {
			// Exponential backoff с jitter
			delay := baseDelay * time.Duration(1<<uint(i))
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			zap.S().Warnf("⚠️ Redis ping неудачен (попытка %d/%d), повтор через %v", i+1, maxRetries, delay)
			time.Sleep(delay)
		}
	}
	return fmt.Errorf("Redis недоступен после %d попыток", maxRetries)
}

// ExecuteWithRetry выполняет Redis команду с retry логикой
func ExecuteWithRetry(ctx context.Context, operation func() error, maxRetries int) error {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			if i < maxRetries-1 {
				// Exponential backoff
				delay := time.Duration(1<<uint(i)) * 100 * time.Millisecond
				if delay > 5*time.Second {
					delay = 5 * time.Second
				}
				zap.S().Errorf("⚠️ Redis операция неудачна (попытка %d/%d), повтор через %v: %v", i+1, maxRetries, delay, err)
				time.Sleep(delay)
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("Redis операция неудачна после %d попыток: %v", maxRetries, lastErr)
}

// XAddWithRetry выполняет XADD. В go-redis/v9 повторные попытки встроены.
func XAddWithRetry(ctx context.Context, client *redis.Client, args *redis.XAddArgs) (string, error) {
	return client.XAdd(ctx, args).Result()
}

// GetConnectionStats возвращает статистику соединений
func GetConnectionStats() *redis.PoolStats {
	return Client.PoolStats()
}

// IsHealthy проверяет здоровье Redis соединения
func IsHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return Client.Ping(ctx).Err() == nil
}
