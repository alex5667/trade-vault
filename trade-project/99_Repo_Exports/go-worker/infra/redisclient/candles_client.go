// Пакет redisclient для данных свечей с отдельным подключением к Redis Worker 1 (порт 6380)
package redisclient

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// CandlesClient - отдельный клиент Redis для данных свечей (redis-worker-1, порт 6380)
var CandlesClient *redis.Client

// CandlesClient2 - второй клиент Redis для данных свечей (redis-worker-2, порт 6381)
var CandlesClient2 *redis.Client

// Счетчики подключений для уменьшения количества логов
var (
	candlesConnectionCounter  uint64
	candlesConnectionCounter2 uint64
)

// init инициализирует клиенты Redis для данных свечей
func init() {
	candlesHost := getEnv("REDIS_CANDLES_HOST", "redis-worker-1")
	candlesPort := getEnv("REDIS_CANDLES_PORT", "6379")
	candlesUser := getEnv("REDIS_CANDLES_USERNAME", getEnv("REDIS_USERNAME", ""))
	candlesPass := getEnv("REDIS_CANDLES_PASSWORD", getEnv("REDIS_PASSWORD", ""))
	candlesAddr := fmt.Sprintf("%s:%s", candlesHost, candlesPort)

	// Right-sized pool: candles are written on bar-close events, not per-tick.
	// 15 symbols × 8 timeframes = 120 peak goroutines; pool of 60 per client is sufficient.
	candlesPoolSize := getEnvInt("REDIS_CANDLES_POOL_SIZE", 60)
	candlesMinIdle := getEnvInt("REDIS_CANDLES_MIN_IDLE", 10)
	candlesMaxRetries := getEnvInt("REDIS_MAX_RETRIES", 5)

	CandlesClient = redis.NewClient(&redis.Options{
		Addr:     candlesAddr,
		Username: candlesUser,
		Password: candlesPass,
		DB:       0,

		// Custom Dialer: KeepAlive=30s → OS detects dead peers in ~90s (3 missed probes)
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			return d.DialContext(ctx, network, addr)
		},

		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  6 * time.Second,

		PoolSize:     candlesPoolSize,   // default 60; was 500
		MinIdleConns: candlesMinIdle,    // default 10; was 50
		MaxRetries:   candlesMaxRetries, // default 5; was 50

		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 3 * time.Second,
		ConnMaxIdleTime: 3 * time.Minute,
		ConnMaxLifetime: 5 * time.Minute,

		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			count := atomic.AddUint64(&candlesConnectionCounter, 1)
			if count%1000 == 0 {
				zap.S().Infof("✅ Redis Candles-1 подключение #%d к %s", count, candlesAddr)
			}
			return nil
		},
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	if err := CandlesClient.Ping(pingCtx).Err(); err != nil {
		zap.S().Errorf("⚠️ Ошибка подключения к Redis Candles Client 1 (%s): %v", candlesAddr, err)
	} else {
		zap.S().Infof("✅ Redis Candles Client 1 готов (%s) pool=%d", candlesAddr, candlesPoolSize)
	}

	candlesHost2 := getEnv("REDIS_CANDLES_HOST_2", "redis-worker-2")
	candlesPort2 := getEnv("REDIS_CANDLES_PORT_2", "6379")
	candlesUser2 := getEnv("REDIS_CANDLES_USERNAME_2", candlesUser)
	candlesPass2 := getEnv("REDIS_CANDLES_PASSWORD_2", candlesPass)
	candlesAddr2 := fmt.Sprintf("%s:%s", candlesHost2, candlesPort2)

	CandlesClient2 = redis.NewClient(&redis.Options{
		Addr:     candlesAddr2,
		Username: candlesUser2,
		Password: candlesPass2,
		DB:       0,

		// Custom Dialer: KeepAlive=30s → OS detects dead peers in ~90s (3 missed probes)
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			return d.DialContext(ctx, network, addr)
		},

		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  6 * time.Second,

		PoolSize:     candlesPoolSize, // same ENV as Client 1
		MinIdleConns: candlesMinIdle,
		MaxRetries:   candlesMaxRetries,

		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 3 * time.Second,
		ConnMaxIdleTime: 3 * time.Minute,
		ConnMaxLifetime: 5 * time.Minute,

		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			count := atomic.AddUint64(&candlesConnectionCounter2, 1)
			if count%1000 == 0 {
				zap.S().Infof("✅ Redis Candles-2 подключение #%d к %s", count, candlesAddr2)
			}
			return nil
		},
	})

	pingCtx2, pingCancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel2()
	if err := CandlesClient2.Ping(pingCtx2).Err(); err != nil {
		zap.S().Errorf("⚠️ Ошибка подключения к Redis Candles Client 2 (%s): %v", candlesAddr2, err)
	} else {
		zap.S().Infof("✅ Redis Candles Client 2 готов (%s) pool=%d", candlesAddr2, candlesPoolSize)
	}
}

// PingCandlesClient проверяет подключение к обоим Redis для свечей
func PingCandlesClient() error {
	ctx := context.Background()

	err1 := CandlesClient.Ping(ctx).Err()
	if err1 != nil {
		zap.S().Errorf("⚠️ Redis Candles Client 1 недоступен: %v", err1)
	}

	err2 := CandlesClient2.Ping(ctx).Err()
	if err2 != nil {
		zap.S().Errorf("⚠️ Redis Candles Client 2 недоступен: %v", err2)
	}

	// Возвращаем ошибку только если оба недоступны
	if err1 != nil && err2 != nil {
		return fmt.Errorf("оба Redis Candles Client недоступны")
	}

	return nil
}

// CloseCandlesClient закрывает соединение с обоими Redis для свечей
func CloseCandlesClient() error {
	err1 := CandlesClient.Close()
	err2 := CandlesClient2.Close()

	if err1 != nil {
		return err1
	}
	return err2
}

// XAddDual записывает данные в оба Redis одновременно (dual-write) с retry логикой
func XAddDual(ctx context.Context, args *XAddArgs) error {
	// Guard: if the caller's context is already cancelled (e.g. during startup
	// connection-refresh cycles in go-worker-1y / go-worker-1month), fall back
	// to a fresh background context with a bounded write timeout.
	// This prevents the first bar-close XADD from being silently dropped.
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	var success1, success2 bool

	// Пишем в первый Redis (redis-worker-1) с retry
	if _, err := XAddWithRetry(ctx, CandlesClient, args); err != nil {
		zap.S().Errorf("⚠️ Ошибка записи в Redis Candles Client 1: %v", err)
	} else {
		success1 = true
	}

	if _, err := XAddWithRetry(ctx, CandlesClient2, args); err != nil {
		// Только логируем ошибку, если она не связана с подключением
		// Это снижает spam в логах при временных проблемах
		if time.Now().Unix()%10 == 0 || err != context.DeadlineExceeded {
			zap.S().Errorf("⚠️ Ошибка записи в Redis Candles Client 2: %v", err)
		}
	} else {
		success2 = true
	}

	// Возвращаем ошибку только если оба Redis недоступны
	if !success1 && !success2 {
		return fmt.Errorf("не удалось записать в оба Redis")
	}

	// Успех если хотя бы один Redis доступен
	return nil
}

// XGroupCreateDual создает consumer group в обоих Redis
func XGroupCreateDual(ctx context.Context, stream, group, start string) error {
	var err1, err2 error

	// Создаем в первом Redis
	err1 = CandlesClient.XGroupCreate(ctx, stream, group, start).Err()
	if err1 != nil && !isGroupAlreadyExists(err1) {
		zap.S().Errorf("⚠️ Ошибка создания группы в Redis Candles Client 1: %v", err1)
	}

	// Создаем во втором Redis
	err2 = CandlesClient2.XGroupCreate(ctx, stream, group, start).Err()
	if err2 != nil && !isGroupAlreadyExists(err2) {
		zap.S().Errorf("⚠️ Ошибка создания группы в Redis Candles Client 2: %v", err2)
	}

	// Возвращаем ошибку только если оба Redis вернули не-BUSYGROUP ошибку
	if err1 != nil && !isGroupAlreadyExists(err1) && err2 != nil && !isGroupAlreadyExists(err2) {
		return fmt.Errorf("не удалось создать группу в обоих Redis: %v, %v", err1, err2)
	}

	return nil
}

// isGroupAlreadyExists проверяет, является ли ошибка "группа уже существует"
func isGroupAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "BUSYGROUP Consumer Group name already exists"
}
