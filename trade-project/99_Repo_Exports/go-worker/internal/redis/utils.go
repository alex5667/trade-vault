// Package redis (internal) provides utilities for waiting on Redis availability.
package redis

import (
	"time"

	"go-worker/infra/redisclient"

	"go.uber.org/zap"
)

// WaitForRedis waits for Redis to become available with retries.
func WaitForRedis() {
	zap.S().Info("🔄 Ожидание готовности Redis...")

	if err := redisclient.PingWithRetry(30, 2*time.Second); err != nil {
		zap.S().Errorf("❌ Не удалось подключиться к Redis: %v", err)
		return
	}

	zap.S().Info("✅ Redis готов к работе")
}
