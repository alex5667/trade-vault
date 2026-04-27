package binance

import (
	"os"
	"time"

	"go.uber.org/zap"
)

// getEnvDuration читает duration из переменной окружения или возвращает значение по умолчанию
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	// Парсим duration (например, "30s", "1m", "45s")
	duration, err := time.ParseDuration(value)
	if err != nil {
		zap.S().Warnf("⚠️ Неверный формат %s=%s, используем значение по умолчанию %v", key, value, defaultValue)
		return defaultValue
	}

	return duration
}
