package binance

import (
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
)

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		zap.S().Warnf("⚠️ Неверный формат %s=%s, используем значение по умолчанию %v", key, value, defaultValue)
		return defaultValue
	}

	return duration
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		zap.S().Warnf("⚠️ Неверный формат %s=%s, используем значение по умолчанию %d", key, value, defaultValue)
		return defaultValue
	}
	return n
}
