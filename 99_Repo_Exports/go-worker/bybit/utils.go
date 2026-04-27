package bybit

import (
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// getEnvDuration читает duration из переменной окружения или возвращает значение по умолчанию.
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

// getEnvInt читает int из переменной окружения или возвращает значение по умолчанию.
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		zap.S().Warnf("⚠️ Неверный формат %s=%s, используем значение по умолчанию %d", key, value, defaultValue)
		return defaultValue
	}
	return parsed
}
