package bybit

import (
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		zap.S().Warnf("⚠️ invalid %s=%q, using default %v", key, value, defaultValue)
		return defaultValue
	}
	return duration
}

func getEnvInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return n
}

func getEnvString(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}
