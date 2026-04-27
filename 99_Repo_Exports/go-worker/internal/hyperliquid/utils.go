package hyperliquid

import (
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// getEnv возвращает значение переменной окружения или defaultValue.
func getEnv(key, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	return v
}

// getEnvDuration читает time.Duration из env (например "250ms", "2s").
// При ошибке — возвращает defaultValue.
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		zap.S().Warnf("⚠️ invalid %s=%q, using default %v", key, v, defaultValue)
		return defaultValue
	}
	return d
}

func getEnvInt(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return defaultValue
	}
	return n
}

func getEnvBool(key string, defaultValue bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return defaultValue
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

// parseSymbolMap парсит маппинг монет Hyperliquid в ваш внутренний Symbol.
// Формат: "BTC:BTCUSDT,ETH:ETHUSDT,1000PEPE:1000PEPEUSDT".
//
// ВАЖНО:
//   - Hyperliquid в WS использует поле coin (например "BTC", "ETH").
//   - Внутри проекта trade символы обычно в формате Binance (например "BTCUSDT").
//     Чтобы Python worker мог использовать единый пайплайн, мы нормализуем coin -> symbol.
func parseSymbolMap(raw string) map[string]string {
	m := map[string]string{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return m
	}
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.ToUpper(strings.TrimSpace(kv[0]))
		v := strings.ToUpper(strings.TrimSpace(kv[1]))
		if k == "" || v == "" {
			continue
		}
		m[k] = v
	}
	return m
}
