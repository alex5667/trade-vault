// REST Candle Fetcher - для редких таймфреймов (3M, 1y)
// Senior Developer Team - Production-ready solution
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go-worker/binance"
	"go-worker/infra/redisclient"

	"go.uber.org/zap"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	zap.S().Infof("🚀 REST Candle Fetcher для редких таймфреймов - запуск")
	zap.S().Infof("=" + strings.Repeat("=", 70))

	// Получаем конфигурацию из environment
	timeframe := getEnv("BINANCE_WS_TIMEFRAME", "kline_3M")
	symbolsStr := getEnv("BINANCE_WS_SYMBOLS", "BTCUSDT,ETHUSDT")
	pollIntervalStr := getEnv("POLL_INTERVAL_HOURS", "1")

	// Парсим символы
	symbols := parseSymbols(symbolsStr)
	if len(symbols) == 0 {
		zap.S().Fatal("❌ Не указаны символы в BINANCE_WS_SYMBOLS")
	}

	// Парсим poll interval
	pollHours := parseInt(pollIntervalStr, 1)
	pollInterval := time.Duration(pollHours) * time.Hour

	// Нормализуем timeframe
	cleanTimeframe := strings.TrimPrefix(timeframe, "kline_")

	zap.S().Infof("⚙️  Конфигурация:")
	zap.S().Infof("   Таймфрейм: %s", cleanTimeframe)
	zap.S().Infof("   Символы: %v (%d шт.)", symbols, len(symbols))
	zap.S().Infof("   Poll interval: %v", pollInterval)
	zap.S().Infof("   Redis: %s:%s", getEnv("REDIS_HOST", "redis"), getEnv("REDIS_PORT", "6379"))

	// Проверяем поддерживается ли таймфрейм
	if !isSupportedTimeframe(cleanTimeframe) {
		zap.S().Fatalf("❌ Неподдерживаемый таймфрейм: %s (поддерживаются: 3M, 1y)", cleanTimeframe)
	}

	// Ждем инициализации Redis
	waitForRedis()

	// Создаем fetcher
	fetcher := binance.NewRestCandleFetcher(binance.RestCandleConfig{
		Symbols:      symbols,
		Timeframe:    cleanTimeframe,
		PollInterval: pollInterval,
	})

	// Запускаем HTTP server для healthcheck и мониторинга
	go startMonitoringServer(fetcher, cleanTimeframe)

	// Запускаем fetcher в отдельной горутине
	go fetcher.Start()

	zap.S().Infof("✅ REST Candle Fetcher для %s успешно запущен", cleanTimeframe)
	zap.S().Infof("=" + strings.Repeat("=", 70))

	// Graceful shutdown
	waitForShutdown(fetcher)
}

// waitForRedis ожидает подключения к Redis
func waitForRedis() {
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		if redisclient.Client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if err := redisclient.Client.Ping(ctx).Err(); err == nil {
				zap.S().Infof("✅ Redis подключен успешно")
				return
			}
		}

		if i == 0 {
			zap.S().Infof("⏳ Ожидание подключения к Redis...")
		}
		time.Sleep(1 * time.Second)
	}

	zap.S().Fatal("❌ Не удалось подключиться к Redis")
}

// startMonitoringServer запускает HTTP сервер для healthcheck
func startMonitoringServer(fetcher *binance.RestCandleFetcher, timeframe string) {
	mux := http.NewServeMux()

	// Healthcheck endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"status":    "healthy",
			"timeframe": timeframe,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}
		json.NewEncoder(w).Encode(response)
	})

	// Stats endpoint
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := fetcher.GetStats()
		stats["status"] = "running"
		stats["timestamp"] = time.Now().UTC().Format(time.RFC3339)
		json.NewEncoder(w).Encode(stats)
	})

	// Metrics endpoint (Prometheus-compatible)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		stats := fetcher.GetStats()

		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "# HELP rest_fetcher_fetches_total Total number of fetch cycles\n")
		fmt.Fprintf(w, "# TYPE rest_fetcher_fetches_total counter\n")
		fmt.Fprintf(w, "rest_fetcher_fetches_total{timeframe=\"%s\"} %v\n", timeframe, stats["total_fetches"])

		fmt.Fprintf(w, "# HELP rest_fetcher_published_total Total number of published candles\n")
		fmt.Fprintf(w, "# TYPE rest_fetcher_published_total counter\n")
		fmt.Fprintf(w, "rest_fetcher_published_total{timeframe=\"%s\"} %v\n", timeframe, stats["total_published"])

		fmt.Fprintf(w, "# HELP rest_fetcher_errors_total Total number of errors\n")
		fmt.Fprintf(w, "# TYPE rest_fetcher_errors_total counter\n")
		fmt.Fprintf(w, "rest_fetcher_errors_total{timeframe=\"%s\"} %v\n", timeframe, stats["total_errors"])

		circuitOpen := 0
		if stats["circuit_open"].(bool) {
			circuitOpen = 1
		}
		fmt.Fprintf(w, "# HELP rest_fetcher_circuit_breaker_open Circuit breaker status\n")
		fmt.Fprintf(w, "# TYPE rest_fetcher_circuit_breaker_open gauge\n")
		fmt.Fprintf(w, "rest_fetcher_circuit_breaker_open{timeframe=\"%s\"} %d\n", timeframe, circuitOpen)
	})

	port := getEnv("HTTP_PORT", "8091")
	addr := fmt.Sprintf(":%s", port)

	zap.S().Infof("🌐 HTTP мониторинг запущен на порту %s", port)
	zap.S().Infof("   Healthcheck: http://localhost:%s/health", port)
	zap.S().Infof("   Stats: http://localhost:%s/stats", port)
	zap.S().Infof("   Metrics: http://localhost:%s/metrics", port)

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		zap.S().Errorf("⚠️ HTTP сервер остановлен: %v", err)
	}
}

// waitForShutdown ожидает сигнала для graceful shutdown
func waitForShutdown(fetcher *binance.RestCandleFetcher) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	zap.S().Infof("🛑 Получен сигнал %v, начинаем graceful shutdown...", sig)

	// Останавливаем fetcher
	fetcher.Stop()

	zap.S().Infof("👋 REST Candle Fetcher остановлен успешно")
	os.Exit(0)
}

// Вспомогательные функции

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseSymbols(symbolsStr string) []string {
	symbols := strings.Split(symbolsStr, ",")
	result := make([]string, 0, len(symbols))

	for _, s := range symbols {
		s = strings.TrimSpace(s)
		if s != "" {
			result = append(result, strings.ToUpper(s))
		}
	}

	return result
}

func parseInt(str string, defaultValue int) int {
	var val int
	if _, err := fmt.Sscanf(str, "%d", &val); err != nil {
		return defaultValue
	}
	return val
}

func isSupportedTimeframe(tf string) bool {
	supported := []string{"3M", "1y", "kline_3M", "kline_1y"}
	for _, s := range supported {
		if tf == s {
			return true
		}
	}
	return false
}
