// Пакет app содержит сервисные функции: старт/остановка, Prometheus и фоновый сбор данных.
package app

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"go-worker/binance"
	"go-worker/bybit"
	"go-worker/infra/redisclient"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
)

// PrintStartupMessage выводит стартовое сообщение
func PrintStartupMessage() {
	zap.S().Info("🚀 Go‑worker запущен")
}

// PrintShutdownMessage выводит сообщение о завершении работы
func PrintShutdownMessage() {
	zap.S().Info("⛔ Go‑worker завершается…")
}

// StartPrometheusMetrics запускает HTTP-сервер для Prometheus метрик
func StartPrometheusMetrics() {
	// Получаем порт из переменной окружения или используем 2112 по умолчанию
	port := os.Getenv("PROMETHEUS_PORT")
	if port == "" {
		port = "2112"
	}

	go func() {
		addr := fmt.Sprintf(":%s", port)
		zap.S().Infof("📈 Prometheus слушает %s/metrics", addr)
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(addr, nil)
	}()
}

// StartDataCollection запускает фоновый сбор рыночных данных
func StartDataCollection() {
	go func() {
		// Bybit сбор данных — опциональный (отдельный тумблер), чтобы не увеличивать
		// нагрузку на API без явного решения.
		// Включение:
		//   ENABLE_DATA_COLLECTION=true
		//   BYBIT_DATA_COLLECTION_ENABLED=true
		bybitEnabled := os.Getenv("BYBIT_DATA_COLLECTION_ENABLED") == "true"

		// Функция для периодического сбора данных (без retry — разовые ошибки
		// раз в час не критичны, следующая итерация исправит ситуацию).
		fetchData := func() {
			if err := binance.FetchAndPublishMarketData(redisclient.Ctx); err != nil {
				zap.S().Errorf("❌ Ошибка при сборе данных: %v", err)
			} else {
				zap.S().Info("✅ Сбор данных завершён успешно")
			}

			if bybitEnabled {
				if err := bybit.FetchAndPublishMarketData(redisclient.Ctx); err != nil {
					zap.S().Errorf("❌ Ошибка при сборе данных Bybit: %v", err)
				} else {
					zap.S().Info("✅ Сбор данных Bybit завершён успешно")
				}
			}
		}

		// retryWithBackoff выполняет fn с экспоненциальным ожиданием.
		// maxRetries=5, initial=1s, cap=30s.
		// Защищает от ситуации когда Redis ещё не готов при старте контейнера.
		retryWithBackoff := func(name string, fn func() error) {
			const maxRetries = 5
			delay := time.Second
			for attempt := 1; attempt <= maxRetries; attempt++ {
				if err := fn(); err == nil {
					zap.S().Infof("✅ [%s] начальный сбор завершён (попытка %d)", name, attempt)
					return
				} else if attempt < maxRetries {
					zap.S().Errorf("❌ [%s] ошибка (попытка %d/%d): %v — повтор через %s",
						name, attempt, maxRetries, err, delay)
					time.Sleep(delay)
					delay *= 2
					if delay > 30*time.Second {
						delay = 30 * time.Second
					}
				} else {
					zap.S().Errorf("⚠️ [%s] все %d попытки исчерпаны: %v — продолжаем без начального сбора",
						name, maxRetries, err)
				}
			}
		}

		// Начальный сбор с backoff — Redis может ещё стартовать.
		zap.S().Info("🔄 Запускаем начальный сбор данных (с backoff)")
		retryWithBackoff("binance", func() error {
			return binance.FetchAndPublishMarketData(redisclient.Ctx)
		})
		if bybitEnabled {
			retryWithBackoff("bybit", func() error {
				return bybit.FetchAndPublishMarketData(redisclient.Ctx)
			})
		}

		// Повторяем каждый час
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		zap.S().Info("⏰ Настроен повтор сбора данных каждый час")

		for range ticker.C {
			zap.S().Info("🔄 Запускаем периодический сбор данных")
			fetchData()
		}
	}()

	zap.S().Info("🔄 Запуск сбора данных")
}
