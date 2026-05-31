// Пакет scheduler управляет периодическими задачами (health‑чек подключений и др.).
package scheduler

import (
	"time"

	"go.uber.org/zap"
)

// HealthChecker управляет периодическими проверками здоровья системы
type connectionManager interface {
	GetActiveConnectionsCount() int
	GetLastFrameAt() time.Time
	UpdateConnections([]string)
}

// HealthChecker проверяет состояние соединений и данных
type HealthChecker struct {
	connectionManager connectionManager
}

// NewHealthChecker создает новый проверяльщик здоровья
func NewHealthChecker(cm connectionManager) *HealthChecker {
	return &HealthChecker{
		connectionManager: cm,
	}
}

// StartPeriodicConnectionCheck запускает периодическую проверку состояния WebSocket-подключений
func (h *HealthChecker) StartPeriodicConnectionCheck() {
	go func() {
		ticker := time.NewTicker(10 * time.Minute) // Интервал проверки 10 минут
		defer ticker.Stop()

		for range ticker.C {
			activeCount := h.connectionManager.GetActiveConnectionsCount()

			if activeCount > 0 {
				lastFrameAt := h.connectionManager.GetLastFrameAt()
				dataStaleFor := time.Since(lastFrameAt)

				// 🎯 WATCHDOG: Если соединения есть, но данных нет более 10 минут — воркер "замерз"
				// Binance отправляет kline-апдейты каждые 2с при наличии торгов.
				// 10 минут тишины по всем 300+ символам — это гарантированный сетевой лаг или зависание.
				if !lastFrameAt.IsZero() && dataStaleFor > 10*time.Minute {
					zap.S().Fatalf("🚨 КРИТИЧЕСКАЯ ОШИБКА: Frozen State! %d активных соединений, но данных нет %v. Принудительный рестарт...",
						activeCount, dataStaleFor.Round(time.Second))
				}

				zap.S().Infof("🔄 Плановая проверка: %d активных WebSocket-подключений работают (данные поступают: %v назад)",
					activeCount, dataStaleFor.Round(time.Second))
				// Не перезапускаем соединения без необходимости
				// Они сами переподключатся при ошибках
			} else {
				zap.S().Warn("⚠️ Нет активных WebSocket-подключений")

				// Если нет соединений, пробуем переподключить базовые пары
				baseSymbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
				zap.S().Info("🔄 Переподключение к базовым парам...")
				h.connectionManager.UpdateConnections(baseSymbols)
			}
		}
	}()
}
