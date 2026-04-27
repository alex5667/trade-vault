// Package monitoring предоставляет HTTP endpoints для мониторинга WebSocket соединений
package monitoring

import (
	"encoding/json"
	"net/http"
	"time"
)

// WebSocketMonitorHTTP предоставляет HTTP endpoints для мониторинга
type WebSocketMonitorHTTP struct {
	monitor *WebSocketMonitor
}

// processStartTime хранит время старта приложения для расчета uptime (Priority 9)
var processStartTime = time.Now()

// NewWebSocketMonitorHTTP создает новый HTTP монитор
func NewWebSocketMonitorHTTP(monitor *WebSocketMonitor) *WebSocketMonitorHTTP {
	return &WebSocketMonitorHTTP{
		monitor: monitor,
	}
}

// RegisterHandlers регистрирует HTTP handlers
func (w *WebSocketMonitorHTTP) RegisterHandlers() {
	http.HandleFunc("/monitoring/websocket/stats", w.handleStats)
	http.HandleFunc("/monitoring/websocket/alerts", w.handleAlerts)
	http.HandleFunc("/monitoring/websocket/health", w.handleHealth)
}

// handleStats возвращает статистику WebSocket соединений
func (w *WebSocketMonitorHTTP) handleStats(resp http.ResponseWriter, r *http.Request) {
	resp.Header().Set("Content-Type", "application/json")

	stats := w.monitor.GetStats()

	// Форматируем статистику для JSON
	response := make(map[string]interface{})
	for key, stat := range stats {
		response[key] = map[string]interface{}{
			"symbol":             stat.Symbol,
			"timeframe":          stat.Timeframe,
			"is_connected":       stat.IsConnected,
			"connection_start":   stat.ConnectionStart.Format(time.RFC3339),
			"last_message":       stat.LastMessage.Format(time.RFC3339),
			"reconnection_count": stat.ReconnectionCount,
			"error_count":        stat.ErrorCount,
			"message_count":      stat.MessageCount,
			"published_count":    stat.PublishedCount,
			"last_activity":      stat.LastActivity.Format(time.RFC3339),
			"last_error":         stat.LastError,
			"last_error_time":    stat.LastErrorTime.Format(time.RFC3339),
			"uptime_seconds":     time.Since(stat.ConnectionStart).Seconds(),
		}
	}

	json.NewEncoder(resp).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"stats":     response,
	})
}

// handleAlerts возвращает активные алерты
func (w *WebSocketMonitorHTTP) handleAlerts(resp http.ResponseWriter, r *http.Request) {
	resp.Header().Set("Content-Type", "application/json")

	// Проверяем алерты
	w.monitor.CheckConnectionLossAlert()

	stats := w.monitor.GetStats()
	alerts := make([]map[string]interface{}, 0)

	for _, stat := range stats {
		// Проверяем алерт на высокую частоту переподключений
		if stat.ReconnectionCount > w.monitor.alertConfig.MaxReconnectionsPerMinute {
			alerts = append(alerts, map[string]interface{}{
				"type":      "high_reconnections",
				"symbol":    stat.Symbol,
				"timeframe": stat.Timeframe,
				"value":     stat.ReconnectionCount,
				"threshold": w.monitor.alertConfig.MaxReconnectionsPerMinute,
				"timestamp": time.Now().Format(time.RFC3339),
				"severity":  "warning",
			})
		}

		// Проверяем алерт на высокую частоту ошибок
		if stat.MessageCount > 0 {
			errorRate := float64(stat.ErrorCount) / float64(stat.MessageCount)
			if errorRate > w.monitor.alertConfig.HighErrorRateThreshold {
				alerts = append(alerts, map[string]interface{}{
					"type":      "high_error_rate",
					"symbol":    stat.Symbol,
					"timeframe": stat.Timeframe,
					"value":     errorRate,
					"threshold": w.monitor.alertConfig.HighErrorRateThreshold,
					"timestamp": time.Now().Format(time.RFC3339),
					"severity":  "critical",
				})
			}
		}

		// Проверяем алерт на потерю соединения
		if stat.IsConnected && !stat.LastMessage.IsZero() {
			timeSinceLastMessage := time.Since(stat.LastMessage).Minutes()
			if timeSinceLastMessage > float64(w.monitor.alertConfig.MaxConnectionLossMinutes) {
				alerts = append(alerts, map[string]interface{}{
					"type":      "connection_loss",
					"symbol":    stat.Symbol,
					"timeframe": stat.Timeframe,
					"value":     timeSinceLastMessage,
					"threshold": w.monitor.alertConfig.MaxConnectionLossMinutes,
					"timestamp": time.Now().Format(time.RFC3339),
					"severity":  "critical",
				})
			}
		}
	}

	json.NewEncoder(resp).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"alerts":    alerts,
		"count":     len(alerts),
	})
}

// handleHealth возвращает статус здоровья системы
func (w *WebSocketMonitorHTTP) handleHealth(resp http.ResponseWriter, r *http.Request) {
	resp.Header().Set("Content-Type", "application/json")

	stats := w.monitor.GetStats()
	connectedCount := 0
	totalCount := len(stats)
	stalledCount := 0

	now := time.Now()
	for _, stat := range stats {
		if stat.IsConnected {
			connectedCount++

			// Для 1M/3M/1y порог 20 минут, для остальных 10 минут
			threshold := 10 * time.Minute
			if stat.Timeframe == "kline_1M" || stat.Timeframe == "1M" ||
				stat.Timeframe == "kline_3M" || stat.Timeframe == "3M" ||
				stat.Timeframe == "kline_1y" || stat.Timeframe == "1y" {
				threshold = 20 * time.Minute
			}

			if !stat.LastActivity.IsZero() && now.Sub(stat.LastActivity) > threshold {
				stalledCount++
			}
		}
	}

	health := "healthy"
	statusCode := http.StatusOK

	if (connectedCount == 0 && totalCount > 0) || stalledCount > 0 {
		health = "critical"
		statusCode = http.StatusServiceUnavailable // 503 заставит Docker healthcheck провалиться
	} else if connectedCount < totalCount {
		health = "warning"
	}

	resp.WriteHeader(statusCode)
	json.NewEncoder(resp).Encode(map[string]interface{}{
		"status":          health,
		"timestamp":       now.Format(time.RFC3339),
		"connected_count": connectedCount,
		"stalled_count":   stalledCount,
		"total_count":     totalCount,
		"uptime_seconds":  time.Since(processStartTime).Seconds(),
	})
}
