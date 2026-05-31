// Package monitoring предоставляет метрики и мониторинг для WebSocket соединений
package monitoring

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// WebSocket метрики
	websocketConnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_connections_total",
			Help: "Общее количество WebSocket подключений",
		},
		[]string{"symbol", "timeframe", "status"},
	)

	websocketConnectionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "websocket_connection_duration_seconds",
			Help:    "Длительность WebSocket соединений в секундах",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"symbol", "timeframe"},
	)

	websocketReconnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_reconnections_total",
			Help: "Общее количество переподключений WebSocket",
		},
		[]string{"symbol", "timeframe", "reason"},
	)

	websocketErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_errors_total",
			Help: "Общее количество ошибок WebSocket",
		},
		[]string{"symbol", "timeframe", "error_type"},
	)

	websocketMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_messages_received_total",
			Help: "Общее количество полученных сообщений",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketMessagesPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_messages_published_total",
			Help: "Общее количество опубликованных сообщений в Redis",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketConnectionStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_connection_status",
			Help: "Статус WebSocket соединения (1 = активное, 0 = неактивное)",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketRetryAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_retry_attempts_total",
			Help: "Общее количество попыток переподключения",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketLastMessageTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_last_message_timestamp",
			Help: "Timestamp последнего полученного сообщения",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketLastActivityTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_last_activity_timestamp",
			Help: "Timestamp последней активности (сообщение или Ping/Pong)",
		},
		[]string{"symbol", "timeframe"},
	)

	// Алерты метрики
	websocketAlertHighReconnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_alert_high_reconnections",
			Help: "Алерт на высокую частоту переподключений (1 = активен, 0 = неактивен)",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketAlertConnectionLoss = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_alert_connection_loss",
			Help: "Алерт на потерю соединения (1 = активен, 0 = неактивен)",
		},
		[]string{"symbol", "timeframe"},
	)

	websocketAlertHighErrorRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "websocket_alert_high_error_rate",
			Help: "Алерт на высокую частоту ошибок (1 = активен, 0 = неактивен)",
		},
		[]string{"symbol", "timeframe"},
	)
)

// WebSocketMonitor управляет метриками WebSocket соединений
type WebSocketMonitor struct {
	mu sync.RWMutex
	// Статистика по символам и таймфреймам
	stats map[string]*ConnectionStats
	// Настройки алертов
	alertConfig AlertConfig
}

// ConnectionStats статистика соединения
type ConnectionStats struct {
	Symbol            string
	Timeframe         string
	ConnectionStart   time.Time
	LastMessage       time.Time
	LastActivity      time.Time
	ReconnectionCount int64
	ErrorCount        int64
	MessageCount      int64
	PublishedCount    int64
	IsConnected       bool
	LastError         string
	LastErrorTime     time.Time
}

// AlertConfig настройки алертов
type AlertConfig struct {
	MaxReconnectionsPerMinute int64
	MaxErrorsPerMinute        int64
	MaxConnectionLossMinutes  int64
	HighErrorRateThreshold    float64
}

// NewWebSocketMonitor создает новый монитор WebSocket
func NewWebSocketMonitor() *WebSocketMonitor {
	return &WebSocketMonitor{
		stats: make(map[string]*ConnectionStats),
		alertConfig: AlertConfig{
			MaxReconnectionsPerMinute: 5,   // Максимум 5 переподключений в минуту
			MaxErrorsPerMinute:        10,  // Максимум 10 ошибок в минуту
			MaxConnectionLossMinutes:  2,   // Алерт если нет сообщений 2 минуты
			HighErrorRateThreshold:    0.1, // 10% ошибок
		},
	}
}

// getKey возвращает ключ для статистики
func (w *WebSocketMonitor) getKey(symbol, timeframe string) string {
	return symbol + "@" + timeframe
}

// RecordConnection записывает новое подключение
func (w *WebSocketMonitor) RecordConnection(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	defer w.mu.Unlock()

	if stats, exists := w.stats[key]; exists {
		stats.IsConnected = true
		stats.ConnectionStart = time.Now()
	} else {
		w.stats[key] = &ConnectionStats{
			Symbol:          symbol,
			Timeframe:       timeframe,
			ConnectionStart: time.Now(),
			LastActivity:    time.Now(),
			IsConnected:     true,
		}
	}

	websocketConnectionsTotal.WithLabelValues(symbol, timeframe, "connected").Inc()
	websocketConnectionStatus.WithLabelValues(symbol, timeframe).Set(1)
}

// RecordDisconnection записывает отключение
func (w *WebSocketMonitor) RecordDisconnection(symbol, timeframe string, reason string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	defer w.mu.Unlock()

	if stats, exists := w.stats[key]; exists {
		stats.IsConnected = false

		// Записываем длительность соединения
		duration := time.Since(stats.ConnectionStart).Seconds()
		websocketConnectionDuration.WithLabelValues(symbol, timeframe).Observe(duration)
	}

	websocketConnectionsTotal.WithLabelValues(symbol, timeframe, "disconnected").Inc()
	websocketConnectionStatus.WithLabelValues(symbol, timeframe).Set(0)
}

// RecordReconnection записывает переподключение
func (w *WebSocketMonitor) RecordReconnection(symbol, timeframe string, reason string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	defer w.mu.Unlock()

	if stats, exists := w.stats[key]; exists {
		stats.ReconnectionCount++
	}

	websocketReconnectionsTotal.WithLabelValues(symbol, timeframe, reason).Inc()
	websocketRetryAttempts.WithLabelValues(symbol, timeframe).Inc()

	// Проверяем алерты
	w.checkReconnectionAlert(symbol, timeframe)
}

// RecordError записывает ошибку
func (w *WebSocketMonitor) RecordError(symbol, timeframe string, errorType string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	defer w.mu.Unlock()

	if stats, exists := w.stats[key]; exists {
		stats.ErrorCount++
		stats.LastError = errorType
		stats.LastErrorTime = time.Now()
	}

	websocketErrorsTotal.WithLabelValues(symbol, timeframe, errorType).Inc()

	// Проверяем алерты
	w.checkErrorAlert(symbol, timeframe)
}

// RecordMessageReceived записывает полученное сообщение
func (w *WebSocketMonitor) RecordMessageReceived(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)

	if stats, exists := w.stats[key]; exists {
		stats.MessageCount++
		stats.LastMessage = time.Now()
	}

	websocketMessagesReceived.WithLabelValues(symbol, timeframe).Inc()
	websocketLastMessageTimestamp.WithLabelValues(symbol, timeframe).Set(float64(time.Now().Unix())) // UTC время в секундах

	websocketAlertConnectionLoss.WithLabelValues(symbol, timeframe).Set(0)

	// Также записываем активность
	w.RecordActivity(symbol, timeframe)
}

// RecordActivity записывает любую активность (сообщение, Ping или Pong)
func (w *WebSocketMonitor) RecordActivity(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	if stats, exists := w.stats[key]; exists {
		stats.LastActivity = time.Now()
	}
	w.mu.Unlock()

	websocketLastActivityTimestamp.WithLabelValues(symbol, timeframe).Set(float64(time.Now().Unix()))
}

// RecordPublishedMessage записывает опубликованное сообщение
func (w *WebSocketMonitor) RecordPublishedMessage(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)

	w.mu.Lock()
	defer w.mu.Unlock()

	if stats, exists := w.stats[key]; exists {
		stats.PublishedCount++
	}

	websocketMessagesPublished.WithLabelValues(symbol, timeframe).Inc()
}

// checkReconnectionAlert проверяет алерт на высокую частоту переподключений
func (w *WebSocketMonitor) checkReconnectionAlert(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)
	stats := w.stats[key]

	if stats == nil {
		return
	}

	// Простая проверка - если переподключений больше порога
	if stats.ReconnectionCount > w.alertConfig.MaxReconnectionsPerMinute {
		websocketAlertHighReconnections.WithLabelValues(symbol, timeframe).Set(1)
	} else {
		websocketAlertHighReconnections.WithLabelValues(symbol, timeframe).Set(0)
	}
}

// checkErrorAlert проверяет алерт на высокую частоту ошибок
func (w *WebSocketMonitor) checkErrorAlert(symbol, timeframe string) {
	key := w.getKey(symbol, timeframe)
	stats := w.stats[key]

	if stats == nil {
		return
	}

	// Проверяем соотношение ошибок к сообщениям
	if stats.MessageCount > 0 {
		errorRate := float64(stats.ErrorCount) / float64(stats.MessageCount)
		if errorRate > w.alertConfig.HighErrorRateThreshold {
			websocketAlertHighErrorRate.WithLabelValues(symbol, timeframe).Set(1)
		} else {
			websocketAlertHighErrorRate.WithLabelValues(symbol, timeframe).Set(0)
		}
	}
}

// CheckConnectionLossAlert проверяет алерт на потерю соединения
func (w *WebSocketMonitor) CheckConnectionLossAlert() {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()

	for _, stats := range w.stats {
		if stats.IsConnected && !stats.LastMessage.IsZero() {
			timeSinceLastMessage := now.Sub(stats.LastMessage).Minutes()
			if timeSinceLastMessage > float64(w.alertConfig.MaxConnectionLossMinutes) {
				websocketAlertConnectionLoss.WithLabelValues(stats.Symbol, stats.Timeframe).Set(1)
			}
		}
	}
}

// GetStats возвращает статистику по всем соединениям
func (w *WebSocketMonitor) GetStats() map[string]*ConnectionStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[string]*ConnectionStats)
	for key, stats := range w.stats {
		result[key] = &ConnectionStats{
			Symbol:            stats.Symbol,
			Timeframe:         stats.Timeframe,
			ConnectionStart:   stats.ConnectionStart,
			LastMessage:       stats.LastMessage,
			ReconnectionCount: stats.ReconnectionCount,
			ErrorCount:        stats.ErrorCount,
			MessageCount:      stats.MessageCount,
			PublishedCount:    stats.PublishedCount,
			IsConnected:       stats.IsConnected,
			LastActivity:      stats.LastActivity,
			LastError:         stats.LastError,
			LastErrorTime:     stats.LastErrorTime,
		}
	}

	return result
}

// SetAlertConfig устанавливает настройки алертов
func (w *WebSocketMonitor) SetAlertConfig(config AlertConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.alertConfig = config
}
