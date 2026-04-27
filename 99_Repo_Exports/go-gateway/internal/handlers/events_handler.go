package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"scanner-gw/internal/events"

	redisv9 "github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// EventsHandler обрабатывает публикацию торговых событий
type EventsHandler struct {
	publisher *events.TradeEventPublisher
}

// NewEventsHandler создаёт новый обработчик событий
func NewEventsHandler(rdb *redisv9.Client, streamName string) *EventsHandler {
	return &EventsHandler{
		publisher: events.NewTradeEventPublisher(rdb, streamName),
	}
}

// PublishEventRequest представляет запрос на публикацию события
type PublishEventRequest struct {
	EventType  string         `json:"event_type"`
	SID        string         `json:"sid"`
	Symbol     string         `json:"symbol"`
	PositionID string         `json:"position_id,omitempty"`
	Ticket     string         `json:"ticket,omitempty"`
	Price      string         `json:"price,omitempty"`
	Lot        string         `json:"lot,omitempty"`
	Timestamp  string         `json:"ts,omitempty"`
	Source     string         `json:"source,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// HandlePublishEvent обрабатывает POST /events/publish
// Принимает событие и публикует его в Redis stream events:trades
func (h *EventsHandler) HandlePublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		zap.S().Errorf("❌ Failed to decode event request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Валидация обязательных полей
	if req.EventType == "" || req.SID == "" || req.Symbol == "" {
		zap.S().Errorf("❌ Missing required fields: event_type=%s sid=%s symbol=%s",
			req.EventType, req.SID, req.Symbol)
		http.Error(w, "Missing required fields: event_type, sid, symbol", http.StatusBadRequest)
		return
	}

	// Преобразуем строковые значения в float
	var price float64
	var lot float64
	var timestamp int64

	if req.Price != "" {
		if _, err := fmt.Sscanf(req.Price, "%f", &price); err != nil {
			price = 0
		}
	}

	if req.Lot != "" {
		if _, err := fmt.Sscanf(req.Lot, "%f", &lot); err != nil {
			lot = 0
		}
	}

	if req.Timestamp != "" {
		if _, err := fmt.Sscanf(req.Timestamp, "%d", &timestamp); err != nil {
			timestamp = 0
		}
	}

	// Используем position_id или ticket (для совместимости)
	positionID := req.PositionID
	if positionID == "" {
		positionID = req.Ticket
	}

	// Создаём событие
	event := events.TradeEvent{
		EventType:  events.TradeEventType(req.EventType),
		SID:        req.SID,
		Symbol:     req.Symbol,
		PositionID: positionID,
		Ticket:     positionID, // Дублируем для MT5
		Price:      price,
		Lot:        lot,
		Timestamp:  timestamp,
		Source:     req.Source,
		Metadata:   req.Metadata,
	}

	// Публикуем событие
	if err := h.publisher.PublishEvent(event); err != nil {
		zap.S().Errorf("❌ Failed to publish event: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	// Успешный ответ
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "ok",
		"event_type": req.EventType,
		"sid":        req.SID,
	})

	zap.S().Infof("✅ Event published: %s for %s (symbol=%s)",
		req.EventType, req.SID, req.Symbol)
}

// HandleHealthCheck обрабатывает GET /events/health
func (h *EventsHandler) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "healthy",
		"service": "events-handler",
	})
}
