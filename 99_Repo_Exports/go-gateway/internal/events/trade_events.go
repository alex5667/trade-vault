package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	redisv9 "github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// TradeEventType represents the type of trading event
type TradeEventType string

const (
	EventPositionOpened  TradeEventType = "POSITION_OPENED"
	EventTP1Hit          TradeEventType = "TP1_HIT"
	EventTP2Hit          TradeEventType = "TP2_HIT"
	EventTP3Hit          TradeEventType = "TP3_HIT"
	EventSLHit           TradeEventType = "SL_HIT"
	EventTrailingStarted TradeEventType = "TRAILING_STARTED"
	EventTrailingMove    TradeEventType = "TRAILING_MOVE"
	EventPositionClosed  TradeEventType = "POSITION_CLOSED"
)

// TradeEvent represents a trading event to be published
type TradeEvent struct {
	EventType  TradeEventType `json:"event_type"`
	SID        string         `json:"sid"`
	Symbol     string         `json:"symbol"`
	PositionID string         `json:"position_id,omitempty"`
	Ticket     string         `json:"ticket,omitempty"`
	Price      float64        `json:"price,omitempty"`
	Lot        float64        `json:"lot,omitempty"`
	Qty        float64        `json:"qty,omitempty"`
	Quantity   float64        `json:"quantity,omitempty"`
	Timestamp  int64          `json:"ts"`
	Source     string         `json:"source"`
	V          int            `json:"v"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// TradeEventPublisher publishes trading events to Redis stream
type TradeEventPublisher struct {
	rdb        *redisv9.Client
	streamName string
	ctx        context.Context
}

// NewTradeEventPublisher creates a new event publisher
func NewTradeEventPublisher(rdb *redisv9.Client, streamName string) *TradeEventPublisher {
	if streamName == "" {
		streamName = "events:trades"
	}
	return &TradeEventPublisher{
		rdb:        rdb,
		streamName: streamName,
		ctx:        context.Background(),
	}
}

// PublishEvent publishes a trading event to Redis stream
func (p *TradeEventPublisher) PublishEvent(event TradeEvent) error {
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}

	// Serialize event to map for Redis XADD
	eventMap := map[string]interface{}{
		"event_type": string(event.EventType),
		"sid":        event.SID,
		"symbol":     event.Symbol,
		"ts":         fmt.Sprintf("%d", event.Timestamp),
		"source":     event.Source,
		"v":          "1",
	}

	// Add optional fields
	if event.PositionID != "" {
		eventMap["position_id"] = event.PositionID
	}
	if event.Ticket != "" {
		eventMap["ticket"] = event.Ticket
	}
	if event.Price > 0 {
		// Use %g to preserve precision for small numbers (e.g. 0.00000123) without trailing zeros
		eventMap["price"] = fmt.Sprintf("%.12g", event.Price)
	}
	if event.Lot > 0 {
		eventMap["lot"] = fmt.Sprintf("%.12g", event.Lot)
	}
	if event.Qty > 0 {
		eventMap["qty"] = fmt.Sprintf("%.12g", event.Qty)
	}
	if event.Quantity > 0 {
		eventMap["quantity"] = fmt.Sprintf("%.12g", event.Quantity)
	}

	// Add metadata as JSON string
	if event.Metadata != nil && len(event.Metadata) > 0 {
		metadataJSON, err := json.Marshal(event.Metadata)
		if err == nil {
			eventMap["metadata"] = string(metadataJSON)
		}
	}

	// Publish to Redis stream
	_, err := p.rdb.XAdd(p.ctx, &redisv9.XAddArgs{
		Stream: p.streamName,
		Values: eventMap,
	}).Result()

	if err != nil {
		zap.S().Errorf("❌ Failed to publish event %s for %s: %v", event.EventType, event.SID, err)
		return err
	}

	zap.S().Infof("📡 Event published: %s for %s (symbol=%s, price=%.2f)",
		event.EventType, event.SID, event.Symbol, event.Price)
	return nil
}

// PublishTP1Hit publishes a TP1 hit event
func (p *TradeEventPublisher) PublishTP1Hit(sid, symbol, positionID string, price, lot float64, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventTP1Hit,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID, // MT5 uses ticket
		Price:      price,
		Lot:        lot,
		Source:     source,
	})
}

// PublishTP2Hit publishes a TP2 hit event
func (p *TradeEventPublisher) PublishTP2Hit(sid, symbol, positionID string, price, lot float64, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventTP2Hit,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Price:      price,
		Lot:        lot,
		Source:     source,
	})
}

// PublishTP3Hit publishes a TP3 hit event
func (p *TradeEventPublisher) PublishTP3Hit(sid, symbol, positionID string, price, lot float64, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventTP3Hit,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Price:      price,
		Lot:        lot,
		Source:     source,
	})
}

// PublishSLHit publishes a SL hit event
func (p *TradeEventPublisher) PublishSLHit(sid, symbol, positionID string, price, lot float64, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventSLHit,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Price:      price,
		Lot:        lot,
		Source:     source,
	})
}

// PublishPositionOpened publishes a position opened event
func (p *TradeEventPublisher) PublishPositionOpened(sid, symbol, positionID string, price, lot float64, source string, metadata map[string]any) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventPositionOpened,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Price:      price,
		Lot:        lot,
		Source:     source,
		Metadata:   metadata,
	})
}

// PublishTrailingStarted publishes a trailing started event
func (p *TradeEventPublisher) PublishTrailingStarted(sid, symbol, positionID string, profileName string, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventTrailingStarted,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Source:     source,
		Metadata: map[string]any{
			"profile": profileName,
		},
	})
}

// PublishTrailingMove publishes a trailing stop move event
func (p *TradeEventPublisher) PublishTrailingMove(sid, symbol, positionID string, newSL float64, source string, metadata map[string]any) error {
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["new_sl"] = newSL

	return p.PublishEvent(TradeEvent{
		EventType:  EventTrailingMove,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Source:     source,
		Metadata:   metadata,
	})
}

// PublishPositionClosed publishes a position closed event
func (p *TradeEventPublisher) PublishPositionClosed(sid, symbol, positionID string, closePrice, pnl float64, source string) error {
	return p.PublishEvent(TradeEvent{
		EventType:  EventPositionClosed,
		SID:        sid,
		Symbol:     symbol,
		PositionID: positionID,
		Ticket:     positionID,
		Price:      closePrice,
		Source:     source,
		Metadata: map[string]any{
			"pnl": pnl,
		},
	})
}
