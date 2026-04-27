package events

import (
	"encoding/json"
	"testing"
)

func TestTradeEventVersion(t *testing.T) {
	event := TradeEvent{
		EventType: EventPositionOpened,
		SID:       "test-sid",
		Symbol:    "XAUUSD",
		V:         1,
	}

	// Verify JSON serialization includes v
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal TradeEvent: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Failed to unmarshal TradeEvent: %v", err)
	}

	if v, ok := m["v"]; !ok || v.(float64) != 1 {
		t.Errorf("Expected field 'v' with value 1 in JSON, got %v", m["v"])
	}
}
