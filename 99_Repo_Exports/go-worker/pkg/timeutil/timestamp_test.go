package timeutil

import (
	"fmt"
	"testing"
	"time"
)

// TestGetCurrentTimestampMs тест получения текущего времени в миллисекундах
func TestGetCurrentTimestampMs(t *testing.T) {
	ts := GetCurrentTimestampMs()

	// Проверяем, что это действительно миллисекунды
	if ts < 1600000000000 { // После 2020
		t.Errorf("Timestamp слишком маленький: %d", ts)
	}
	if ts > 2000000000000 { // До 2033
		t.Errorf("Timestamp слишком большой: %d", ts)
	}

	// Проверяем, что это близко к time.Now()
	current := time.Now().UnixMilli()
	diff := ts - current
	if diff < 0 {
		diff = -diff
	}
	if diff > 100 { // Разница меньше 100ms
		t.Errorf("Разница с time.Now() слишком большая: %d ms", diff)
	}
}

// TestFormatTimestampForRedis тест форматирования timestamp для Redis
func TestFormatTimestampForRedis(t *testing.T) {
	ts := int64(1697366459999)
	formatted := FormatTimestampForRedis(ts)

	expected := "1697366459999"
	if formatted != expected {
		t.Errorf("Ожидалось %s, получено %s", expected, formatted)
	}

	// Проверяем длину
	if len(formatted) != 13 {
		t.Errorf("Неверная длина: %d (ожидалось 13)", len(formatted))
	}
}

// TestValidateTimestamp тест валидации timestamp
func TestValidateTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		ts       int64
		expected bool
	}{
		{"Valid timestamp", 1697366459999, true},
		{"Too small", 123, false},
		{"Too large", 3000000000000, false},
		{"Before 2020", 1500000000000, false},
		{"After 2033", 2100000000000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateTimestamp(tt.ts)
			if result != tt.expected {
				t.Errorf("Для %d ожидалось %v, получено %v", tt.ts, tt.expected, result)
			}
		})
	}
}

// TestExtractEventTimestamp тест извлечения timestamp из данных
func TestExtractEventTimestamp(t *testing.T) {
	t.Run("Extract int64", func(t *testing.T) {
		data := map[string]interface{}{
			"closeTime": int64(1697366459999),
			"symbol":    "BTCUSDT",
		}
		ts := ExtractEventTimestamp(data, "closeTime", false)
		if ts != 1697366459999 {
			t.Errorf("Ожидалось 1697366459999, получено %d", ts)
		}
	})

	t.Run("Extract float64", func(t *testing.T) {
		data := map[string]interface{}{
			"closeTime": float64(1697366459999),
			"symbol":    "BTCUSDT",
		}
		ts := ExtractEventTimestamp(data, "closeTime", false)
		if ts != 1697366459999 {
			t.Errorf("Ожидалось 1697366459999, получено %d", ts)
		}
	})

	t.Run("Extract string", func(t *testing.T) {
		data := map[string]interface{}{
			"closeTime": "1697366459999",
			"symbol":    "BTCUSDT",
		}
		ts := ExtractEventTimestamp(data, "closeTime", false)
		if ts != 1697366459999 {
			t.Errorf("Ожидалось 1697366459999, получено %d", ts)
		}
	})

	t.Run("Missing field no fallback", func(t *testing.T) {
		data := map[string]interface{}{
			"symbol": "BTCUSDT",
		}
		ts := ExtractEventTimestamp(data, "closeTime", false)
		if ts != 0 {
			t.Errorf("Ожидалось 0, получено %d", ts)
		}
	})

	t.Run("Missing field with fallback", func(t *testing.T) {
		data := map[string]interface{}{
			"symbol": "BTCUSDT",
		}
		ts := ExtractEventTimestamp(data, "closeTime", true)
		if !ValidateTimestamp(ts) {
			t.Errorf("Fallback вернул невалидный timestamp: %d", ts)
		}
	})
}

// TestExtractBinanceCloseTime тест извлечения closeTime из данных Binance
func TestExtractBinanceCloseTime(t *testing.T) {
	t.Run("WebSocket format (T field)", func(t *testing.T) {
		candle := map[string]interface{}{
			"T":      int64(1697366459999),
			"symbol": "BTCUSDT",
		}
		ts := ExtractBinanceCloseTime(candle)
		if ts != 1697366459999 {
			t.Errorf("Ожидалось 1697366459999, получено %d", ts)
		}
	})

	t.Run("REST format (closeTime field)", func(t *testing.T) {
		candle := map[string]interface{}{
			"closeTime": int64(1697366459999),
			"symbol":    "BTCUSDT",
		}
		ts := ExtractBinanceCloseTime(candle)
		if ts != 1697366459999 {
			t.Errorf("Ожидалось 1697366459999, получено %d", ts)
		}
	})

	t.Run("Calculate from openTime", func(t *testing.T) {
		candle := map[string]interface{}{
			"t":      int64(1697366400000), // openTime
			"i":      "1m",
			"symbol": "BTCUSDT",
		}
		ts := ExtractBinanceCloseTime(candle)
		expected := int64(1697366400000 + 60000 - 1) // openTime + 1 minute - 1ms
		if ts != expected {
			t.Errorf("Ожидалось %d, получено %d", expected, ts)
		}
	})
}

// TestParseIntervalToMs тест парсинга интервалов
func TestParseIntervalToMs(t *testing.T) {
	tests := []struct {
		interval string
		expected int64
	}{
		{"1m", 60 * 1000},
		{"5m", 5 * 60 * 1000},
		{"15m", 15 * 60 * 1000},
		{"1h", 60 * 60 * 1000},
		{"4h", 4 * 60 * 60 * 1000},
		{"1d", 24 * 60 * 60 * 1000},
		{"1w", 7 * 24 * 60 * 60 * 1000},
		{"unknown", 60 * 1000}, // Default to 1m
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			result := ParseIntervalToMs(tt.interval)
			if result != tt.expected {
				t.Errorf("Для %s ожидалось %d, получено %d", tt.interval, tt.expected, result)
			}
		})
	}
}

// TestTimestampToISO тест конвертации в ISO 8601
func TestTimestampToISO(t *testing.T) {
	ts := int64(1697366459999)
	iso := TimestampToISO(ts)

	// Проверяем что строка содержит ключевые элементы
	if len(iso) == 0 {
		t.Error("ISO строка пустая")
	}

	// Должна содержать дату и время
	if len(iso) < 20 {
		t.Errorf("ISO строка слишком короткая: %s", iso)
	}
}

// TestTimestampToHuman тест конвертации в человекочитаемый формат
func TestTimestampToHuman(t *testing.T) {
	ts := int64(1697366459999)
	human := TimestampToHuman(ts, "2006-01-02 15:04:05")

	// Проверяем формат
	if len(human) != 19 {
		t.Errorf("Неверная длина: %s", human)
	}
}

// TestConvertTimestampSafely тест безопасной конвертации
func TestConvertTimestampSafely(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int64", int64(1697366459999), 1697366459999},
		{"float64", float64(1697366459999), 1697366459999},
		{"int", int(1697366459999), 1697366459999},
		{"string", "1697366459999", 1697366459999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertTimestampSafely(tt.input)
			if result != tt.expected {
				t.Errorf("Ожидалось %d, получено %d", tt.expected, result)
			}
		})
	}

	// Тест с невалидной строкой должен вернуть текущее время
	t.Run("invalid string", func(t *testing.T) {
		result := ConvertTimestampSafely("invalid")
		if !ValidateTimestamp(result) {
			t.Errorf("Должен вернуть валидный текущий timestamp: %d", result)
		}
	})
}

// TestCreateRedisStreamFields тест создания полей для Redis Stream
func TestCreateRedisStreamFields(t *testing.T) {
	t.Run("With event time", func(t *testing.T) {
		candle := map[string]interface{}{
			"closeTime": int64(1697366459999),
			"symbol":    "BTCUSDT",
			"close":     "28500",
		}

		fields := CreateRedisStreamFields(candle, "timestamp", true, "closeTime")

		// Проверяем timestamp
		if tsStr, ok := fields["timestamp"].(string); ok {
			if tsStr != "1697366459999" {
				t.Errorf("Неверный timestamp: %s", tsStr)
			}
		} else {
			t.Error("timestamp не string")
		}

		// Проверяем другие поля
		if fields["symbol"] != "BTCUSDT" {
			t.Errorf("Неверный symbol: %v", fields["symbol"])
		}
	})

	t.Run("With current time", func(t *testing.T) {
		data := map[string]interface{}{
			"symbol": "BTCUSDT",
			"action": "added",
		}

		fields := CreateRedisStreamFields(data, "timestamp", false, "closeTime")

		// Проверяем что timestamp валидный
		if tsStr, ok := fields["timestamp"].(string); ok {
			// Парсим и валидируем
			var ts int64
			if _, err := fmt.Sscanf(tsStr, "%d", &ts); err == nil {
				if !ValidateTimestamp(ts) {
					t.Errorf("Невалидный timestamp: %s", tsStr)
				}
			} else {
				t.Errorf("Не удалось распарсить timestamp: %s", tsStr)
			}
		} else {
			t.Error("timestamp не string")
		}
	})
}

// TestExtractMaxTimestamp тест извлечения максимального timestamp
func TestExtractMaxTimestamp(t *testing.T) {
	tickers := []map[string]interface{}{
		{"closeTime": int64(1697366459999), "symbol": "BTCUSDT"},
		{"closeTime": int64(1697366460000), "symbol": "ETHUSDT"},
		{"closeTime": int64(1697366458000), "symbol": "BNBUSDT"},
	}

	maxTs := ExtractMaxTimestamp(tickers, "closeTime")
	expected := int64(1697366460000)
	if maxTs != expected {
		t.Errorf("Ожидалось %d, получено %d", expected, maxTs)
	}
}

// TestNormalizeTimeframe тест нормализации таймфрейма
func TestNormalizeTimeframe(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"kline_1m", "1m"},
		{"kline_5m", "5m"},
		{"1m", "1m"},
		{"5m", "5m"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := NormalizeTimeframe(tt.input)
			if result != tt.expected {
				t.Errorf("Ожидалось %s, получено %s", tt.expected, result)
			}
		})
	}
}

// BenchmarkGetCurrentTimestampMs benchmark для GetCurrentTimestampMs
func BenchmarkGetCurrentTimestampMs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetCurrentTimestampMs()
	}
}

// BenchmarkExtractEventTimestamp benchmark для ExtractEventTimestamp
func BenchmarkExtractEventTimestamp(b *testing.B) {
	data := map[string]interface{}{
		"closeTime": int64(1697366459999),
		"symbol":    "BTCUSDT",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractEventTimestamp(data, "closeTime", false)
	}
}
