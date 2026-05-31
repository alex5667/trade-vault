// Package timeutil предоставляет утилиты для работы с временными метками в едином формате.
//
// Стандарт проекта: Unix timestamp в миллисекундах (UTC)
package timeutil

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// GetCurrentTimestampMs возвращает текущее время в Unix timestamp в миллисекундах (UTC).
//
// Example:
//
//	ts := GetCurrentTimestampMs()
//	fmt.Println(ts) // 1697366459999
func GetCurrentTimestampMs() int64 {
	return time.Now().UTC().UnixMilli()
}

// FormatTimestampForRedis форматирует timestamp для записи в Redis.
//
// Args:
//   - ts: Unix timestamp в миллисекундах
//
// Returns:
//   - Строковое представление timestamp
//
// Example:
//
//	formatted := FormatTimestampForRedis(1697366459999)
//	fmt.Println(formatted) // "1697366459999"
func FormatTimestampForRedis(ts int64) string {
	return fmt.Sprintf("%d", ts)
}

// ExtractEventTimestamp извлекает timestamp события из данных.
//
// Args:
//   - data: Карта с данными
//   - field: Имя поля с timestamp (например, "closeTime", "timestamp")
//   - fallbackToNow: Если true и timestamp не найден, вернет текущее время
//
// Returns:
//   - Unix timestamp в миллисекундах или 0 если не найден
//
// Note:
//
//	По умолчанию НЕ использует текущее время как fallback!
//	Используйте fallbackToNow=true только для служебных событий.
//
// Example:
//
//	candleData := map[string]interface{}{
//	    "closeTime": int64(1697366459999),
//	    "symbol":    "BTCUSDT",
//	}
//	ts := ExtractEventTimestamp(candleData, "closeTime", false)
//	fmt.Println(ts) // 1697366459999
func ExtractEventTimestamp(data map[string]interface{}, field string, fallbackToNow bool) int64 {
	value, exists := data[field]
	if !exists {
		if fallbackToNow {
			return GetCurrentTimestampMs()
		}
		return 0
	}

	// Обработка различных типов
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}

	// Если не удалось извлечь
	if fallbackToNow {
		return GetCurrentTimestampMs()
	}
	return 0
}

// ExtractBinanceCloseTime извлекает closeTime из данных свечи Binance (поддерживает разные форматы).
//
// Args:
//   - candleData: Данные свечи от Binance
//
// Returns:
//   - Unix timestamp в миллисекундах
//
// Example:
//
//	candle := map[string]interface{}{
//	    "T":      int64(1697366459999),
//	    "symbol": "BTCUSDT",
//	}
//	ts := ExtractBinanceCloseTime(candle)
//	fmt.Println(ts) // 1697366459999
func ExtractBinanceCloseTime(candleData map[string]interface{}) int64 {
	// Binance использует разные поля в разных API
	fields := []string{"closeTime", "T", "close_time"}
	for _, field := range fields {
		if ts := ExtractEventTimestamp(candleData, field, false); ts != 0 {
			return ts
		}
	}

	// Если нет closeTime, пробуем openTime + interval
	openTime := ExtractEventTimestamp(candleData, "openTime", false)
	if openTime == 0 {
		openTime = ExtractEventTimestamp(candleData, "t", false)
	}

	if openTime != 0 {
		// Определяем interval и добавляем к openTime
		interval := "1m"
		if i, ok := candleData["i"].(string); ok {
			interval = i
		}
		intervalMs := ParseIntervalToMs(interval)
		// КРИТИЧНЫЙ ФИКС: Биржевая свеча закрывается на 1 мс раньше следующей!
		// Если Open = 1000, Interval = 60000, то Close ОБЯЗАН быть 60999, а не 61000.
		return openTime + intervalMs - 1
	}

	return 0
}

// ParseIntervalToMs конвертирует строковый interval ('1m', '5m', '1h', etc.) в миллисекунды.
//
// Args:
//   - interval: Интервал в формате Binance ('1m', '5m', '15m', '1h', '4h', '1d', etc.)
//
// Returns:
//   - Количество миллисекунд
//
// Example:
//
//	ms := ParseIntervalToMs("1m")
//	fmt.Println(ms) // 60000
func ParseIntervalToMs(interval string) int64 {
	intervals := map[string]int64{
		"1m":  60 * 1000,
		"3m":  3 * 60 * 1000,
		"5m":  5 * 60 * 1000,
		"15m": 15 * 60 * 1000,
		"30m": 30 * 60 * 1000,
		"1h":  60 * 60 * 1000,
		"2h":  2 * 60 * 60 * 1000,
		"4h":  4 * 60 * 60 * 1000,
		"6h":  6 * 60 * 60 * 1000,
		"8h":  8 * 60 * 60 * 1000,
		"12h": 12 * 60 * 60 * 1000,
		"1d":  24 * 60 * 60 * 1000,
		"3d":  3 * 24 * 60 * 60 * 1000,
		"1w":  7 * 24 * 60 * 60 * 1000,
		"1M":  30 * 24 * 60 * 60 * 1000, // Приблизительно
	}

	if ms, ok := intervals[interval]; ok {
		return ms
	}
	return 60 * 1000 // По умолчанию 1 минута
}

// TimestampToISO конвертирует Unix timestamp в миллисекундах в ISO 8601 строку.
// Используется для логов и отображения (НЕ для хранения в Redis).
//
// Args:
//   - tsMs: Unix timestamp в миллисекундах
//
// Returns:
//   - ISO 8601 строка с UTC timezone
//
// Example:
//
//	iso := TimestampToISO(1697366459999)
//	fmt.Println(iso) // "2023-10-15T12:34:19.999Z"
func TimestampToISO(tsMs int64) string {
	t := time.UnixMilli(tsMs).UTC()
	return t.Format(time.RFC3339Nano)
}

// TimestampToHuman конвертирует Unix timestamp в человекочитаемую строку.
// Используется для отображения пользователю (НЕ для хранения).
//
// Args:
//   - tsMs: Unix timestamp в миллисекундах
//   - layout: Формат строки (time.Layout), пустая строка = RFC3339
//
// Returns:
//   - Форматированная строка
//
// Example:
//
//	human := TimestampToHuman(1697366459999, "2006-01-02 15:04:05")
//	fmt.Println(human) // "2023-10-15 12:34:19"
func TimestampToHuman(tsMs int64, layout string) string {
	t := time.UnixMilli(tsMs).UTC()
	if layout == "" {
		layout = time.RFC3339
	}
	return t.Format(layout)
}

// ValidateTimestamp валидирует timestamp (проверяет, что это разумное значение).
//
// Args:
//   - ts: Timestamp для проверки
//
// Returns:
//   - true если timestamp валидный
//
// Example:
//
//	valid := ValidateTimestamp(1697366459999)
//	fmt.Println(valid) // true
func ValidateTimestamp(ts int64) bool {
	// Проверяем диапазон (после 2020 и до 2033)
	return ts > 1600000000000 && ts < 2000000000000
}

// ConvertTimestampSafely безопасно конвертирует timestamp из различных типов в int64.
//
// Args:
//   - timestamp: Значение для конвертации
//
// Returns:
//   - Unix timestamp в миллисекундах или текущее время если конвертация не удалась
//
// Example:
//
//	ts := ConvertTimestampSafely(1697366459999.0)
//	fmt.Println(ts) // 1697366459999
func ConvertTimestampSafely(timestamp interface{}) int64 {
	switch v := timestamp.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		// Если не удалось распарсить, возвращаем текущее время
		return GetCurrentTimestampMs()
	default:
		// Для неизвестных типов возвращаем текущее время
		return GetCurrentTimestampMs()
	}
}

// CreateRedisStreamFields создает map с полями для Redis Stream с правильным timestamp.
//
// Args:
//   - data: Исходные данные
//   - timestampField: Имя поля для timestamp в результате
//   - useEventTime: Использовать время события (true) или текущее (false)
//   - eventTimeField: Имя поля с временем события в data
//
// Returns:
//   - Map с полями для XAdd, все значения - interface{}
//
// Example:
//
//	candle := map[string]interface{}{
//	    "closeTime": int64(1697366459999),
//	    "symbol":    "BTCUSDT",
//	    "close":     "28500",
//	}
//	fields := CreateRedisStreamFields(candle, "timestamp", true, "closeTime")
//	fmt.Println(fields["timestamp"]) // "1697366459999"
func CreateRedisStreamFields(
	data map[string]interface{},
	timestampField string,
	useEventTime bool,
	eventTimeField string,
) map[string]interface{} {
	fields := make(map[string]interface{})

	// Определяем timestamp
	var ts int64
	if useEventTime {
		ts = ExtractEventTimestamp(data, eventTimeField, true)
	} else {
		ts = GetCurrentTimestampMs()
	}

	fields[timestampField] = FormatTimestampForRedis(ts)

	// Добавляем остальные данные
	for key, value := range data {
		if value != nil && key != timestampField {
			fields[key] = value
		}
	}

	return fields
}

// ExtractMaxTimestamp извлекает максимальный timestamp из массива данных.
// Полезно для батч-данных (например, ticker-24h).
//
// Args:
//   - dataArray: Массив с данными
//   - field: Имя поля с timestamp
//
// Returns:
//   - Максимальный timestamp или 0 если не найден
//
// Example:
//
//	tickers := []map[string]interface{}{
//	    {"closeTime": int64(1697366459999), "symbol": "BTCUSDT"},
//	    {"closeTime": int64(1697366460000), "symbol": "ETHUSDT"},
//	}
//	maxTs := ExtractMaxTimestamp(tickers, "closeTime")
//	fmt.Println(maxTs) // 1697366460000
func ExtractMaxTimestamp(dataArray []map[string]interface{}, field string) int64 {
	var maxTs int64
	for _, item := range dataArray {
		ts := ExtractEventTimestamp(item, field, false)
		if ts > maxTs {
			maxTs = ts
		}
	}
	return maxTs
}

// NormalizeTimeframe нормализует название таймфрейма (убирает префикс "kline_").
//
// Args:
//   - timeframe: Строка с таймфреймом
//
// Returns:
//   - Нормализованный таймфрейм
//
// Example:
//
//	tf := NormalizeTimeframe("kline_1m")
//	fmt.Println(tf) // "1m"
func NormalizeTimeframe(timeframe string) string {
	return strings.TrimPrefix(timeframe, "kline_")
}

// Алиасы для удобства
var (
	// GetUTCTimestampMs - алиас для GetCurrentTimestampMs
	GetUTCTimestampMs = GetCurrentTimestampMs
	// FormatTS - алиас для FormatTimestampForRedis
	FormatTS = FormatTimestampForRedis
	// ExtractTS - алиас для ExtractEventTimestamp
	ExtractTS = ExtractEventTimestamp
)
