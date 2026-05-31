// Пакет binance содержит клиента REST/WS Binance и публикацию данных в Redis Streams.
package binance

import (
	"encoding/json"
	"strings"
)

// Timeframe представляет таймфрейм для свечей Binance
type Timeframe string

const (
	// Минутные таймфреймы
	M1  Timeframe = "kline_1m"
	M5  Timeframe = "kline_5m"
	M15 Timeframe = "kline_15m"

	// Часовые таймфреймы
	H1 Timeframe = "kline_1h"
	H4 Timeframe = "kline_4h"

	// Дневные таймфреймы
	D1 Timeframe = "kline_1d"

	// Недельные и месячные таймфреймы
	W1  Timeframe = "kline_1w"
	MN1 Timeframe = "kline_1M"

	// Квартальные и годовые таймфреймы
	Q1 Timeframe = "kline_3M" // 3 месяца (квартал)
	Y1 Timeframe = "kline_1y" // 1 год
)

// Маппинг коротких названий на полные таймфреймы
var shortNameMapping = map[string]Timeframe{
	"M1":  M1,
	"M5":  M5,
	"M15": M15,
	"H1":  H1,
	"H4":  H4,
	"D1":  D1,
	"W1":  W1,
	"MN1": MN1,
	"Q1":  Q1, // Квартал (3 месяца)
	"Y1":  Y1, // Год
}

var timeframeShortName = map[Timeframe]string{
	M1:  "M1",
	M5:  "M5",
	M15: "M15",
	H1:  "H1",
	H4:  "H4",
	D1:  "D1",
	W1:  "W1",
	MN1: "MN1",
	Q1:  "Q1",
	Y1:  "Y1",
}

// String возвращает строковое представление таймфрейма
func (t Timeframe) String() string {
	return string(t)
}

// IsValid проверяет, является ли таймфрейм валидным
func (t Timeframe) IsValid() bool {
	switch t {
	case M1, M5, M15, H1, H4, D1, W1, MN1, Q1, Y1:
		return true
	default:
		return false
	}
}

// GetTimeframeByString возвращает Timeframe по строковому значению
// Поддерживает как полные названия (kline_1m), так и короткие (M1)
func GetTimeframeByString(s string) (Timeframe, bool) {
	// Сначала проверяем полное название
	tf := Timeframe(s)
	if tf.IsValid() {
		return tf, true
	}

	// Если не найдено, проверяем короткое название
	if shortTf, exists := shortNameMapping[s]; exists {
		return shortTf, true
	}

	return "", false
}

// GetAllTimeframes возвращает все доступные таймфреймы
func GetAllTimeframes() []Timeframe {
	return []Timeframe{M1, M5, M15, H1, H4, D1, W1, MN1, Q1, Y1}
}

// SerializeTimeframesForRedis преобразует таймфреймы в строку JSON с короткими названиями.
func SerializeTimeframesForRedis(timeframes []Timeframe) string {
	shortNames := TimeframesToShortNames(timeframes)
	if len(shortNames) == 0 {
		return "[]"
	}

	data, err := json.Marshal(shortNames)
	if err != nil {
		return "[]"
	}

	return string(data)
}

// TimeframesToShortNames возвращает слайс коротких названий таймфреймов.
func TimeframesToShortNames(timeframes []Timeframe) []string {
	result := make([]string, 0, len(timeframes))

	for _, tf := range timeframes {
		if name, ok := timeframeShortName[tf]; ok {
			result = append(result, name)
			continue
		}

		tfStr := strings.TrimPrefix(strings.ToUpper(tf.String()), "KLINE_")
		if tfStr != "" {
			result = append(result, tfStr)
		}
	}

	return result
}

// TestTimeframes тестирует работу с таймфреймами
func TestTimeframes() {
	// Тестируем новые таймфреймы
	if Q1 != "kline_3M" {
		panic("Q1 должен быть равен kline_3M")
	}

	if Y1 != "kline_1y" {
		panic("Y1 должен быть равен kline_1y")
	}

	// Тестируем маппинг
	if shortNameMapping["Q1"] != Q1 {
		panic("shortNameMapping['Q1'] должен быть равен Q1")
	}

	if shortNameMapping["Y1"] != Y1 {
		panic("shortNameMapping['Y1'] должен быть равен Y1")
	}

	// Тестируем валидацию
	if !Q1.IsValid() {
		panic("Q1 должен быть валидным")
	}

	if !Y1.IsValid() {
		panic("Y1 должен быть валидным")
	}

	// Тестируем GetTimeframeByString
	if tf, valid := GetTimeframeByString("Q1"); !valid || tf != Q1 {
		panic("GetTimeframeByString('Q1') должен возвращать Q1")
	}

	if tf, valid := GetTimeframeByString("Y1"); !valid || tf != Y1 {
		panic("GetTimeframeByString('Y1') должен возвращать Y1")
	}

	if tf, valid := GetTimeframeByString("kline_3M"); !valid || tf != Q1 {
		panic("GetTimeframeByString('kline_3M') должен возвращать Q1")
	}

	if tf, valid := GetTimeframeByString("kline_1y"); !valid || tf != Y1 {
		panic("GetTimeframeByString('kline_1y') должен возвращать Y1")
	}
}
