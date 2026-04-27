package binance

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ProcessTickerData обрабатывает данные тикеров из JSON
func ProcessTickerData(data []byte) ([]TickerData, error) {
	var tickers []TickerData
	if err := json.Unmarshal(data, &tickers); err != nil {
		return nil, fmt.Errorf("парсинг JSON тикеров: %w", err)
	}
	return tickers, nil
}

// ProcessFundingRates обрабатывает данные funding rates из JSON
func ProcessFundingRates(data []byte) ([]FundingRate, error) {
	var fundingRates []FundingRate
	if err := json.Unmarshal(data, &fundingRates); err != nil {
		return nil, fmt.Errorf("парсинг JSON funding rates: %w", err)
	}
	return fundingRates, nil
}

// FilterActivePairs фильтрует активные торговые пары
func FilterActivePairs(tickers []TickerData) []string {
	var activePairs []string

	for _, ticker := range tickers {
		symbol := strings.ToLower(ticker.Symbol)

		// Только USDT пары
		if !strings.HasSuffix(symbol, "usdt") {
			continue
		}

		// Фильтруем по объему торгов (больше определенного порога)
		if ticker.Count > 1000 { // минимум 1000 сделок за 24 часа
			activePairs = append(activePairs, symbol)
		}

		// Ограничиваем количество пар для начала
		if len(activePairs) >= 50 {
			break
		}
	}

	zap.S().Infof("📊 Отфильтровано %d активных торговых пар", len(activePairs))
	return activePairs
}

// ValidateSymbol проверяет валидность символа торговой пары
func ValidateSymbol(symbol string) bool {
	if symbol == "" {
		return false
	}

	// Проверяем, что символ заканчивается на USDT
	if !strings.HasSuffix(strings.ToLower(symbol), "usdt") {
		return false
	}

	// Исключаем UP/DOWN токены
	if strings.Contains(strings.ToUpper(symbol), "UP") ||
		strings.Contains(strings.ToUpper(symbol), "DOWN") {
		return false
	}

	// Проверяем минимальную длину
	if len(symbol) < 5 {
		return false
	}

	return true
}

// ExtractSymbolsFromTickers извлекает символы из данных тикеров
func ExtractSymbolsFromTickers(tickers []TickerData) []string {
	var symbols []string

	for _, ticker := range tickers {
		if ValidateSymbol(ticker.Symbol) {
			symbols = append(symbols, strings.ToLower(ticker.Symbol))
		}
	}

	return symbols
}

// SortPairsByVolume сортирует пары по объему торгов
func SortPairsByVolume(tickers []TickerData) []TickerData {
	// Создаем копию для сортировки
	sorted := make([]TickerData, len(tickers))
	copy(sorted, tickers)

	// Сортировка по объему (убывание)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if getVolumeValue(sorted[i].Volume) < getVolumeValue(sorted[j].Volume) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// getVolumeValue извлекает числовое значение объема
func getVolumeValue(volumeStr string) float64 {
	// Убираем кавычки и пробелы
	volumeStr = strings.Trim(volumeStr, `" `)

	// Парсим как float64
	var volume float64
	if _, err := fmt.Sscanf(volumeStr, "%f", &volume); err != nil {
		return 0
	}

	return volume
}

// FundingRate представляет структуру funding rate из Binance API
type FundingRate struct {
	Symbol               string `json:"symbol"`
	MarkPrice            string `json:"markPrice"`
	IndexPrice           string `json:"indexPrice"`
	EstimatedSettlePrice string `json:"estimatedSettlePrice"`
	LastFundingRate      string `json:"lastFundingRate"`
	NextFundingTime      int64  `json:"nextFundingTime"`
	InterestRate         string `json:"interestRate"`
	Time                 int64  `json:"time"`
}

// FilterHighFundingRates фильтрует funding rates выше порогового значения
func FilterHighFundingRates(rates []FundingRate, threshold float64) []FundingRate {
	var highRates []FundingRate

	for _, rate := range rates {
		if fundingValue := parseFundingRate(rate.LastFundingRate); fundingValue > threshold || fundingValue < -threshold {
			highRates = append(highRates, rate)
		}
	}

	return highRates
}

// parseFundingRate парсит значение funding rate
func parseFundingRate(rateStr string) float64 {
	rateStr = strings.Trim(rateStr, `" `)

	var rate float64
	if _, err := fmt.Sscanf(rateStr, "%f", &rate); err != nil {
		return 0
	}

	return rate
}

// GetFundingRateSummary получает сводку по funding rates
func GetFundingRateSummary(rates []FundingRate) map[string]int {
	summary := map[string]int{
		"total":    len(rates),
		"positive": 0,
		"negative": 0,
		"neutral":  0,
	}

	for _, rate := range rates {
		fundingValue := parseFundingRate(rate.LastFundingRate)

		if fundingValue > 0 {
			summary["positive"]++
		} else if fundingValue < 0 {
			summary["negative"]++
		} else {
			summary["neutral"]++
		}
	}

	return summary
}
