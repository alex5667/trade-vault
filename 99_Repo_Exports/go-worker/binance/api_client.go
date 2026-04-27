// Пакет binance содержит клиента REST/WS Binance и публикацию данных в Redis Streams.
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"
	"go-worker/pkg/timeutil"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// URL для API запросов
const (
	// URL для получения 24-часовой статистики тикеров
	TICKER_24H_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
	// URL для получения ставок финансирования
	FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate?limit=100"
	// URL для получения информации об инструментах
	EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
)

// BinanceAPIClient структура для работы с Binance API
type BinanceAPIClient struct {
	httpClient  *http.Client
	redisClient *redis.Client
}

// NewBinanceAPIClient создает новый экземпляр API клиента
func NewBinanceAPIClient() *BinanceAPIClient {
	return &BinanceAPIClient{
		httpClient: &http.Client{
			Timeout: getEnvDuration("API_CLIENT_TIMEOUT", 10*time.Second),
		},
		redisClient: redisclient.Client,
	}
}

// FetchAndPublishMarketData выполняет запросы к Binance и отправляет данные в Redis
func FetchAndPublishMarketData(ctx context.Context) error {
	client := NewBinanceAPIClient()

	if err := client.fetchAndPublishTickerData(ctx, streams.Ticker24h); err != nil {
		zap.S().Errorf("❌ Ошибка получения тикеров: %v", err)
	}

	if err := client.fetchAndPublishFundingRates(ctx, streams.FundingRate); err != nil {
		zap.S().Errorf("❌ Ошибка получения ставок финансирования: %v", err)
	}

	return nil
}

// GetActiveTradingPairs получает список активных торговых пар USDT из Binance
func GetActiveTradingPairs(ctx context.Context) ([]string, error) {
	client := NewBinanceAPIClient()
	return client.getActiveTradingPairs(ctx)
}

// InitializeConnectionsFromAPI получает пары из API и инициализирует WebSocket подключения
func InitializeConnectionsFromAPI(ctx context.Context, connectionManager interface{}) error {
	client := NewBinanceAPIClient()

	// Получаем активные торговые пары
	pairs, err := client.getActiveTradingPairs(ctx)
	if err != nil {
		return fmt.Errorf("получение активных торговых пар: %w", err)
	}

	// Используем интерфейс для обновления подключений
	if manager, ok := connectionManager.(interface{ UpdateConnections([]string) }); ok {
		zap.S().Infof("🚀 Инициализация WebSocket подключений для %d пар...", len(pairs))
		manager.UpdateConnections(pairs)
		return nil
	}

	return fmt.Errorf("connectionManager не поддерживает метод UpdateConnections")
}

// fetchAndPublishTickerData получает данные тикеров и публикует их в Redis Stream
func (c *BinanceAPIClient) fetchAndPublishTickerData(ctx context.Context, streamName string) error {
	zap.S().Info("🌐 Запрос к Binance:", TICKER_24H_URL)

	// Создаем запрос с контекстом для возможной отмены
	req, err := http.NewRequestWithContext(ctx, "GET", TICKER_24H_URL, nil)
	if err != nil {
		return fmt.Errorf("создание запроса тикеров: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP запрос тикеров: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("чтение тела ответа тикеров: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("неверный статус %d: %s", resp.StatusCode, string(body))
	}

	// Парсим JSON для извлечения максимального closeTime
	var tickers []TickerData
	if err := json.Unmarshal(body, &tickers); err != nil {
		// Если не удалось распарсить, используем текущее время
		zap.S().Errorf("⚠️ Ошибка парсинга тикеров для извлечения closeTime: %v", err)
	}

	// Находим максимальный closeTime
	var maxCloseTime int64
	for _, ticker := range tickers {
		if ticker.CloseTime > maxCloseTime {
			maxCloseTime = ticker.CloseTime
		}
	}

	// Если не нашли closeTime, используем текущее время
	if maxCloseTime == 0 {
		maxCloseTime = timeutil.GetCurrentTimestampMs()
		zap.S().Warnf("⚠️ closeTime не найден в тикерах, используем текущее время")
	}

	// Публикуем в Redis Stream вместо канала
	fields := map[string]interface{}{
		"data":      string(body),
		"type":      "ticker_24h",
		"timestamp": timeutil.FormatTimestampForRedis(maxCloseTime), // Время события (UTC ms)
	}

	if _, err := redisclient.XAddWithRetry(redisclient.Ctx, c.redisClient, &redis.XAddArgs{
		Stream: streamName,
		Values: fields,
	}); err != nil {
		return fmt.Errorf("публикация тикеров в Redis Stream: %w", err)
	}

	// Закомментировано для уменьшения шума в логах
	// zap.S().Infof("📡 Тикеры опубликованы в Redis Stream: %s", streamName)
	return nil
}

// fetchAndPublishFundingRates получает ставки финансирования и публикует их в Redis Stream
func (c *BinanceAPIClient) fetchAndPublishFundingRates(ctx context.Context, streamName string) error {
	zap.S().Info("🌐 Запрос к Binance:", FUNDING_RATE_URL)

	// Создаем запрос с контекстом для возможной отмены
	req, err := http.NewRequestWithContext(ctx, "GET", FUNDING_RATE_URL, nil)
	if err != nil {
		return fmt.Errorf("создание запроса ставок финансирования: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP запрос ставок финансирования: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("чтение тела ответа ставок финансирования: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("неверный статус %d: %s", resp.StatusCode, string(body))
	}

	// Парсим JSON для извлечения максимального fundingTime
	type FundingRate struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}

	var fundingRates []FundingRate
	if err := json.Unmarshal(body, &fundingRates); err != nil {
		// Если не удалось распарсить, используем текущее время
		zap.S().Errorf("⚠️ Ошибка парсинга funding rates для извлечения fundingTime: %v", err)
	}

	// Находим максимальный fundingTime
	var maxFundingTime int64
	for _, rate := range fundingRates {
		if rate.FundingTime > maxFundingTime {
			maxFundingTime = rate.FundingTime
		}
	}

	// Если не нашли fundingTime, используем текущее время
	if maxFundingTime == 0 {
		maxFundingTime = timeutil.GetCurrentTimestampMs()
		zap.S().Warnf("⚠️ fundingTime не найден, используем текущее время")
	}

	// Публикуем в Redis Stream вместо канала
	fields := map[string]interface{}{
		"data":      string(body),
		"type":      "funding_rates",
		"timestamp": timeutil.FormatTimestampForRedis(maxFundingTime), // Время события (UTC ms)
	}

	if _, err := redisclient.XAddWithRetry(redisclient.Ctx, c.redisClient, &redis.XAddArgs{
		Stream: streamName,
		Values: fields,
	}); err != nil {
		return fmt.Errorf("публикация ставок финансирования в Redis Stream: %w", err)
	}

	zap.S().Infof("📡 Ставки финансирования опубликованы в Redis Stream: %s", streamName)
	return nil
}

// getActiveTradingPairs получает список активных торговых пар USDT из Binance
func (c *BinanceAPIClient) getActiveTradingPairs(ctx context.Context) ([]string, error) {
	zap.S().Info("🌐 Запрос активных торговых пар из Binance...")

	// Получаем 24-часовую статистику для определения активных пар
	req, err := http.NewRequestWithContext(ctx, "GET", TICKER_24H_URL, nil)
	if err != nil {
		return nil, fmt.Errorf("создание запроса тикеров: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP запрос тикеров: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("неверный статус ответа: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("чтение тела ответа: %w", err)
	}

	var tickers []TickerData
	if err := json.Unmarshal(body, &tickers); err != nil {
		return nil, fmt.Errorf("парсинг JSON: %w", err)
	}

	// Фильтруем активные USDT пары
	var activePairs []string
	for _, ticker := range tickers {
		symbol := strings.ToLower(ticker.Symbol)

		// Только USDT пары
		if !strings.HasSuffix(symbol, "usdt") {
			continue
		}

		// Фильтруем по объему торгов (больше определенного порога)
		// Можно настроить критерии активности здесь
		if ticker.Count > 1000 { // минимум 1000 сделок за 24 часа
			activePairs = append(activePairs, symbol)
		}

		// Ограничиваем количество пар для начала
		if len(activePairs) >= 50 {
			break
		}
	}

	zap.S().Infof("📊 Найдено %d активных торговых пар", len(activePairs))
	return activePairs, nil
}

// TickerData представляет структуру тикера из Binance API
type TickerData struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	PrevClosePrice     string `json:"prevClosePrice"`
	LastPrice          string `json:"lastPrice"`
	LastQty            string `json:"lastQty"`
	BidPrice           string `json:"bidPrice"`
	BidQty             string `json:"bidQty"`
	AskPrice           string `json:"askPrice"`
	AskQty             string `json:"askQty"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	FirstId            int64  `json:"firstId"`
	LastId             int64  `json:"lastId"`
	Count              int64  `json:"count"`
}

// GetTickerDataBySymbol получает данные тикера по конкретному символу
func (c *BinanceAPIClient) GetTickerDataBySymbol(ctx context.Context, symbol string) (*TickerData, error) {
	url := fmt.Sprintf("%s?symbol=%s", TICKER_24H_URL, strings.ToUpper(symbol))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("создание запроса тикера для %s: %w", symbol, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP запрос тикера для %s: %w", symbol, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("неверный статус ответа для %s: %d", symbol, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("чтение тела ответа для %s: %w", symbol, err)
	}

	var ticker TickerData
	if err := json.Unmarshal(body, &ticker); err != nil {
		return nil, fmt.Errorf("парсинг JSON для %s: %w", symbol, err)
	}

	return &ticker, nil
}

// GetTickerDataBySymbol получает данные тикера по конкретному символу (публичная функция)
func GetTickerDataBySymbol(ctx context.Context, symbol string) (*TickerData, error) {
	client := NewBinanceAPIClient()
	return client.GetTickerDataBySymbol(ctx, symbol)
}

// ExchangeInfoSymbol представляет символ из exchangeInfo
type ExchangeInfoSymbol struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

// ExchangeInfo представляет ответ от /exchangeInfo
type ExchangeInfo struct {
	Symbols []ExchangeInfoSymbol `json:"symbols"`
}
