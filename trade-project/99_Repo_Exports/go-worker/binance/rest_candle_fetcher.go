// Package binance - REST API polling для редких таймфреймов
// Автор: Senior Go Developer Team
// Назначение: Получение данных для таймфреймов, не поддерживаемых WebSocket (3M, 1y)
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"go-worker/infra/redisclient"
	"go-worker/internal/streams"
	"go-worker/pkg/timeutil"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// RestCandleFetcher получает данные свечей через REST API для редких таймфреймов
type RestCandleFetcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	// HTTP клиент с таймаутами
	httpClient *http.Client

	// Redis для публикации
	redisClient        *redis.Client
	redisClientCandles *redis.Client // Второй Redis для candles

	// Конфигурация
	symbols   []string
	timeframe string // "3M" или "1y"
	interval  string // Binance interval: "1M" для аппроксимации

	// Умное управление опросами
	lastFetchTime map[string]time.Time // symbol -> last fetch time
	fetchMutex    sync.RWMutex

	// Интервал опроса (зависит от таймфрейма)
	pollInterval time.Duration

	// Circuit breaker
	consecutiveFailures int64
	circuitOpen         bool
	circuitMutex        sync.RWMutex

	// Статистика
	totalFetches   uint64
	totalPublished uint64
	totalErrors    uint64
}

// BinanceKlineResponse - ответ от REST API
type BinanceKlineResponse [][]interface{}

// RestCandleConfig конфигурация fetcher
type RestCandleConfig struct {
	Symbols      []string
	Timeframe    string // "3M" или "1y"
	PollInterval time.Duration
}

// NewRestCandleFetcher создает новый REST fetcher для редких таймфреймов
func NewRestCandleFetcher(config RestCandleConfig) *RestCandleFetcher {
	ctx, cancel := context.WithCancel(context.Background())

	// Определяем реальный Binance interval для аппроксимации
	binanceInterval := mapToBinanceInterval(config.Timeframe)

	// Умный poll interval зависит от таймфрейма
	pollInterval := config.PollInterval
	if pollInterval == 0 {
		pollInterval = calculateOptimalPollInterval(config.Timeframe)
	}

	fetcher := &RestCandleFetcher{
		ctx:           ctx,
		cancel:        cancel,
		symbols:       config.Symbols,
		timeframe:     config.Timeframe,
		interval:      binanceInterval,
		pollInterval:  pollInterval,
		lastFetchTime: make(map[string]time.Time),
		httpClient: &http.Client{
			Timeout: getEnvDuration("REST_CANDLE_TIMEOUT", 30*time.Second),
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
				DisableKeepAlives:   false,
			},
		},
		redisClient:        redisclient.Client,
		redisClientCandles: redisclient.CandlesClient,
	}

	zap.S().Infof("🔧 REST Candle Fetcher инициализирован:")
	zap.S().Infof("   Таймфрейм: %s (использует Binance interval: %s)", config.Timeframe, binanceInterval)
	zap.S().Infof("   Символы: %d", len(config.Symbols))
	zap.S().Infof("   Poll interval: %v", pollInterval)

	return fetcher
}

// mapToBinanceInterval маппинг наших таймфреймов на Binance intervals
func mapToBinanceInterval(timeframe string) string {
	switch timeframe {
	case "3M", "kline_3M":
		// Квартал - аппроксимируем месячными свечами
		return "1M"
	case "1y", "kline_1y":
		// Год - аппроксимируем месячными свечами
		return "1M"
	default:
		return "1M"
	}
}

// calculateOptimalPollInterval рассчитывает оптимальный интервал опроса
func calculateOptimalPollInterval(timeframe string) time.Duration {
	switch timeframe {
	case "3M", "kline_3M":
		// Квартальные свечи - проверяем раз в час
		return 1 * time.Hour
	case "1y", "kline_1y":
		// Годовые свечи - проверяем раз в 6 часов
		return 6 * time.Hour
	default:
		return 1 * time.Hour
	}
}

// Start запускает fetcher
func (rcf *RestCandleFetcher) Start() {
	zap.S().Infof("🚀 Запуск REST Candle Fetcher для таймфрейма %s", rcf.timeframe)

	// Первая загрузка сразу
	go rcf.fetchAllSymbols()

	// Периодический опрос
	ticker := time.NewTicker(rcf.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rcf.fetchAllSymbols()
		case <-rcf.ctx.Done():
			zap.S().Infof("👋 REST Candle Fetcher остановлен")
			return
		}
	}
}

// Stop останавливает fetcher
func (rcf *RestCandleFetcher) Stop() {
	rcf.cancel()
}

// fetchAllSymbols загружает данные для всех символов
func (rcf *RestCandleFetcher) fetchAllSymbols() {
	// Проверяем circuit breaker
	rcf.circuitMutex.RLock()
	if rcf.circuitOpen {
		failures := atomic.LoadInt64(&rcf.consecutiveFailures)
		rcf.circuitMutex.RUnlock()

		if failures%10 == 0 {
			zap.S().Errorf("⚡ Circuit breaker open: %d consecutive failures, waiting before retry", failures)
		}

		// Пробуем закрыть circuit через 5 минут
		if failures > 0 && time.Now().Unix()%300 == 0 {
			rcf.circuitMutex.Lock()
			rcf.circuitOpen = false
			rcf.circuitMutex.Unlock()
		}
		return
	}
	rcf.circuitMutex.RUnlock()

	successCount := 0
	failureCount := 0

	for _, symbol := range rcf.symbols {
		if err := rcf.fetchAndPublish(symbol); err != nil {
			failureCount++
			atomic.AddUint64(&rcf.totalErrors, 1)

			// Логируем только каждую 100-ю ошибку
			if atomic.LoadUint64(&rcf.totalErrors)%100 == 0 {
				zap.S().Errorf("❌ Ошибка получения данных для %s (%s): %v", symbol, rcf.timeframe, err)
			}
		} else {
			successCount++
			atomic.StoreInt64(&rcf.consecutiveFailures, 0) // Сбрасываем при успехе
		}

		// Небольшая задержка между запросами (rate limiting)
		time.Sleep(100 * time.Millisecond)
	}

	// Обновляем circuit breaker
	if failureCount > 0 {
		failures := atomic.AddInt64(&rcf.consecutiveFailures, int64(failureCount))
		if failures >= 50 {
			rcf.circuitMutex.Lock()
			rcf.circuitOpen = true
			rcf.circuitMutex.Unlock()
			zap.S().Errorf("🔴 Circuit breaker открыт после %d неудач", failures)
		}
	}

	atomic.AddUint64(&rcf.totalFetches, 1)

	// Логируем статистику каждые 10 циклов
	if atomic.LoadUint64(&rcf.totalFetches)%10 == 0 {
		zap.S().Errorf("📊 REST Fetcher stats (%s): успех=%d, ошибки=%d, всего опубликовано=%d",
			rcf.timeframe, successCount, failureCount, atomic.LoadUint64(&rcf.totalPublished))
	}
}

// fetchAndPublish получает и публикует данные для одного символа
func (rcf *RestCandleFetcher) fetchAndPublish(symbol string) error {
	// Проверяем нужно ли обновлять данные
	if !rcf.shouldFetch(symbol) {
		return nil
	}

	// Получаем свечи через REST API
	candles, err := rcf.fetchCandles(symbol)
	if err != nil {
		return fmt.Errorf("fetch candles: %w", err)
	}

	if len(candles) == 0 {
		return fmt.Errorf("no candles returned")
	}

	// Агрегируем в нужный таймфрейм и публикуем
	if err := rcf.aggregateAndPublish(symbol, candles); err != nil {
		return fmt.Errorf("aggregate and publish: %w", err)
	}

	// Обновляем время последнего получения
	rcf.fetchMutex.Lock()
	rcf.lastFetchTime[symbol] = time.Now()
	rcf.fetchMutex.Unlock()

	return nil
}

// shouldFetch определяет нужно ли обновлять данные
func (rcf *RestCandleFetcher) shouldFetch(symbol string) bool {
	rcf.fetchMutex.RLock()
	lastTime, exists := rcf.lastFetchTime[symbol]
	rcf.fetchMutex.RUnlock()

	if !exists {
		return true // Первый раз
	}

	// Проверяем прошло ли достаточно времени
	elapsed := time.Since(lastTime)
	minInterval := rcf.pollInterval / 2 // Минимум половина интервала

	return elapsed >= minInterval
}

// fetchCandles получает свечи через REST API
func (rcf *RestCandleFetcher) fetchCandles(symbol string) (BinanceKlineResponse, error) {
	// Определяем сколько свечей нужно для агрегации
	limit := calculateRequiredCandles(rcf.timeframe)

	// Binance Futures REST API endpoint
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		symbol, rcf.interval, limit)

	req, err := http.NewRequestWithContext(rcf.ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := rcf.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, string(body))
	}

	var candles BinanceKlineResponse
	if err := json.NewDecoder(resp.Body).Decode(&candles); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}

	return candles, nil
}

// calculateRequiredCandles рассчитывает сколько месячных свечей нужно
func calculateRequiredCandles(timeframe string) int {
	switch timeframe {
	case "3M", "kline_3M":
		// Для квартала берем последние 4 месяца (3 + 1 для контекста)
		return 4
	case "1y", "kline_1y":
		// Для года берем последние 13 месяцев (12 + 1 для контекста)
		return 13
	default:
		return 12
	}
}

// aggregateAndPublish агрегирует месячные свечи в нужный таймфрейм
func (rcf *RestCandleFetcher) aggregateAndPublish(symbol string, candles BinanceKlineResponse) error {
	if len(candles) == 0 {
		return fmt.Errorf("no candles to aggregate")
	}

	// Берем последнюю полную свечу (предпоследняя, так как последняя может быть незакрытой)
	var targetCandle []interface{}

	switch rcf.timeframe {
	case "3M", "kline_3M":
		// Для квартала агрегируем последние 3 месяца
		targetCandle = rcf.aggregateQuarterly(candles)
	case "1y", "kline_1y":
		// Для года агрегируем последние 12 месяцев
		targetCandle = rcf.aggregateYearly(candles)
	default:
		return fmt.Errorf("unsupported timeframe: %s", rcf.timeframe)
	}

	if targetCandle == nil {
		return fmt.Errorf("failed to aggregate candle")
	}

	// Публикуем в Redis
	return rcf.publishCandle(symbol, targetCandle)
}

// aggregateQuarterly агрегирует 3 последние месячные свечи в квартальную
func (rcf *RestCandleFetcher) aggregateQuarterly(candles BinanceKlineResponse) []interface{} {
	if len(candles) < 3 {
		return nil
	}

	// Берем последние 3 закрытые месячные свечи
	recent := candles[len(candles)-4 : len(candles)-1]

	return aggregateCandles(recent)
}

// aggregateYearly агрегирует 12 последних месячных свечей в годовую
func (rcf *RestCandleFetcher) aggregateYearly(candles BinanceKlineResponse) []interface{} {
	if len(candles) < 12 {
		return nil
	}

	// Берем последние 12 закрытых месячных свечей
	recent := candles[len(candles)-13 : len(candles)-1]

	return aggregateCandles(recent)
}

// aggregateCandles объединяет несколько свечей в одну
func aggregateCandles(candles BinanceKlineResponse) []interface{} {
	if len(candles) == 0 {
		return nil
	}

	first := candles[0]
	last := candles[len(candles)-1]

	// Рассчитываем агрегированные значения
	openTime := first[0].(float64)
	closeTime := last[6].(float64)
	open := candleToString(first[1])
	high := findMaxCandle(candles, 2) // index 2 = high
	low := findMinCandle(candles, 3)  // index 3 = low
	close := candleToString(last[4])
	volume := sumCandleVolumes(candles, 5)      // index 5 = volume
	quoteVolume := sumCandleVolumes(candles, 7) // index 7 = quote volume
	trades := sumCandleTrades(candles, 8)       // index 8 = trades

	return []interface{}{
		openTime,    // 0: Open time
		open,        // 1: Open
		high,        // 2: High
		low,         // 3: Low
		close,       // 4: Close
		volume,      // 5: Volume
		closeTime,   // 6: Close time
		quoteVolume, // 7: Quote asset volume
		trades,      // 8: Number of trades
		"0",         // 9: Taker buy base asset volume
		"0",         // 10: Taker buy quote asset volume
		"0",         // 11: Ignore
	}
}

// Вспомогательные функции для агрегации
func candleToString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func findMaxCandle(candles BinanceKlineResponse, index int) string {
	max := 0.0
	for _, candle := range candles {
		val := candleToFloat(candle[index])
		if val > max {
			max = val
		}
	}
	return fmt.Sprintf("%.8f", max)
}

func findMinCandle(candles BinanceKlineResponse, index int) string {
	min := 999999999.0
	for _, candle := range candles {
		val := candleToFloat(candle[index])
		if val < min && val > 0 {
			min = val
		}
	}
	return fmt.Sprintf("%.8f", min)
}

func sumCandleVolumes(candles BinanceKlineResponse, index int) string {
	sum := 0.0
	for _, candle := range candles {
		sum += candleToFloat(candle[index])
	}
	return fmt.Sprintf("%.8f", sum)
}

func sumCandleTrades(candles BinanceKlineResponse, index int) float64 {
	sum := 0.0
	for _, candle := range candles {
		sum += candleToFloat(candle[index])
	}
	return sum
}

func candleToFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	default:
		return 0
	}
}

// publishCandle публикует свечу в Redis напрямую (аналогично connections.Manager)
func (rcf *RestCandleFetcher) publishCandle(symbol string, candle []interface{}) error {
	// Преобразуем в формат Binance kline
	openTime := int64(candle[0].(float64))
	closeTime := int64(candle[6].(float64))
	trades := int64(candle[8].(float64))
	candleData := map[string]interface{}{
		"t": openTime,            // Open time
		"T": closeTime,           // Close time
		"o": candle[1].(string),  // Open
		"h": candle[2].(string),  // High
		"l": candle[3].(string),  // Low
		"c": candle[4].(string),  // Close
		"v": candle[5].(string),  // Volume
		"q": candle[7].(string),  // Quote volume
		"n": trades,              // Trades
		"V": candle[9].(string),  // Taker buy base volume
		"Q": candle[10].(string), // Taker buy quote volume
		"x": true,                // Is closed (всегда true для REST)

		// Full-name aliases match the live CandlePublisher contract and Python consumers.
		"openTime":            openTime,
		"closeTime":           closeTime,
		"open":                candle[1].(string),
		"high":                candle[2].(string),
		"low":                 candle[3].(string),
		"close":               candle[4].(string),
		"volume":              candle[5].(string),
		"quoteVolume":         candle[7].(string),
		"numberOfTrades":      trades,
		"trades":              trades,
		"takerBuyVolume":      candle[9].(string),
		"takerBuyQuote":       candle[10].(string),
		"takerBuyQuoteVolume": candle[10].(string),
	}

	// Маппинг наших таймфреймов на формат для Redis
	tfMapping := map[string]string{
		"3M":       "3M",
		"kline_3M": "3M",
		"1y":       "1y",
		"kline_1y": "1y",
	}

	timeframeStr := tfMapping[rcf.timeframe]
	if timeframeStr == "" {
		timeframeStr = rcf.timeframe
	}

	// Преобразуем candleData в JSON для payload
	candleDataJSON, err := json.Marshal(candleData)
	if err != nil {
		return fmt.Errorf("marshal candle data: %w", err)
	}

	// Формируем поля для Redis Stream согласно протоколу candles:data
	fields := map[string]interface{}{
		"symbol":  symbol,
		"tf":      timeframeStr,
		"ts":      fmt.Sprintf("%d", closeTime), // closeTime в миллисекундах
		"payload": string(candleDataJSON),
	}

	// Публикуем в оба Redis (dual-write как в connections.Manager)
	streamName := streams.CandleDataStream

	// Публикуем в первый Redis
	if rcf.redisClient != nil {
		if _, err := redisclient.XAddWithRetry(rcf.ctx, rcf.redisClient, &redis.XAddArgs{
			Stream: streamName,
			Values: fields,
			MaxLen: streams.MaxLenCandles(),
			Approx: true,
		}); err != nil {
			zap.S().Errorf("⚠️ Ошибка публикации в Redis-1: %v", err)
		}
	}

	// Публикуем во второй Redis (candles)
	if rcf.redisClientCandles != nil {
		if _, err := redisclient.XAddWithRetry(rcf.ctx, rcf.redisClientCandles, &redis.XAddArgs{
			Stream: streamName,
			Values: fields,
			MaxLen: streams.MaxLenCandles(),
			Approx: true,
		}); err != nil {
			return fmt.Errorf("publish to redis candles: %w", err)
		}
	}

	atomic.AddUint64(&rcf.totalPublished, 1)

	// Логируем успешную публикацию (редко)
	if atomic.LoadUint64(&rcf.totalPublished)%100 == 0 {
		zap.S().Infof("✅ Опубликована %s свеча для %s (closeTime: %s, всего: %d)",
			timeframeStr, symbol, timeutil.TimestampToISO(closeTime), atomic.LoadUint64(&rcf.totalPublished))
	}

	return nil
}

// GetStats возвращает статистику работы fetcher
func (rcf *RestCandleFetcher) GetStats() map[string]interface{} {
	rcf.circuitMutex.RLock()
	circuitOpen := rcf.circuitOpen
	rcf.circuitMutex.RUnlock()

	return map[string]interface{}{
		"timeframe":         rcf.timeframe,
		"symbols_count":     len(rcf.symbols),
		"poll_interval":     rcf.pollInterval.String(),
		"total_fetches":     atomic.LoadUint64(&rcf.totalFetches),
		"total_published":   atomic.LoadUint64(&rcf.totalPublished),
		"total_errors":      atomic.LoadUint64(&rcf.totalErrors),
		"circuit_open":      circuitOpen,
		"consecutive_fails": atomic.LoadInt64(&rcf.consecutiveFailures),
	}
}
