package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go-worker/infra/redisclient"
	"go-worker/internal/streams"
	"go-worker/pkg/timeutil"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

// Счетчики для уменьшения логов
var symbolsSufficientCounter uint64
var symbolsCountCheckCounter uint64
var symbolsAddedCounter uint64
var symbolsAddedFromAPICounter uint64

// Константы для ограничений (значения по умолчанию)
const (
	DEFAULT_MIN_SYMBOLS_REQUIRED = 150  // Минимальное количество символов, которое должно быть в Redis
	DEFAULT_MAX_PAIRS_TO_FETCH   = 150  // Максимальное количество пар для выбора из Binance API
	DEFAULT_MAX_PAIRS_TO_ADD     = 300  // Максимальное количество пар для добавления в Redis
	DEFAULT_MIN_TRADES_24H       = 1000 // Минимальное количество сделок за 24 часа для активности
)

// Функции для получения значений из переменных окружения с fallback на константы
func getMinSymbolsRequired() int {
	if val := os.Getenv("BINANCE_MIN_SYMBOLS_REQUIRED"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_MIN_SYMBOLS_REQUIRED
}

func getMaxPairsToFetch() int {
	if val := os.Getenv("BINANCE_MAX_PAIRS_TO_FETCH"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_MAX_PAIRS_TO_FETCH
}

func getMaxPairsToAdd() int {
	if val := os.Getenv("BINANCE_MAX_PAIRS_TO_ADD"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_MAX_PAIRS_TO_ADD
}

func getMinTrades24H() int {
	if val := os.Getenv("BINANCE_MIN_TRADES_24H"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_MIN_TRADES_24H
}

// Временные интервалы для свечей
const (
	TimeframeM1  = "1m"  // 1 минута
	TimeframeM5  = "5m"  // 5 минут
	TimeframeM15 = "15m" // 15 минут
	TimeframeH1  = "1h"  // 1 час
	TimeframeH4  = "4h"  // 4 часа
	TimeframeD1  = "1d"  // 1 день
	TimeframeW1  = "1w"  // 1 неделя
	TimeframeMN1 = "1M"  // 1 месяц
	TimeframeQ1  = "3M"  // 1 квартал (3 месяца)
	TimeframeY1  = "1y"  // 1 год
)

// SymbolSupplementer дополняет символы через Binance API
type SymbolSupplementer struct {
	client       *redis.Client
	clientWorker *redis.Client // Клиент для redis-worker-1 (6380)
	ctx          context.Context
}

// NewSymbolSupplementer создает новый экземпляр дополнения символов
func NewSymbolSupplementer(client *redis.Client, ctx context.Context) *SymbolSupplementer {
	// Создаем дополнительный клиент для redis-worker-1 (6380)
	clientWorker := redis.NewClient(&redis.Options{
		Addr: "redis-worker-1:6379", // внутри контейнера порт 6379, снаружи 6380
		DB:   0,
		// Тюнинг для высокой пропускной способности (HFT)
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	return &SymbolSupplementer{
		client:       client,
		clientWorker: clientWorker,
		ctx:          ctx,
	}
}

// SupplementSymbolsFromBinanceAPI дополняет символы через Binance API если их меньше MIN_SYMBOLS_REQUIRED
func (ss *SymbolSupplementer) SupplementSymbolsFromBinanceAPI() error {
	// Сначала проверяем текущее количество символов
	currentCount, err := ss.getSymbolsCount()
	if err != nil {
		return fmt.Errorf("ошибка подсчета символов: %v", err)
	}

	// Логируем только каждое 10000-е сообщение
	count := atomic.AddUint64(&symbolsCountCheckCounter, 1)
	if count%10000 == 0 {
		zap.S().Infof("📊 Текущее количество символов в Redis: %d (проверок: %d)", currentCount, count)
	}

	// Если символов уже достаточно, не дополняем
	minSymbolsRequired := getMinSymbolsRequired()
	if currentCount >= minSymbolsRequired {
		count := atomic.AddUint64(&symbolsSufficientCounter, 1)
		// Логируем только каждое 10000-е сообщение
		if count%10000 == 0 {
			zap.S().Infof("✅ Символов достаточно (%d), дополнение не требуется (проверок: %d)", currentCount, count)
		}
		return nil
	}

	// Проверяем флаг глобального отключения
	enableTopSymbols := os.Getenv("ENABLE_BINANCE_TOP_SYMBOLS")
	if enableTopSymbols == "false" || enableTopSymbols == "0" {
		count := atomic.AddUint64(&symbolsSufficientCounter, 1)
		if count%10000 == 0 {
			zap.S().Infof("🛑 Дополнение символов отключено (ENABLE_BINANCE_TOP_SYMBOLS=false) (проверок: %d)", count)
		}
		return nil
	}

	zap.S().Warnf("⚠️ Символов меньше %d (%d), дополняем через Binance API", minSymbolsRequired, currentCount)

	// Запрашиваем активные пары из Binance API
	activePairs, err := ss.fetchActivePairsFromBinance()
	if err != nil {
		return fmt.Errorf("ошибка получения активных пар: %v", err)
	}

	// Добавляем новые символы в Redis
	addedCount, err := ss.addNewSymbolsToRedis(activePairs)
	if err != nil {
		return fmt.Errorf("ошибка добавления символов: %v", err)
	}

	zap.S().Infof("📊 Добавлено %d новых символов из Binance API", addedCount)
	return nil
}

// getSymbolsCount возвращает количество символов в Redis
func (ss *SymbolSupplementer) getSymbolsCount() (int, error) {
	var cursor uint64
	var count int

	for {
		var keys []string
		var err error
		keys, cursor, err = ss.client.Scan(ss.ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			return 0, err
		}

		count += len(keys)

		if cursor == 0 {
			break
		}
	}

	return count, nil
}

// fetchActivePairsFromBinance получает активные торговые пары из Binance API 1
func (ss *SymbolSupplementer) fetchActivePairsFromBinance() ([]string, error) {
	zap.S().Infof("🌐 Запрос активных торговых пар из Binance API...")

	// Создаем HTTP клиент
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Запрос к Binance API
	req, err := http.NewRequestWithContext(ss.ctx, "GET", "https://fapi.binance.com/fapi/v1/ticker/24hr", nil)
	if err != nil {
		return nil, fmt.Errorf("создание запроса: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP запрос: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("неверный статус ответа: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("чтение тела ответа: %v", err)
	}

	// Парсим JSON
	var tickers []map[string]interface{}
	if err := json.Unmarshal(body, &tickers); err != nil {
		return nil, fmt.Errorf("парсинг JSON: %v", err)
	}

	// Фильтруем активные USDT пары
	var activePairs []string
	for _, ticker := range tickers {
		symbol, ok := ticker["symbol"].(string)
		if !ok {
			continue
		}

		symbol = strings.ToLower(symbol)

		// Только USDT пары
		if !strings.HasSuffix(symbol, "usdt") {
			continue
		}

		// Фильтруем по объему торгов (больше определенного порога)
		minTrades24H := getMinTrades24H()
		count, ok := ticker["count"].(float64)
		if !ok || count < float64(minTrades24H) {
			continue
		}

		activePairs = append(activePairs, symbol)

		// Ограничиваем количество пар для начала
		maxPairsToFetch := getMaxPairsToFetch()
		if len(activePairs) >= maxPairsToFetch {
			break
		}
	}

	zap.S().Infof("📊 Получено %d активных пар из Binance API", len(activePairs))
	return activePairs, nil
}

// addNewSymbolsToRedis добавляет новые символы в Redis
func (ss *SymbolSupplementer) addNewSymbolsToRedis(activePairs []string) (int, error) {
	addedCount := 0

	for _, symbol := range activePairs {
		// Проверяем, есть ли уже такой символ в Redis
		key := fmt.Sprintf("symbol:details:%s", symbol)
		exists, err := ss.client.Exists(ss.ctx, key).Result()
		if err != nil {
			zap.S().Errorf("⚠️ Ошибка проверки существования ключа %s: %v", key, err)
			continue
		}

		if exists == 0 {
			// Добавляем новый символ в Redis с полным набором таймфреймов
			currentTimeMs := timeutil.GetCurrentTimestampMs()
			symbolData := map[string]interface{}{
				"symbol":          strings.ToUpper(symbol),
				"baseAsset":       strings.TrimSuffix(strings.ToUpper(symbol), "USDT"),
				"quoteAsset":      "USDT",
				"instrumentType":  "FUTURES",
				"exchange":        "binance",
				"status":          "TRADING",
				"timeframes":      "[\"M1\",\"M5\",\"M15\",\"H1\",\"H4\",\"D1\",\"W1\",\"MN1\",\"Q1\",\"Y1\"]", // Полный набор из 10 таймфреймов
				"timeframesCount": "10",
				"source":          "binance_api",
				"createdAt":       timeutil.FormatTimestampForRedis(currentTimeMs), // Unix timestamp ms (UTC)
				"updatedAt":       timeutil.FormatTimestampForRedis(currentTimeMs), // Unix timestamp ms (UTC)
				"note":            "",
			}

			// Добавляем в основной Redis (6379)
			if err := ss.client.HSet(ss.ctx, key, symbolData).Err(); err != nil {
				zap.S().Errorf("❌ Ошибка добавления символа %s в Redis: %v", symbol, err)
				continue
			}

			// Также добавляем в redis-worker-1 (6380) для бэкенда
			if err := ss.clientWorker.HSet(ss.ctx, key, symbolData).Err(); err != nil {
				zap.S().Errorf("⚠️ Ошибка добавления символа %s в redis-worker-1: %v", symbol, err)
			}

			// Публикуем уведомление в stream:symbols на redis-worker-1
			streamData := map[string]interface{}{
				"symbol":    strings.ToUpper(symbol),
				"action":    "added",
				"timestamp": timeutil.GetCurrentTimestampMs(),
				"data":      fmt.Sprintf("{\"symbol\":\"%s\",\"timeframes\":[\"M1\",\"M5\",\"M15\",\"H1\",\"H4\",\"D1\",\"W1\",\"MN1\",\"Q1\",\"Y1\"]}", strings.ToUpper(symbol)),
			}

			if _, err := redisclient.XAddWithRetry(ss.ctx, ss.clientWorker, &redis.XAddArgs{
				Stream: streams.Symbols,
				MaxLen: streams.MaxLenPerSymbol,
				Approx: true,
				ID:     "*",
				Values: streamData,
			}); err != nil {
				zap.S().Errorf("⚠️ Ошибка публикации в stream:symbols: %v", err)
			}

			// Логируем только каждое 10000-е подобное сообщение
			count := atomic.AddUint64(&symbolsAddedFromAPICounter, 1)
			if count%10000 == 0 {
				zap.S().Infof("✅ Добавлен новый символ %s из Binance API с полным набором таймфреймов (на 6379 и 6380) (добавлений: %d)", symbol, count)
			}
			addedCount++

			// Ограничиваем количество добавляемых символов
			maxPairsToAdd := getMaxPairsToAdd()
			if addedCount >= maxPairsToAdd {
				break
			}
		}
	}

	return addedCount, nil
}

// EnsureSymbolDetails гарантирует наличие symbol:details для указанного символа в обоих Redis.
func (ss *SymbolSupplementer) EnsureSymbolDetails(symbol string, timeframes []Timeframe) error {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return fmt.Errorf("пустой символ")
	}

	symbolUpper := strings.ToUpper(symbol)
	symbolLower := strings.ToLower(symbolUpper)
	key := fmt.Sprintf("symbol:details:%s", symbolLower)

	exists, err := ss.client.Exists(ss.ctx, key).Result()
	if err != nil {
		return fmt.Errorf("проверка существования ключа %s: %w", key, err)
	}

	if exists > 0 {
		// Cинхронизируем с redis-worker-1, чтобы гарантировать наличие данных
		if err := ss.syncKeyToWorker(key); err != nil {
			zap.S().Errorf("⚠️ Не удалось синхронизировать существующий символ %s с worker-redis: %v", symbolUpper, err)
		}
		return nil
	}

	if len(timeframes) == 0 {
		timeframes = GetAllTimeframes()
	}

	timeframesJSON := SerializeTimeframesForRedis(timeframes)
	timeframesCount := fmt.Sprintf("%d", len(timeframes))

	baseAsset := strings.TrimSuffix(strings.ToUpper(symbolUpper), "USDT")
	quoteAsset := "USDT"

	now := timeutil.FormatTimestampForRedis(timeutil.GetCurrentTimestampMs())
	symbolData := map[string]interface{}{
		"symbol":          symbolUpper,
		"baseAsset":       baseAsset,
		"quoteAsset":      quoteAsset,
		"instrumentType":  "FUTURES",
		"exchange":        "binance",
		"status":          "TRADING",
		"timeframes":      timeframesJSON,
		"timeframesCount": timeframesCount,
		"source":          "symbol_consumer_autofix",
		"createdAt":       now,
		"updatedAt":       now,
		"note":            "",
	}

	if err := ss.client.HSet(ss.ctx, key, symbolData).Err(); err != nil {
		return fmt.Errorf("добавление символа %s в основной Redis: %w", symbolUpper, err)
	}

	if err := ss.clientWorker.HSet(ss.ctx, key, symbolData).Err(); err != nil {
		zap.S().Errorf("⚠️ Ошибка добавления символа %s в redis-worker-1: %v", symbolUpper, err)
	}

	// Публикуем уведомление в stream:symbols на redis-worker-1 для быстрой подписки через SymbolConsumer
	timeframesShort := TimeframesToShortNames(timeframes)
	timeframesJSONArray, _ := json.Marshal(timeframesShort)
	streamData := map[string]interface{}{
		"symbol":     symbolUpper,
		"timeframes": string(timeframesJSONArray),
		"action":     "added",
		"timestamp":  timeutil.GetCurrentTimestampMs(),
		"source":     "ensure_default_crypto",
	}

	if _, err := redisclient.XAddWithRetry(ss.ctx, ss.clientWorker, &redis.XAddArgs{
		Stream: streams.Symbols,
		MaxLen: streams.MaxLenPerSymbol,
		Approx: true,
		ID:     "*",
		Values: streamData,
	}); err != nil {
		zap.S().Errorf("⚠️ Ошибка публикации %s в stream:symbols: %v", symbolUpper, err)
	}

	// Логируем только каждое 10000-е подобное сообщение
	count := atomic.AddUint64(&symbolsAddedCounter, 1)
	if count%10000 == 0 {
		zap.S().Infof("✅ Символ %s добавлен автоматически с таймфреймами %s (добавлений: %d)", symbolUpper, timeframesJSON, count)
	}
	return nil
}

func (ss *SymbolSupplementer) syncKeyToWorker(key string) error {
	data, err := ss.client.HGetAll(ss.ctx, key).Result()
	if err != nil {
		return fmt.Errorf("чтение данных символа %s: %w", key, err)
	}

	if len(data) == 0 {
		return nil
	}

	if err := ss.clientWorker.HSet(ss.ctx, key, data).Err(); err != nil {
		return fmt.Errorf("сохранение символа %s в redis-worker-1: %w", key, err)
	}

	return nil
}

// UpdateExistingSymbolsTimeframes обновляет существующие символы до полного набора таймфреймов
func (ss *SymbolSupplementer) UpdateExistingSymbolsTimeframes() (int, error) {
	updatedCount := 0

	// Получаем все существующие символы используя SCAN вместо KEYS
	var cursor uint64
	var allKeys []string

	for {
		var keys []string
		var err error
		keys, cursor, err = ss.client.Scan(ss.ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			return 0, fmt.Errorf("ошибка сканирования ключей символов: %v", err)
		}

		allKeys = append(allKeys, keys...)

		if cursor == 0 {
			break
		}
	}

	zap.S().Infof("🔍 Найдено %d символов для проверки", len(allKeys))

	for _, key := range allKeys {
		// Получаем текущие данные символа
		symbolData, err := ss.client.HGetAll(ss.ctx, key).Result()
		if err != nil {
			zap.S().Errorf("⚠️ Ошибка чтения данных символа %s: %v", key, err)
			continue
		}

		// Проверяем, нужно ли обновление
		currentTimeframes, exists := symbolData["timeframes"]

		// Если поле timeframes отсутствует, добавляем его
		if !exists {
			zap.S().Warnf("⚠️ У символа %s отсутствует поле timeframes, добавляем автоматически", key)
		} else {
			// Если уже полный набор таймфреймов, пропускаем
			if strings.Contains(currentTimeframes, "M1") && strings.Contains(currentTimeframes, "M5") &&
				strings.Contains(currentTimeframes, "M15") && strings.Contains(currentTimeframes, "H1") &&
				strings.Contains(currentTimeframes, "H4") && strings.Contains(currentTimeframes, "D1") &&
				strings.Contains(currentTimeframes, "W1") && strings.Contains(currentTimeframes, "MN1") &&
				strings.Contains(currentTimeframes, "Q1") && strings.Contains(currentTimeframes, "Y1") {
				continue
			}
		}

		// Обновляем таймфреймы и добавляем недостающие поля
		updateData := map[string]interface{}{
			"timeframes":      "[\"M1\",\"M5\",\"M15\",\"H1\",\"H4\",\"D1\",\"W1\",\"MN1\",\"Q1\",\"Y1\"]",
			"timeframesCount": "10",
			"updatedAt":       timeutil.FormatTimestampForRedis(timeutil.GetCurrentTimestampMs()), // Unix timestamp ms (UTC)
		}

		// Добавляем недостающие поля, если их нет
		if _, exists := symbolData["instrumentType"]; !exists {
			updateData["instrumentType"] = "FUTURES"
		}
		if _, exists := symbolData["exchange"]; !exists {
			updateData["exchange"] = "binance"
		}
		if _, exists := symbolData["note"]; !exists {
			updateData["note"] = ""
		}
		if _, exists := symbolData["status"]; !exists {
			updateData["status"] = "TRADING"
		}
		if _, exists := symbolData["source"]; !exists {
			updateData["source"] = "autofix"
		}
		if _, exists := symbolData["createdAt"]; !exists {
			updateData["createdAt"] = timeutil.FormatTimestampForRedis(timeutil.GetCurrentTimestampMs())
		}

		// Обновляем символ в основном Redis
		if err := ss.client.HSet(ss.ctx, key, updateData).Err(); err != nil {
			zap.S().Errorf("❌ Ошибка обновления символа %s: %v", key, err)
			continue
		}

		// Синхронизируем с redis-worker-1
		if err := ss.syncKeyToWorker(key); err != nil {
			zap.S().Errorf("⚠️ Не удалось синхронизировать символ %s с worker-redis: %v", key, err)
		}

		zap.S().Infof("✅ Обновлен символ %s: добавлен полный набор таймфреймов", key)
		updatedCount++
	}

	zap.S().Infof("🎯 Обновлено %d символов до полного набора таймфреймов", updatedCount)
	return updatedCount, nil
}

// SyncSymbolsToWorkerRedis синхронизирует все существующие символы с redis-worker-1 (6380)
func (ss *SymbolSupplementer) SyncSymbolsToWorkerRedis() (int, error) {
	syncedCount := 0

	// Получаем все существующие символы из основного Redis
	var cursor uint64
	var allKeys []string

	for {
		var keys []string
		var err error
		keys, cursor, err = ss.client.Scan(ss.ctx, cursor, "symbol:details:*", 100).Result()
		if err != nil {
			return 0, fmt.Errorf("ошибка сканирования ключей: %v", err)
		}

		allKeys = append(allKeys, keys...)

		if cursor == 0 {
			break
		}
	}

	zap.S().Infof("🔄 Синхронизация %d символов с redis-worker-1 (6380)...", len(allKeys))

	for _, key := range allKeys {
		// Получаем данные символа из основного Redis
		symbolData, err := ss.client.HGetAll(ss.ctx, key).Result()
		if err != nil {
			zap.S().Errorf("⚠️ Ошибка чтения данных символа %s: %v", key, err)
			continue
		}

		if len(symbolData) == 0 {
			continue
		}

		// Копируем данные в redis-worker-1
		if err := ss.clientWorker.HSet(ss.ctx, key, symbolData).Err(); err != nil {
			zap.S().Errorf("⚠️ Ошибка синхронизации символа %s: %v", key, err)
			continue
		}

		syncedCount++

		// Логируем каждое 10000-е подобное сообщение для отслеживания прогресса
		if syncedCount%10000 == 0 {
			zap.S().Infof("📊 Синхронизировано %d/%d символов...", syncedCount, len(allKeys))
		}
	}

	zap.S().Infof("✅ Синхронизировано %d символов с redis-worker-1 (6380)", syncedCount)
	return syncedCount, nil
}

// CandleData представляет структуру свечи
type CandleData struct {
	OpenTime       int64   `json:"openTime"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         float64 `json:"volume"`
	CloseTime      int64   `json:"closeTime"`
	QuoteVolume    float64 `json:"quoteVolume"`
	NumberOfTrades int64   `json:"numberOfTrades"`
	TakerBuyVolume float64 `json:"takerBuyVolume"`
	TakerBuyQuote  float64 `json:"takerBuyQuote"`
}

// GetCandles получает свечи для символа и таймфрейма с Binance
func (ss *SymbolSupplementer) GetCandles(symbol, timeframe string, limit int) ([]CandleData, error) {
	if limit <= 0 {
		limit = 100 // По умолчанию 100 свечей
	}
	if limit > 1000 {
		limit = 1000 // Максимум 1000 свечей
	}

	// Формируем URL для запроса
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		strings.ToUpper(symbol), timeframe, limit)

	zap.S().Infof("🌐 Запрос свечей %s@%s с Binance API (лимит: %d)", symbol, timeframe, limit)

	// Создаем HTTP клиент
	httpClient := &http.Client{
		Timeout: 15 * time.Second,
	}

	// Запрос к Binance API
	req, err := http.NewRequestWithContext(ss.ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("создание запроса: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP запрос: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("неверный статус ответа: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("чтение тела ответа: %v", err)
	}

	// Парсим JSON ответ
	var rawCandles [][]interface{}
	if err := json.Unmarshal(body, &rawCandles); err != nil {
		return nil, fmt.Errorf("парсинг JSON: %v", err)
	}

	// Преобразуем в структуру CandleData
	var candles []CandleData
	for _, raw := range rawCandles {
		if len(raw) < 11 {
			continue
		}

		candle := CandleData{
			OpenTime:       int64(raw[0].(float64)),
			Open:           parseFloat(raw[1]),
			High:           parseFloat(raw[2]),
			Low:            parseFloat(raw[3]),
			Close:          parseFloat(raw[4]),
			Volume:         parseFloat(raw[5]),
			CloseTime:      int64(raw[6].(float64)),
			QuoteVolume:    parseFloat(raw[7]),
			NumberOfTrades: int64(raw[8].(float64)),
			TakerBuyVolume: parseFloat(raw[9]),
			TakerBuyQuote:  parseFloat(raw[10]),
		}

		candles = append(candles, candle)
	}

	zap.S().Infof("✅ Получено %d свечей для %s@%s", len(candles), symbol, timeframe)
	return candles, nil
}

// GetCandlesForTimeframes получает свечи для нескольких таймфреймов одновременно
func (ss *SymbolSupplementer) GetCandlesForTimeframes(symbol string, timeframes []string, limit int) (map[string][]CandleData, error) {
	result := make(map[string][]CandleData)

	for _, timeframe := range timeframes {
		candles, err := ss.GetCandles(symbol, timeframe, limit)
		if err != nil {
			zap.S().Errorf("⚠️ Ошибка получения свечей %s@%s: %v", symbol, timeframe, err)
			continue
		}
		result[timeframe] = candles
	}

	return result, nil
}

// GetCandlesForAllTimeframes получает свечи для всех поддерживаемых таймфреймов
func (ss *SymbolSupplementer) GetCandlesForAllTimeframes(symbol string, limit int) (map[string][]CandleData, error) {
	allTimeframes := []string{
		TimeframeM1, TimeframeM5, TimeframeM15, TimeframeH1, TimeframeH4,
		TimeframeD1, TimeframeW1, TimeframeMN1, TimeframeQ1, TimeframeY1,
	}

	return ss.GetCandlesForTimeframes(symbol, allTimeframes, limit)
}

// parseFloat безопасно парсит float64 из interface{}
func parseFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 0
}
