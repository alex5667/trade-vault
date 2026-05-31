// Приложение go-worker: сбор рыночных данных, WS‑подключения и обработка Redis Streams.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"go-worker/binance"
	"go-worker/infra/redisclient"
	"go-worker/internal/app"
	internalbinance "go-worker/internal/binance"
	internalbybit "go-worker/internal/bybit"
	"go-worker/internal/connections"
	"go-worker/internal/crossasset"
	internalhyperliquid "go-worker/internal/hyperliquid"
	"go-worker/internal/liquidation"
	"go-worker/internal/logging"
	"go-worker/internal/marketdata"
	"go-worker/internal/metrics"
	"go-worker/internal/monitoring"
	"go-worker/internal/orderflow"
	"go-worker/internal/redis"
	"go-worker/internal/scheduler"
	"go-worker/internal/stream"
	goRedis "go-worker/redis"

	extRedis "github.com/redis/go-redis/v9"

	"go.uber.org/zap"
)

var validSymbolRegex = regexp.MustCompile(`^[A-Z0-9_\-]+$`)

// updateExistingSymbols обновляет существующие символы до полного набора таймфреймов.
func updateExistingSymbols(ctx context.Context, client *extRedis.Client) {
	zap.S().Info("🔧 Обновление существующих символов до полного набора таймфреймов...")

	supplementer := binance.NewSymbolSupplementer(client, ctx)
	updatedCount, err := supplementer.UpdateExistingSymbolsTimeframes()
	if err != nil {
		zap.S().Errorf("⚠️ Ошибка обновления символов: %v", err)
		return
	}

	zap.S().Infof("✅ Обновлено %d символов до полного набора таймфреймов", updatedCount)
}

// syncSymbolsToBackend синхронизирует символы с redis-worker-1 для бэкенда.
func syncSymbolsToBackend(ctx context.Context, client *extRedis.Client) {
	zap.S().Info("🔄 Синхронизация символов с redis-worker-1 (6380) для бэкенда...")

	supplementer := binance.NewSymbolSupplementer(client, ctx)
	syncedCount, err := supplementer.SyncSymbolsToWorkerRedis()
	if err != nil {
		zap.S().Errorf("⚠️ Ошибка синхронизации символов: %v", err)
		return
	}

	zap.S().Infof("✅ Синхронизировано %d символов с redis-worker-1 (6380)", syncedCount)
}

// ensureDefaultCryptoSymbols гарантирует наличие основных символов в symbol:details
// с таймфреймами kline_1m, kline_5m, kline_15m для автоматической подписки на свечи
func ensureDefaultCryptoSymbols(ctx context.Context, client *extRedis.Client) {
	zap.S().Info("🔧 Проверка наличия основных символов в symbol:details...")

	supplementer := binance.NewSymbolSupplementer(client, ctx)

	// Таймфреймы для криптовалют: 1m, 5m, 15m (основные для ATR расчета)
	defaultTimeframes := []binance.Timeframe{
		binance.M1,  // kline_1m
		binance.M5,  // kline_5m
		binance.M15, // kline_15m
	}

	// Список обязательных символов — исключительно из ENV.
	// Hardcoded fallback удалён: отсутствие REQUIRED_SYMBOLS — ошибка конфигурации.
	var requiredSymbols []string

	envSymbols := os.Getenv("REQUIRED_SYMBOLS")
	if strings.TrimSpace(envSymbols) == "" {
		// P1: молчаливый fallback на 14 символов удалён.
		// Если ENABLE_DATA_COLLECTION=true, REQUIRED_SYMBOLS обязателен.
		zap.S().Fatal("❌ REQUIRED_SYMBOLS не задан или пуст. " +
			"Установите REQUIRED_SYMBOLS в docker-compose или .env (через CRYPTO_SYMBOLS). " +
			"Hardcoded fallback отключён для предотвращения работы в ограниченном режиме.")
	}

	parts := strings.Split(envSymbols, ",")
	var invalidSymbols []string
	for _, part := range parts {
		s := strings.ToUpper(strings.TrimSpace(part))
		if s != "" {
			if validSymbolRegex.MatchString(s) {
				requiredSymbols = append(requiredSymbols, s)
			} else {
				invalidSymbols = append(invalidSymbols, s)
			}
		}
	}

	if len(invalidSymbols) > 0 {
		zap.S().Fatalf("❌ Обнаружены невалидные символы в REQUIRED_SYMBOLS: %v. "+
			"Разрешены только [A-Z0-9_\\-].", invalidSymbols)
	}

	if len(requiredSymbols) == 0 {
		zap.S().Fatal("❌ REQUIRED_SYMBOLS задан, но не содержит валидных символов после парсинга. " +
			"Проверьте формат: BTCUSDT,ETHUSDT,...")
	}
	zap.S().Infof("✅ REQUIRED_SYMBOLS из ENV (%d символов): %v", len(requiredSymbols), requiredSymbols)

	for _, sym := range requiredSymbols {
		if err := supplementer.EnsureSymbolDetails(sym, defaultTimeframes); err != nil {
			zap.S().Errorf("⚠️ Ошибка обеспечения %s: %v", sym, err)
		} else {
			// Логируем только раз, в EnsureSymbolDetails есть свое логирование, но оно редкое
			// zap.S().Infof("✅ %s гарантирован в symbol:details", sym)
		}
	}
	zap.S().Infof("✅ Проверка основных символов завершена")
}

func parseFuturesSymbols(raw string, isEnabled bool) []string {
	if strings.TrimSpace(raw) == "" {
		if !isEnabled {
			zap.S().Warn("⚠️ FUTURES_SYMBOLS пуст, но FUTURES_WS_ENABLED=false. Пропускаем.")
			return nil
		}
		// P1: hardcoded fallback удалён — FUTURES_SYMBOLS обязателен.
		zap.S().Fatal("❌ FUTURES_SYMBOLS не задан или пуст. " +
			"Установите FUTURES_SYMBOLS в docker-compose или .env. " +
			"Hardcoded fallback [BTCUSDT,ETHUSDT] отключён.")
	}

	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	var invalidSymbols []string
	for _, part := range parts {
		s := strings.ToUpper(strings.TrimSpace(part))
		if s != "" {
			if validSymbolRegex.MatchString(s) {
				result = append(result, s)
			} else {
				invalidSymbols = append(invalidSymbols, s)
			}
		}
	}

	if len(invalidSymbols) > 0 {
		zap.S().Fatalf("❌ Обнаружены невалидные символы в FUTURES_SYMBOLS: %v. "+
			"Разрешены только [A-Z0-9_\\-].", invalidSymbols)
	}
	if len(result) == 0 {
		if !isEnabled {
			return nil
		}
		zap.S().Fatal("❌ FUTURES_SYMBOLS задан, но не содержит валидных символов после парсинга.")
	}
	return result
}

func parseFuturesEnabled(raw string) bool {
	if raw == "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func parseSkipSymbolsMap(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	m := make(map[string]bool, len(parts))
	var invalidSymbols []string
	for _, part := range parts {
		s := strings.ToUpper(strings.TrimSpace(part))
		if s != "" {
			if validSymbolRegex.MatchString(s) {
				m[s] = true
			} else {
				invalidSymbols = append(invalidSymbols, s)
			}
		}
	}

	if len(invalidSymbols) > 0 {
		zap.S().Fatalf("❌ Обнаружены невалидные символы в SKIP_SYMBOLS: %v. "+
			"Разрешены только [A-Z0-9_\\-].", invalidSymbols)
	}
	return m
}

func parseBybitFuturesEnabled(raw string) bool {
	// Bybit futures ingestion is opt-in by default to avoid mixing venues unexpectedly.
	if strings.TrimSpace(raw) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func parseBybitFuturesSymbols(raw string, fallback []string) []string {
	// If BYBIT_FUTURES_SYMBOLS is empty, reuse FUTURES_SYMBOLS fallback.
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	var invalidSymbols []string
	for _, part := range parts {
		s := strings.ToUpper(strings.TrimSpace(part))
		if s != "" {
			if validSymbolRegex.MatchString(s) {
				result = append(result, s)
			} else {
				invalidSymbols = append(invalidSymbols, s)
			}
		}
	}

	if len(invalidSymbols) > 0 {
		zap.S().Fatalf("❌ Обнаружены невалидные символы в BYBIT_FUTURES_SYMBOLS: %v. "+
			"Разрешены только [A-Z0-9_\\-].", invalidSymbols)
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}

// parseOptInEnabled — opt-in флаг: пустое значение = false.
// Использовать для новых источников данных, чтобы не включать их случайно.
func parseOptInEnabled(raw string) bool {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return false
	}
	switch raw {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func main() {
	// Инициализируем файловое логирование
	// Логи записываются в /app/logs (монтируется в ./logs на хосте)
	logsDir := os.Getenv("LOG_DIR")
	if logsDir == "" {
		// Пытаемся найти корень проекта (где есть logs/ директория)
		// В Docker контейнере это /app/logs
		if _, err := os.Stat("/app/logs"); err == nil {
			logsDir = "/app/logs"
		} else {
			// Fallback: используем logs/ относительно текущей директории
			logsDir = "logs"
		}
	}

	// Создаем абсолютный путь
	absLogsDir, err := filepath.Abs(logsDir)
	if err != nil {
		absLogsDir = logsDir
	}

	// Инициализируем файловый логгер
	if err := logging.SetupGlobalFileLogger(absLogsDir); err != nil {
		zap.S().Errorf("⚠️ Не удалось инициализировать файловый логгер: %v (продолжаем с консольным логированием)", err)
	} else {
		zap.S().Infof("📁 Логи будут записываться в: %s", absLogsDir)
	}

	// Закрываем файловый логгер при завершении
	defer func() {
		if fl := logging.GetGlobalFileLogger(); fl != nil {
			if err := fl.Close(); err != nil {
				zap.S().Errorf("⚠️ Ошибка закрытия файлового логгера: %v", err)
			}
		}
	}()

	app.PrintStartupMessage()
	app.StartPrometheusMetrics()
	redis.WaitForRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize health metrics (5-second window)
	healthMetrics := metrics.NewHealthMetrics(redisclient.Client, 5*time.Second)
	go healthMetrics.Run()

	// Check if this worker should perform global data collection/syncing
	enableDataCollection := os.Getenv("ENABLE_DATA_COLLECTION") == "true"

	if enableDataCollection {
		updateExistingSymbols(ctx, redisclient.Client)
		syncSymbolsToBackend(ctx, redisclient.Client)
		ensureDefaultCryptoSymbols(ctx, redisclient.Client) // Гарантируем наличие BTCUSDT и ETHUSDT для свечей
	}

	symbolConsumer := binance.NewSymbolConsumer()
	connManager := connections.NewManager(ctx, symbolConsumer) // Пробрасываем ctx (Priority 5)

	websocketMonitor := monitoring.NewWebSocketMonitor()
	connManager.SetMonitor(websocketMonitor)

	httpMonitor := monitoring.NewWebSocketMonitorHTTP(websocketMonitor)
	httpMonitor.RegisterHandlers()

	symbolConsumer.SetCandlePublisher(connManager)
	symbolConsumer.SetMonitor(websocketMonitor)

	if err := connManager.InitializeCandleDataStream(); err != nil {
		zap.S().Errorf("❌ Ошибка инициализации потока данных свечей: %v", err)
	} else {
		zap.S().Infof("✅ Поток данных свечей инициализирован")
	}

	binance.SetGlobalCandlePublisher(connManager)
	connManager.InitializeInitialConnections()

	if enableDataCollection {
		app.StartDataCollection()
	}

	isFuturesEnabled := parseFuturesEnabled(os.Getenv("FUTURES_WS_ENABLED"))
	futuresSymbols := parseFuturesSymbols(os.Getenv("FUTURES_SYMBOLS"), isFuturesEnabled)

	bybitFuturesSymbols := parseBybitFuturesSymbols(os.Getenv("BYBIT_FUTURES_SYMBOLS"), futuresSymbols)
	isBybitFuturesEnabled := parseBybitFuturesEnabled(os.Getenv("BYBIT_FUTURES_WS_ENABLED"))

	// Hyperliquid ingestion (opt-in): trades + l2Book snapshots
	isHyperliquidEnabled := parseOptInEnabled(os.Getenv("HYPERLIQUID_WS_ENABLED"))
	hyperCoins := internalhyperliquid.LoadBaseCoins(futuresSymbols)
	hyperCoinsKey := strings.TrimSpace(os.Getenv("HYPERLIQUID_COINS_REDIS_KEY"))
	if hyperCoinsKey == "" {
		hyperCoinsKey = "hyperliquid:perps:coins"
	}

	// --- A1: Liquidation WS ingestion (forceOrder / allLiquidation) ---
	//
	// Поток ликвидаций включается отдельно от тик/книги, чтобы можно было безопасно
	// раскатывать feature без влияния на core pipeline.
	//
	// Включение:
	//   LIQ_WS_ENABLED=true
	//   LIQ_SYMBOLS=BTCUSDT,ETHUSDT (fallback = FUTURES_SYMBOLS)
	//
	// Рекомендация (A1):
	//   * Binance: !forceOrder@arr (all-market) + локальный allowlist
	//   * Bybit: allLiquidation.{symbol} по allowlist
	liqCfg := liquidation.LoadConfigFromEnv(futuresSymbols)
	liqLogger := zap.S()
	liqController := liquidation.NewController(redisclient.Client, liqCfg, liqLogger)
	liqController.Start(ctx)

	var futuresController *stream.Controller
	var bybitFuturesController *stream.Controller
	var hyperController *stream.Controller
	var batchPublisher *redis.BatchTickPublisher

	if isFuturesEnabled || isBybitFuturesEnabled || isHyperliquidEnabled {
		batchPublisher = redis.NewBatchTickPublisher(redisclient.ClientTicks, 2000, 10*time.Millisecond)
		batchPublisher.Start(ctx)
	}

	// Configure staleness thresholds (shared).
	stalenessConfig := orderflow.StalenessConfig{
		MaxAgeTickMs: 150,  // 150ms for signal gating (relative to tick)
		MaxAgeNowMs:  1000, // 1sec for SRE alerts (relative to now)
	}

	// -----------------------------------------------------------------------
	// v12_of: Cross-asset tracker + market-data pollers
	// All components are opt-in: CROSSASSET_ENABLED defaults to true.
	// -----------------------------------------------------------------------
	var caTracker *crossasset.Tracker
	var spotPoller *marketdata.SpotPoller
	var scPoller *marketdata.StableCoinPoller
	var v13CrossAssetPoller *marketdata.V13CrossAssetPoller

	if parseFuturesEnabled(os.Getenv("CROSSASSET_ENABLED")) {
		caTracker = crossasset.New(redisclient.Client)
		zap.S().Infof("✅ CrossAsset Tracker (v12_of) запущен")

		// --- Spot price poller (Binance REST, 10 s) -------------------------
		spotBaseURL := getEnvStr("BINANCE_SPOT_BASE_URL", "https://api.binance.com")
		spotIntervalMs := time.Duration(getEnvInt("SPOT_POLL_INTERVAL_SEC", 10)) * time.Second
		spotTTL := time.Duration(getEnvInt("SPOT_REDIS_TTL_SEC", 60)) * time.Second
		spotPoller = marketdata.NewSpotPoller(redisclient.Client, marketdata.SpotPollerConfig{
			Symbols:     futuresSymbols, // same allowlist as futures WS
			SkipSymbols: parseSkipSymbolsMap(os.Getenv("SPOT_POLLER_SKIP_SYMBOLS")),
			BaseURL:     spotBaseURL,
			Interval:    spotIntervalMs,
			RedisTTL:    spotTTL,
		})
		spotPoller.Start(ctx)
		zap.S().Infof("✅ SpotPoller запущен: %d символов, interval=%s", len(futuresSymbols), spotIntervalMs)

		// --- Stable-coin dominance poller (CoinGecko, 60 s) ----------------
		cgBaseURL := getEnvStr("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
		cgInterval := time.Duration(getEnvInt("SC_POLL_INTERVAL_SEC", 60)) * time.Second
		scPoller = marketdata.NewStableCoinPoller(caTracker, marketdata.StableCoinPollerConfig{
			BaseURL:  cgBaseURL,
			Interval: cgInterval,
			APIKey:   getEnvStr("COINGECKO_API_KEY", ""),
		})
		scPoller.Start(ctx)
		zap.S().Infof("✅ StableCoinPoller запущен: interval=%s", cgInterval)

		// --- v13_of cross-asset poller (Binance FAPI + CoinGecko) -----------
		v13FapiBase := getEnvStr("V13_FAPI_BASE_URL", "https://fapi.binance.com")
		v13PollSec := getEnvInt("V13_CROSSASSET_POLL_SEC", 15)
		v13GlobalSec := getEnvInt("V13_CROSSASSET_GLOBAL_POLL_SEC", 60)
		v13CrossAssetPoller = marketdata.NewV13CrossAssetPoller(redisclient.Client, marketdata.V13CrossAssetPollerConfig{
			Symbols:            futuresSymbols,
			FapiBaseURL:        v13FapiBase,
			CGBaseURL:          cgBaseURL,
			CGAPIKey:           getEnvStr("COINGECKO_API_KEY", ""),
			BinanceAPIKey:      getEnvStr("BINANCE_FAPI_KEY", ""),
			BinanceAPISecret:   getEnvStr("BINANCE_FAPI_SECRET", ""),
			PollInterval:       time.Duration(v13PollSec) * time.Second,
			GlobalPollInterval: time.Duration(v13GlobalSec) * time.Second,
		})
		v13CrossAssetPoller.Start(ctx)
		zap.S().Infof("✅ V13CrossAssetPoller запущен: %d символов, poll=%ds, global=%ds",
			len(futuresSymbols), v13PollSec, v13GlobalSec)
	}

	_ = scPoller // referenced in Stop block below

	if isFuturesEnabled {
		zap.S().Infof("✅ Binance Futures WS включён для символов: %v", futuresSymbols)

		futuresLogger := zap.S()

		// v12_of cross-asset tracker (shared, concurrency-safe).
		// Enabled by default; set CROSSASSET_ENABLED=false to disable.
		var caTracker *crossasset.Tracker
		if parseFuturesEnabled(os.Getenv("CROSSASSET_ENABLED")) {
			caTracker = crossasset.New(redisclient.Client)
			zap.S().Infof("✅ CrossAsset Tracker (v12_of) запущен")
		}

		futuresController = stream.NewController(
			"binance",
			redisclient.Client,
			batchPublisher,
			futuresLogger,
			"binance:futures:usdtm:symbols",
			30*time.Second,
			healthMetrics,
			stalenessConfig,
			func(symbols []string, logger *zap.SugaredLogger) stream.ExchangeManager {
				return internalbinance.NewFuturesMultiplexManager(symbols, logger)
			},
			&internalbinance.Normalizer{},
			nil,
		)
		if caTracker != nil {
			futuresController.WithCrossAssetHook(caTracker)
		}

		go futuresController.Run(ctx, futuresSymbols)
	} else {
		zap.S().Warnf("⚠️ Binance Futures WS отключён (FUTURES_WS_ENABLED=false)")
	}

	if isBybitFuturesEnabled {
		zap.S().Infof("✅ Bybit Futures WS включён для символов: %v", bybitFuturesSymbols)

		bybitLogger := zap.S()
		bookDepth := getEnvInt("BYBIT_BOOK_DEPTH", 50)
		pingPeriod := getEnvDuration("BYBIT_WS_PING_PERIOD", 20*time.Second)

		bybitFuturesController = stream.NewController(
			"bybit",
			redisclient.Client,
			batchPublisher,
			bybitLogger,
			"bybit:futures:linear:symbols",
			30*time.Second,
			healthMetrics,
			stalenessConfig,
			func(symbols []string, logger *zap.SugaredLogger) stream.ExchangeManager {
				return internalbybit.NewFuturesMultiplexManager(symbols, logger, bookDepth, pingPeriod)
			},
			internalbybit.NewNormalizer(bookDepth),
			func(symbols []string) []string {
				maxSymbols := getEnvInt("BYBIT_MAX_SYMBOLS_PER_CONN", 50)
				if maxSymbols <= 0 {
					maxSymbols = 50
				}
				if len(symbols) <= maxSymbols {
					return symbols
				}
				bybitLogger.Infof("⚠️ BYBIT_MAX_SYMBOLS_PER_CONN=%d: truncating symbols %d -> %d", maxSymbols, len(symbols), maxSymbols)
				return symbols[:maxSymbols]
			},
		)

		go bybitFuturesController.Run(ctx, bybitFuturesSymbols)
		if caTracker != nil {
			bybitFuturesController.WithCrossAssetHook(caTracker)
		}
	} else {
		zap.S().Warnf("⚠️ Bybit Futures WS отключён (BYBIT_FUTURES_WS_ENABLED=false)")
	}

	if isHyperliquidEnabled {
		zap.S().Infof("✅ Hyperliquid WS включён для coins: %v", hyperCoins)

		hlLogger := zap.S()
		hyperController = stream.NewController(
			"hyperliquid",
			redisclient.Client,
			batchPublisher,
			hlLogger,
			hyperCoinsKey,
			30*time.Second,
			healthMetrics,
			stalenessConfig,
			func(coins []string, logger *zap.SugaredLogger) stream.ExchangeManager {
				return internalhyperliquid.NewHyperliquidFuturesManager(coins, logger)
			},
			internalhyperliquid.NewNormalizer(),
			nil,
		)
		go hyperController.Run(ctx, hyperCoins)
		if caTracker != nil {
			hyperController.WithCrossAssetHook(caTracker)
		}
	} else {
		zap.S().Warnf("⚠️ Hyperliquid WS отключён (HYPERLIQUID_WS_ENABLED!=true)")
	}

	// Используем уникальные имена для общих стримов (volatility и т.д.),
	// чтобы каждый воркер (1m, 5m...) получал свою копию сообщения.
	hostname, _ := os.Hostname()
	tf := os.Getenv("BINANCE_WS_TIMEFRAME")
	if tf == "" {
		tf = "default"
	}
	consumerGroup := fmt.Sprintf("scanner-group-%s", tf)
	consumerName := fmt.Sprintf("go-worker-%s-%s", tf, hostname)

	consumer := goRedis.NewStreamConsumer(consumerGroup, consumerName)
	streamHandlers := map[string]goRedis.MessageHandler{
		"stream:volatility": func(streamName string, messageID string, fields map[string]interface{}) error {
			zap.S().Infof("📊 Получен сигнал волатильности: %s", messageID)
			return nil
		},
		"stream:volatilityRange": func(streamName string, messageID string, fields map[string]interface{}) error {
			zap.S().Infof("📈 Получен сигнал диапазона волатильности: %s", messageID)
			return nil
		},
		"stream:top-gainers": func(streamName string, messageID string, fields map[string]interface{}) error {
			zap.S().Infof("🚀 Получен топ растущих: %s", messageID)
			return nil
		},
		"stream:top-losers": func(streamName string, messageID string, fields map[string]interface{}) error {
			zap.S().Infof("📉 Получен топ падающих: %s", messageID)
			return nil
		},
		"stream:ws-new-pairs": func(streamName string, messageID string, fields map[string]interface{}) error {
			zap.S().Infof("🆕 Получена новая пара: %s", messageID)
			return nil
		},
	}

	go func() {
		if err := consumer.ConsumeFromMultipleStreams(streamHandlers); err != nil {
			zap.S().Errorf("Ошибка запуска потребителя стримов: %v", err)
		}
	}()

	healthChecker := scheduler.NewHealthChecker(connManager)
	healthChecker.StartPeriodicConnectionCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()

	// Stop health metrics
	healthMetrics.Stop()

	connManager.Stop()
	consumer.Stop()
	symbolConsumer.Stop()

	if futuresController != nil {
		futuresController.Stop()
	}
	if bybitFuturesController != nil {
		bybitFuturesController.Stop()
	}

	if hyperController != nil {
		hyperController.Stop()
	}

	// Stop market-data pollers (fail-open if nil)
	if spotPoller != nil {
		spotPoller.Stop()
	}
	if scPoller != nil {
		scPoller.Stop()
	}

	// Stop v13 cross-asset poller
	if v13CrossAssetPoller != nil {
		v13CrossAssetPoller.Stop()
	}

	drainTimeoutSec := getEnvInt("DRAIN_TIMEOUT_SEC", 10)
	drainTimeout := time.Duration(drainTimeoutSec) * time.Second

	// Stop liquidation ingestion (best-effort flush)
	if liqController != nil {
		liqController.Stop(drainTimeout)
	}

	if batchPublisher != nil {
		_ = batchPublisher.Close(drainTimeout)
	}

	app.PrintShutdownMessage()
}

func getEnvInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	var val int
	_, err := fmt.Sscanf(valStr, "%d", &val)
	if err != nil {
		return fallback
	}
	return val
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	d, err := time.ParseDuration(valStr)
	if err != nil {
		return fallback
	}
	return d
}

func getEnvStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
