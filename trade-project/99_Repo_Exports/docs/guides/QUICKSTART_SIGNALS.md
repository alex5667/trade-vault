# 🚀 Quick Start: Сигналы в Redis

**Дата**: 2025-11-04  
**Статус**: ✅ РАБОТАЕТ

---

## 📊 Компоненты системы

### 1. **OrderFlow Handler** (multi-symbol-orderflow)

Обрабатывает 3 инструмента одновременно:

- ✅ **XAUUSD** - Gold (XAUUSDOrderFlowHandlerV2)
- ✅ **BTCUSD** - Bitcoin (CryptoOrderFlowHandler)
- ✅ **ETHUSD** - Ethereum (CryptoOrderFlowHandler)

**Контейнер**: `scanner_infra_multi-symbol-orderflow_1`  
**Статус**: Running (35+ минут uptime, 0 ошибок)

### 2. **AggregatedHub-V2**

Объединяет сигналы из нескольких источников с весовыми коэффициентами.

### 3. **TechnicalAnalysis**

Генерирует сигналы на основе технических индикаторов (RSI, MACD, EMA).

---

## 📡 Redis Streams

### Входящие данные:

```
stream:tick_XAUUSD    → Тики для XAUUSD
stream:tick_BTCUSD    → Тики для BTCUSD
stream:tick_ETHUSD    → Тики для ETHUSD
stream:book_XAUUSD    → Order Book для XAUUSD
```

### Исходящие сигналы:

```
signals:orderflow:XAUUSD   → OrderFlow сигналы
signals:ta:XAUUSD          → TechnicalAnalysis сигналы
notify:telegram            → Уведомления для Telegram
signals:audit:XAUUSD       → Полный контекст для ML
```

---

## 🔍 Примеры сигналов

### 1. OrderFlow Signal

```json
{
	"data": {
		"sid": "1730735722000:SHORT:265418",
		"symbol": "XAUUSD",
		"source": "OrderFlow",
		"side": "SHORT",
		"entry": 2654.18,
		"sl": 2655.38,
		"tp_levels": [2652.98, 2651.78, 2650.58],
		"lot": 0.15,
		"confidence": 85.5,
		"atr": 1.2,
		"reason": "Extreme sell delta spike detected",
		"ts": 1730735722000,
		"indicators": {
			"z_delta": -7.2,
			"obi": 0.38,
			"volume_imbalance": -450.5
		}
	}
}
```

### 2. AggregatedHub-V2 Signal

```json
{
	"sid": "1730735722000:LONG:265045",
	"symbol": "XAUUSD",
	"source": "AggregatedHub-V2",
	"side": "LONG",
	"entry": 2650.45,
	"sl": 2649.05,
	"tp_levels": [2651.85, 2653.25, 2654.65],
	"lot": 0.2,
	"confidence": 82.5,
	"atr": 1.4,
	"reason": "Pro detector: z=5.8 | Cluster score: 0.82",
	"ts": 1730735722000,
	"indicators": {
		"pro_z": 5.8,
		"cluster_score": 0.82,
		"weighted_conf": 82.5
	}
}
```

### 3. TechnicalAnalysis Signal

```json
{
	"data": {
		"sid": "1730735800000:LONG:265125",
		"symbol": "XAUUSD",
		"source": "TechnicalAnalysis",
		"side": "LONG",
		"entry": 2651.25,
		"sl": 2649.5,
		"tp_levels": [2653.0, 2654.75, 2656.5],
		"lot": 0.18,
		"confidence": 78.0,
		"atr": 1.75,
		"reason": "RSI oversold reversal + MACD bullish cross",
		"ts": 1730735800000,
		"indicators": {
			"rsi": 28.5,
			"macd": 0.45,
			"ema_50": 2648.8,
			"trend": "bullish"
		}
	}
}
```

---

## 💻 Команды для работы

### Проверка статуса:

```bash
# Статус сервисов
docker-compose ps

# Логи multi-symbol-orderflow
docker-compose logs -f multi-symbol-orderflow

# Логи всех сервисов
docker-compose logs -f
```

### Проверка Redis:

```bash
# Проверка streams
docker exec scanner-redis redis-cli KEYS "signals:*"

# Количество сигналов в stream
docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD

# Последний сигнал
docker exec scanner-redis redis-cli XREVRANGE signals:orderflow:XAUUSD + - COUNT 1

# Проверка подключения из контейнера
docker exec scanner_infra_multi-symbol-orderflow_1 python3 -c \
  "import redis; r = redis.from_url('redis://scanner-redis:6379/0'); print('PING:', r.ping())"
```

### Управление:

```bash
# Перезапуск
docker-compose restart multi-symbol-orderflow

# Остановка
docker-compose stop multi-symbol-orderflow

# Запуск
docker-compose up -d multi-symbol-orderflow

# Пересборка (после изменений кода)
docker-compose build multi-symbol-orderflow
docker-compose up -d multi-symbol-orderflow
```

---

## 🔧 Конфигурация

### Environment переменные (docker-compose.yml):

```yaml
# Основные
REDIS_URL=redis://scanner-redis:6379/0
SYMBOLS=XAUUSD,BTCUSD,ETHUSD

# XAUUSD настройки
XAU_TICK_STREAM=stream:tick_XAUUSD
XAU_DELTA_Z_THRESHOLD=3.0
XAU_OBI_THRESHOLD=0.5
XAU_MIN_SIGNAL_INTERVAL=60

# BTCUSD настройки
BTCUSD_TICK_STREAM=stream:tick_BTCUSD
BTC_DELTA_Z_THRESHOLD=2.5
BTC_MIN_SIGNAL_INTERVAL=30

# ETHUSD настройки
ETHUSD_TICK_STREAM=stream:tick_ETHUSD
```

---

## 📚 Документация

- `REDIS_CONNECTION_FIX.md` - исправление Error 22
- `SUMMARY_REDIS_FIX_2025-11-04.md` - полная сводка исправлений
- `QUICKSTART_SIGNALS.md` - этот файл (quick start)

---

## ✅ Текущий статус

```
✅ scanner-go-gateway          | Up (healthy)
✅ scanner-redis                | Up
✅ scanner-redis-worker-1       | Up
✅ scanner-redis-worker-2       | Up
✅ multi-symbol-orderflow       | Up (35+ min, 0 errors)
   ├─ XAUUSD Handler           | RUNNING (0 restarts)
   ├─ BTCUSD Handler           | RUNNING (0 restarts)
   └─ ETHUSD Handler           | RUNNING (0 restarts)
```

---

## 🎯 Быстрая диагностика

### Проблема: Нет сигналов

**Проверьте**:

1. Есть ли входящие тики? `docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD`
2. Работает ли handler? `docker-compose logs multi-symbol-orderflow | grep RUNNING`
3. Есть ли ошибки? `docker-compose logs multi-symbol-orderflow | grep Error`

### Проблема: Error 22 подключения к Redis

**Решение**: См. `REDIS_CONNECTION_FIX.md`

### Проблема: Handler не запускается

```bash
# Проверьте логи запуска
docker-compose logs multi-symbol-orderflow | head -50

# Пересоздайте контейнер
docker-compose stop multi-symbol-orderflow
docker-compose rm -f multi-symbol-orderflow
docker-compose up -d multi-symbol-orderflow
```

---

**Готово к работе!** 🚀
