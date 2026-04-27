# 🔧 Services Reference

## Обзор сервисов

Система состоит из 30+ микросервисов, каждый из которых выполняет специфическую задачу. Этот документ подробно описывает назначение, API и внутреннюю логику каждого сервиса.

---

## 📦 Data Ingestion Layer

### Go Worker (10 instances)

#### Назначение

Получение рыночных данных с Binance WebSocket API в реальном времени для всех поддерживаемых таймфреймов.

#### Технические детали

**Язык**: Go 1.22+  
**Dockerfile**: `go-worker/Dockerfile`  
**Entry point**: `go-worker/main.go`

#### Функциональность

1. **WebSocket Connection Management**

   - Подключение к Binance Stream API
   - Auto-reconnect при разрыве соединения
   - Heartbeat monitoring
   - Graceful shutdown

2. **Data Processing**

   - Парсинг kline (OHLCV) данных
   - Валидация данных
   - Timestamp normalization (UTC)

3. **Redis Publishing**

   - Dual publishing (redis-worker-1 + redis-worker-2)
   - Stream: `candles:data`
   - Format: JSON

4. **Monitoring**
   - Prometheus metrics на порту 2112-2121
   - Health checks
   - Connection status

#### Конфигурация

```bash
BINANCE_WS_TIMEFRAME=kline_1m     # Какой таймфрейм слушать
REDIS_HOST=redis-worker-1         # Primary Redis
REDIS_CANDLES_HOST_2=redis-worker-2  # Secondary Redis
PROMETHEUS_PORT=2112              # Уникальный порт для каждого worker
```

#### Metrics

```
binance_ws_messages_total         # Всего сообщений получено
binance_ws_errors_total           # Ошибки WebSocket
binance_ws_reconnects_total       # Количество переподключений
redis_publish_duration_seconds    # Latency публикации в Redis
```

#### Алгоритм работы

```go
func main() {
    // 1. Инициализация
    redisClient := initRedis()
    wsClient := initBinanceWS()

    // 2. Подключение к Binance
    stream := fmt.Sprintf("%s@%s", symbols, timeframe)
    conn, err := wsClient.Connect(stream)

    // 3. Основной цикл
    for {
        select {
        case msg := <-conn.Messages():
            // Парсинг kline
            kline := parseKline(msg)

            // Валидация
            if !validateKline(kline) {
                continue
            }

            // Публикация в Redis (dual)
            publishToRedis(redisClient, "candles:data", kline)
            publishToRedis(redisClient2, "candles:data", kline)

            // Метрики
            metricsCounter.Inc()

        case <-ctx.Done():
            // Graceful shutdown
            cleanup()
            return
        }
    }
}
```

#### Таймфреймы

| Worker | Timeframe | Container Name   | Prometheus Port |
| ------ | --------- | ---------------- | --------------- |
| 1      | 1m        | go-worker-1m     | 2112            |
| 2      | 5m        | go-worker-5m     | 2113            |
| 3      | 15m       | go-worker-15m    | 2114            |
| 4      | 1h        | go-worker-1h     | 2115            |
| 5      | 4h        | go-worker-4h     | 2116            |
| 6      | 1d        | go-worker-1d     | 2117            |
| 7      | 1w        | go-worker-1w     | 2118            |
| 8      | 1M        | go-worker-1month | 2119            |
| 9      | 3M        | go-worker-3month | 2120            |
| 10     | 1y        | go-worker-1y     | 2121            |

---

### Tick Ingest Server

#### Назначение

HTTP API для приема tick данных от MT5 Expert Advisor через Wine.

#### Технические детали

**Язык**: Python 3.11+  
**Framework**: FastAPI + Uvicorn  
**Port**: 8087  
**Dockerfile**: `python-worker/Dockerfile`

#### API Endpoints

##### POST /tick

Прием tick данных от MT5.

**Request**:

```json
{
	"symbol": "XAUUSD",
	"time": "2024-01-01T12:00:00.123Z",
	"bid": 2055.25,
	"ask": 2055.35,
	"last": 2055.3,
	"volume": 1.5,
	"flags": 6
}
```

**Response**:

```json
{
	"status": "ok",
	"stream_id": "1234567890-0"
}
```

##### POST /book

Прием Order Book snapshot от MT5.

**Request**:

```json
{
	"symbol": "XAUUSD",
	"time": "2024-01-01T12:00:00.123Z",
	"bids": [
		{ "price": 2055.2, "volume": 10.5 },
		{ "price": 2055.1, "volume": 5.2 }
	],
	"asks": [
		{ "price": 2055.3, "volume": 8.3 },
		{ "price": 2055.4, "volume": 12.1 }
	]
}
```

##### GET /health

Health check endpoint.

**Response**:

```json
{ "status": "healthy", "redis": "connected" }
```

#### Функциональность

1. **Tick Processing**

   - Валидация входных данных
   - Timestamp normalization
   - Публикация в Redis stream

2. **Order Book Processing**

   - Snapshot сохранение
   - Last state update
   - Stream publishing

3. **Stream Management**
   - Auto-trimming (опционально через MAXLEN)
   - Batch trimming через stream-trimmer

#### Алгоритм работы

```python
@app.post("/tick")
async def ingest_tick(tick: TickData):
    # 1. Валидация
    if tick.symbol not in ALLOW_SYMBOLS:
        raise HTTPException(400, "Symbol not allowed")

    # 2. Нормализация
    tick_record = {
        "symbol": tick.symbol,
        "time": tick.time.isoformat(),
        "bid": float(tick.bid),
        "ask": float(tick.ask),
        "last": float(tick.last),
        "volume": float(tick.volume)
    }

    # 3. Публикация в Redis
    stream_key = f"stream:tick_{tick.symbol}"
    stream_id = redis.xadd(
        stream_key,
        tick_record,
        maxlen=TICK_STREAM_MAXLEN if USE_MAXLEN else None
    )

    # 4. Метрики
    metrics.ticks_received.inc()

    return {"status": "ok", "stream_id": stream_id}
```

---

## 🧠 Processing Layer

### Multi-Symbol OrderFlow Handler

#### Назначение

Унифицированный обработчик для анализа Order Flow по множеству торговых инструментов (XAUUSD, BTCUSD, ETHUSD и др.).

#### Технические детали

**Язык**: Python 3.11+  
**Dockerfile**: `python-worker/Dockerfile`  
**Entry point**: `python-worker/main_multi_symbol.py` или `main_multi_symbol_dynamic.py`

#### Архитектура (>85% code reuse)

```
BaseOrderFlowHandler (abstract)
    │
    ├─→ XAUOrderFlowHandler (Gold)
    ├─→ BTCOrderFlowHandler (Bitcoin)
    └─→ ETHOrderFlowHandler (Ethereum)
```

**Общие компоненты**:

1. Delta Analyzer
2. OBI Detector
3. Iceberg Detector
4. Cluster Analyzer
5. Speed Monitor
6. Signal Publisher

**Symbol-specific**:

- Пороги (thresholds)
- Множители (multipliers)
- Веса (weights)

#### Функциональность

##### 1. Delta Analysis

Анализ разницы между покупками и продажами.

```python
class DeltaAnalyzer:
    def __init__(self, window_size=120):
        self.window_size = window_size
        self.deltas = deque(maxlen=1000)

    def calculate_delta(self, ticks):
        """Вычисление дельты объемов"""
        delta = sum(
            tick['volume'] if tick['is_buyer'] else -tick['volume']
            for tick in ticks
        )
        return delta

    def calculate_zscore(self, delta):
        """Z-score нормализация"""
        if len(self.deltas) < 30:
            return 0.0

        mean = np.mean(self.deltas)
        std = np.std(self.deltas)

        if std == 0:
            return 0.0

        zscore = (delta - mean) / std
        return zscore

    def detect_signal(self, zscore, threshold=3.0):
        """Определение сигнала по Z-score"""
        if zscore > threshold:
            return "LONG", abs(zscore)
        elif zscore < -threshold:
            return "SHORT", abs(zscore)
        return None, 0.0
```

##### 2. OBI Detection (Order Book Imbalance)

Анализ дисбаланса стакана ордеров.

```python
class OBIDetector:
    def calculate_obi(self, book_snapshot):
        """
        OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)
        Range: [-1, 1]
        > 0: Больше покупателей
        < 0: Больше продавцов
        """
        bid_volume = sum(level['volume'] for level in book_snapshot['bids'])
        ask_volume = sum(level['volume'] for level in book_snapshot['asks'])

        total = bid_volume + ask_volume
        if total == 0:
            return 0.0

        obi = (bid_volume - ask_volume) / total
        return obi

    def detect_imbalance(self, obi, threshold=0.5, duration=2.0):
        """
        Детектирование значительного дисбаланса
        """
        if abs(obi) > threshold:
            # Проверка длительности
            if self.check_sustained(obi, duration):
                side = "LONG" if obi > 0 else "SHORT"
                confidence = abs(obi)
                return side, confidence

        return None, 0.0
```

##### 3. Iceberg Order Detection

Обнаружение скрытых крупных ордеров.

```python
class IcebergDetector:
    def detect_iceberg(self, price_level, refresh_count, duration):
        """
        Признаки айсберг-ордера:
        1. Частое обновление на одном уровне цены
        2. Постоянный объем (пополнение после исполнения)
        3. Длительное присутствие
        """
        if refresh_count >= self.refresh_threshold:
            if duration >= self.duration_threshold:
                # Айсберг обнаружен
                return {
                    "price": price_level,
                    "refresh_count": refresh_count,
                    "duration": duration,
                    "type": "iceberg"
                }

        return None
```

##### 4. Cluster Analysis

Анализ кластеров объема на уровнях цен.

```python
class ClusterAnalyzer:
    def analyze_clusters(self, ticks, price_step=0.1):
        """
        Группировка тиков по price levels
        Определение "горячих" зон с высоким объемом
        """
        clusters = defaultdict(lambda: {
            'volume': 0.0,
            'buy_volume': 0.0,
            'sell_volume': 0.0,
            'count': 0
        })

        for tick in ticks:
            # Округление до price step
            price_level = round(tick['price'] / price_step) * price_step

            clusters[price_level]['volume'] += tick['volume']
            clusters[price_level]['count'] += 1

            if tick['is_buyer']:
                clusters[price_level]['buy_volume'] += tick['volume']
            else:
                clusters[price_level]['sell_volume'] += tick['volume']

        # Поиск значимых кластеров
        significant = self.find_significant_clusters(clusters)
        return significant

    def find_significant_clusters(self, clusters):
        """Кластеры с объемом выше среднего + 2 std"""
        volumes = [c['volume'] for c in clusters.values()]
        mean_vol = np.mean(volumes)
        std_vol = np.std(volumes)
        threshold = mean_vol + 2 * std_vol

        significant = [
            {'price': price, **data}
            for price, data in clusters.items()
            if data['volume'] > threshold
        ]

        return sorted(significant, key=lambda x: x['volume'], reverse=True)
```

##### 5. Speed Monitor

Мониторинг скорости движения цены.

```python
class SpeedMonitor:
    def calculate_speed(self, ticks, time_window=5.0):
        """
        Скорость = изменение цены / время
        """
        if len(ticks) < 2:
            return 0.0

        # Фильтрация по time window
        now = time.time()
        recent_ticks = [
            t for t in ticks
            if (now - t['timestamp']) <= time_window
        ]

        if len(recent_ticks) < 2:
            return 0.0

        # Расчет скорости
        price_change = recent_ticks[-1]['price'] - recent_ticks[0]['price']
        time_delta = recent_ticks[-1]['timestamp'] - recent_ticks[0]['timestamp']

        if time_delta == 0:
            return 0.0

        speed = price_change / time_delta
        return speed
```

#### Signal Generation Logic

Комбинирование всех анализаторов для генерации финального сигнала:

```python
class OrderFlowHandler:
    def generate_signal(self, context):
        """
        Генерация сигнала на основе всех компонентов
        """
        # 1. Delta analysis
        delta_side, delta_conf = self.delta_analyzer.detect_signal(
            context['delta_zscore'],
            threshold=self.config['delta_threshold']
        )

        # 2. OBI analysis
        obi_side, obi_conf = self.obi_detector.detect_imbalance(
            context['obi'],
            threshold=self.config['obi_threshold']
        )

        # 3. Speed analysis
        speed = self.speed_monitor.calculate_speed(context['ticks'])
        speed_conf = min(abs(speed) / self.config['max_speed'], 1.0)

        # 4. Cluster analysis
        clusters = self.cluster_analyzer.analyze_clusters(context['ticks'])
        cluster_conf = self.calculate_cluster_confidence(clusters)

        # 5. Проверка согласованности
        if not self.check_agreement([delta_side, obi_side]):
            return None  # Противоречивые сигналы

        # 6. Weighted confidence
        confidence = (
            self.weights['delta'] * delta_conf +
            self.weights['obi'] * obi_conf +
            self.weights['speed'] * speed_conf +
            self.weights['cluster'] * cluster_conf
        )

        # 7. Проверка порога
        if confidence < self.min_confidence:
            return None

        # 8. Cooldown check
        if not self.check_cooldown():
            return None

        # 9. Генерация сигнала
        signal = {
            "symbol": self.symbol,
            "side": delta_side,
            "confidence": confidence,
            "timestamp": time.time(),
            "features": {
                "delta_zscore": context['delta_zscore'],
                "obi": context['obi'],
                "speed": speed,
                "clusters": len(clusters)
            }
        }

        # 10. Публикация
        self.publish_signal(signal)

        return signal
```

#### Dynamic Symbol Management

Поддержка динамического добавления/удаления символов без перезапуска:

```python
class DynamicSymbolManager:
    def __init__(self, redis_client, config_stream="config:symbols"):
        self.redis = redis_client
        self.config_stream = config_stream
        self.handlers = {}

    def watch_config_changes(self):
        """Слушать изменения конфигурации"""
        while True:
            # Read from stream
            messages = self.redis.xread(
                {self.config_stream: '$'},
                block=1000
            )

            for stream, msg_list in messages:
                for msg_id, data in msg_list:
                    self.process_config_change(data)

    def process_config_change(self, data):
        """Обработка изменения конфигурации"""
        action = data.get('action')  # add | remove
        symbol = data.get('symbol')

        if action == 'add':
            self.add_symbol(symbol, data.get('config', {}))
        elif action == 'remove':
            self.remove_symbol(symbol)

    def add_symbol(self, symbol, config):
        """Добавить новый символ на лету"""
        if symbol in self.handlers:
            logger.warning(f"Symbol {symbol} already exists")
            return

        # Создать handler
        handler = create_handler_for_symbol(symbol, config)
        handler.start()

        self.handlers[symbol] = handler
        logger.info(f"Symbol {symbol} added dynamically")

    def remove_symbol(self, symbol):
        """Удалить символ"""
        if symbol not in self.handlers:
            return

        # Graceful stop
        handler = self.handlers[symbol]
        handler.stop()

        del self.handlers[symbol]
        logger.info(f"Symbol {symbol} removed")
```

---

### ATR Worker

#### Назначение

Вычисление Average True Range (ATR) из candles для использования в risk management.

#### Технические детали

**Язык**: Python 3.11+  
**Entry point**: `python-worker/services/atr_from_candles.py`

#### Функциональность

```python
class ATRCalculator:
    def __init__(self, period=14):
        self.period = period
        self.atr_values = defaultdict(lambda: deque(maxlen=period))

    def calculate_true_range(self, candle, prev_close):
        """
        True Range = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        """
        high = float(candle['high'])
        low = float(candle['low'])

        if prev_close is None:
            return high - low

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        return tr

    def calculate_atr(self, symbol, timeframe, candles):
        """Вычисление ATR по формуле Уайлдера"""
        key = f"{symbol}:{timeframe}"

        prev_close = None
        true_ranges = []

        for candle in candles:
            tr = self.calculate_true_range(candle, prev_close)
            true_ranges.append(tr)
            prev_close = float(candle['close'])

        if len(true_ranges) < self.period:
            return None

        # Первый ATR = среднее первых N true ranges
        if len(self.atr_values[key]) == 0:
            atr = np.mean(true_ranges[-self.period:])
        else:
            # Последующие ATR по формуле Уайлдера
            prev_atr = self.atr_values[key][-1]
            current_tr = true_ranges[-1]
            atr = (prev_atr * (self.period - 1) + current_tr) / self.period

        self.atr_values[key].append(atr)

        return atr

    def publish_atr(self, symbol, timeframe, atr):
        """Публикация ATR в Redis"""
        key = f"ta:last:atr:{symbol}:{timeframe}"

        redis.set(key, json.dumps({
            "symbol": symbol,
            "timeframe": timeframe,
            "atr": atr,
            "timestamp": time.time()
        }))

        logger.debug(f"ATR published: {symbol} {timeframe} = {atr:.4f}")
```

#### Consumer Logic

```python
def main():
    # Создание consumer group
    try:
        redis.xgroup_create(
            "candles:data",
            "atr-worker-group",
            id='0',
            mkstream=True
        )
    except Exception as e:
        pass  # Group already exists

    atr_calc = ATRCalculator(period=14)

    while True:
        # Чтение из stream
        messages = redis.xreadgroup(
            "atr-worker-group",
            "atr-worker-1",
            {"candles:data": '>'},
            count=10,
            block=1000
        )

        for stream, msg_list in messages:
            for msg_id, data in msg_list:
                # Обработка candle
                symbol = data['symbol']
                timeframe = data['timeframe']

                # Фильтр по символам и таймфреймам
                if symbol not in ATR_SYMBOLS:
                    continue
                if timeframe not in ATR_TFS:
                    continue

                # Получение исторических candles
                candles = get_recent_candles(symbol, timeframe, count=20)

                # Вычисление ATR
                atr = atr_calc.calculate_atr(symbol, timeframe, candles)

                if atr is not None:
                    # Публикация
                    atr_calc.publish_atr(symbol, timeframe, atr)

                # Acknowledge
                redis.xack("candles:data", "atr-worker-group", msg_id)
```

---

## 🎯 Signal Generation Layer

### Signal Generator (Technical Analysis)

#### Назначение

Генерация торговых сигналов на основе технических индикаторов (EMA, RSI, MACD, ATR).

#### Технические детали

**Язык**: Python 3.11+  
**Entry point**: `signal-generator/`  
**Port**: N/A (worker)

#### Индикаторы

##### 1. EMA (Exponential Moving Average)

```python
class EMAIndicator:
    def __init__(self, fast_period=9, slow_period=21):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.fast_ema = None
        self.slow_ema = None

    def calculate(self, prices):
        """Вычисление EMA"""
        # Fast EMA
        self.fast_ema = self._ema(prices, self.fast_period)

        # Slow EMA
        self.slow_ema = self._ema(prices, self.slow_period)

        return self.fast_ema, self.slow_ema

    def _ema(self, prices, period):
        """Экспоненциальная скользящая средняя"""
        if len(prices) < period:
            return None

        # Множитель
        multiplier = 2 / (period + 1)

        # Начальное SMA
        ema = np.mean(prices[:period])

        # Итеративное вычисление EMA
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema

        return ema

    def get_signal(self):
        """Определение сигнала по пересечению EMA"""
        if self.fast_ema is None or self.slow_ema is None:
            return None

        if self.fast_ema > self.slow_ema:
            return "LONG"  # Бычий тренд
        elif self.fast_ema < self.slow_ema:
            return "SHORT"  # Медвежий тренд

        return None
```

##### 2. RSI (Relative Strength Index)

```python
class RSIIndicator:
    def __init__(self, period=14, oversold=35, overbought=65):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.rsi = None

    def calculate(self, prices):
        """Вычисление RSI"""
        if len(prices) < self.period + 1:
            return None

        # Изменения цен
        deltas = np.diff(prices)

        # Разделение на gains и losses
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        # Средние gains и losses (EMA)
        avg_gain = self._ema(gains, self.period)
        avg_loss = self._ema(losses, self.period)

        if avg_loss == 0:
            self.rsi = 100
        else:
            rs = avg_gain / avg_loss
            self.rsi = 100 - (100 / (1 + rs))

        return self.rsi

    def get_signal(self):
        """Определение сигнала по RSI"""
        if self.rsi is None:
            return None

        if self.rsi < self.oversold:
            return "LONG"  # Перепроданность
        elif self.rsi > self.overbought:
            return "SHORT"  # Перекупленность

        return None
```

##### 3. MACD (Moving Average Convergence Divergence)

```python
class MACDIndicator:
    def __init__(self, fast=12, slow=26, signal=9):
        self.fast = fast
        self.slow = slow
        self.signal_period = signal
        self.macd_line = None
        self.signal_line = None
        self.histogram = None

    def calculate(self, prices):
        """Вычисление MACD"""
        # MACD Line = EMA(12) - EMA(26)
        ema_fast = self._ema(prices, self.fast)
        ema_slow = self._ema(prices, self.slow)
        self.macd_line = ema_fast - ema_slow

        # Signal Line = EMA(9) of MACD Line
        # (требуется накопление MACD значений)
        # Упрощенно:
        self.signal_line = self.macd_line  # Placeholder

        # Histogram = MACD Line - Signal Line
        self.histogram = self.macd_line - self.signal_line

        return self.macd_line, self.signal_line, self.histogram

    def get_signal(self):
        """Определение сигнала по MACD"""
        if self.histogram is None:
            return None

        if self.histogram > 0:
            return "LONG"  # Бычий сигнал
        elif self.histogram < 0:
            return "SHORT"  # Медвежий сигнал

        return None
```

#### Strategy Combination

```python
class TechnicalAnalysisStrategy:
    def __init__(self):
        self.ema = EMAIndicator(fast=9, slow=21)
        self.rsi = RSIIndicator(period=14, oversold=35, overbought=65)
        self.macd = MACDIndicator()
        self.atr = ATRIndicator(period=14)

    def analyze(self, candles):
        """Комплексный анализ"""
        prices = [float(c['close']) for c in candles]
        highs = [float(c['high']) for c in candles]
        lows = [float(c['low']) for c in candles]

        # Вычисление индикаторов
        self.ema.calculate(prices)
        self.rsi.calculate(prices)
        self.macd.calculate(prices)
        self.atr.calculate(highs, lows, prices)

        # Получение сигналов
        ema_signal = self.ema.get_signal()
        rsi_signal = self.rsi.get_signal()
        macd_signal = self.macd.get_signal()

        # Комбинация сигналов (все должны совпадать)
        if ema_signal == rsi_signal == macd_signal:
            if ema_signal in ["LONG", "SHORT"]:
                # Генерация финального сигнала
                signal = self.create_signal(
                    side=ema_signal,
                    entry=prices[-1],
                    atr=self.atr.value
                )

                return signal

        return None

    def create_signal(self, side, entry, atr):
        """Создание сигнала с SL/TP"""
        # Расчет Stop Loss
        sl_distance = atr * ATR_SL_MULTIPLIER  # 1.5
        sl = entry - sl_distance if side == "LONG" else entry + sl_distance

        # Расчет Take Profit levels
        tp_multipliers = [2.0, 3.0, 4.0]
        tps = []

        for mult in tp_multipliers:
            tp_distance = atr * mult
            tp = entry + tp_distance if side == "LONG" else entry - tp_distance
            tps.append(tp)

        # Формирование сигнала
        signal = {
            "symbol": SYMBOL,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tps[0],
            "tp2": tps[1],
            "tp3": tps[2],
            "atr": atr,
            "indicators": {
                "ema_fast": self.ema.fast_ema,
                "ema_slow": self.ema.slow_ema,
                "rsi": self.rsi.rsi,
                "macd": self.macd.histogram
            },
            "timestamp": time.time()
        }

        return signal
```

---

### Aggregated Hub V2

#### Назначение

Агрегация сигналов из разных источников (OrderFlow + TA) с взвешенным scoring и интеллектуальной фильтрацией.

#### Технические детали

**Язык**: Python 3.11+  
**Entry point**: `python-worker/aggregated_signal_hub_v2.py`

#### Weighted Confidence Blending

```python
class SignalAggregator:
    def __init__(self, weights):
        self.weights = weights
        # W_DELTA_PRO=0.50, W_SPEED=0.15, W_CLUSTER=0.25, W_LEGACY=0.10

    def blend_confidence(self, signals):
        """
        Взвешенное комбинирование confidence из разных источников
        """
        # Извлечение confidence
        delta_conf = signals.get('orderflow', {}).get('delta_confidence', 0)
        speed_conf = signals.get('orderflow', {}).get('speed_confidence', 0)
        cluster_conf = signals.get('orderflow', {}).get('cluster_confidence', 0)
        ta_conf = signals.get('ta', {}).get('confidence', 0)

        # Взвешенное среднее
        total_confidence = (
            self.weights['delta'] * delta_conf +
            self.weights['speed'] * speed_conf +
            self.weights['cluster'] * cluster_conf +
            self.weights['ta'] * ta_conf
        )

        return total_confidence
```

#### Anti-Dither Protection

Предотвращение частой смены направления сигналов.

```python
class SideLockManager:
    def __init__(self, lock_duration=20):
        self.lock_duration = lock_duration  # seconds
        self.last_side = None
        self.lock_until = 0

    def check_side_change(self, new_side):
        """
        Проверка, можно ли сменить направление сигнала
        """
        now = time.time()

        # Если сторона не менялась, ОК
        if new_side == self.last_side:
            return True

        # Если lock еще активен, блокируем смену
        if now < self.lock_until:
            logger.debug(f"Side change blocked, lock for {self.lock_until - now:.1f}s")
            return False

        # Разрешаем смену и устанавливаем новый lock
        self.last_side = new_side
        self.lock_until = now + self.lock_duration

        return True
```

#### Signal Aggregation Logic

```python
def aggregate_signals():
    """Основной цикл агрегации сигналов"""
    aggregator = SignalAggregator(weights=WEIGHTS)
    side_lock = SideLockManager(lock_duration=HUB_SIDE_LOCK_SEC)
    cooldown_manager = CooldownManager(interval=HUB_MIN_SIG_INT_SEC)

    while True:
        # 1. Читаем сигналы из разных streams
        orderflow_signals = read_stream('signals:orderflow:XAUUSD')
        ta_signals = read_stream('signals:ta:XAUUSD')

        # 2. Проверяем наличие сигналов
        if not orderflow_signals and not ta_signals:
            time.sleep(1)
            continue

        # 3. Комбинируем сигналы
        combined = {
            'orderflow': orderflow_signals[-1] if orderflow_signals else {},
            'ta': ta_signals[-1] if ta_signals else {}
        }

        # 4. Проверяем согласованность направлений
        of_side = combined['orderflow'].get('side')
        ta_side = combined['ta'].get('side')

        if of_side != ta_side:
            logger.debug("Signals disagree, skipping")
            continue

        final_side = of_side

        # 5. Взвешенный confidence
        confidence = aggregator.blend_confidence(combined)

        # 6. Проверка порога
        if confidence < HUB_CONFIDENCE_THR:
            logger.debug(f"Confidence {confidence:.2f} below threshold")
            continue

        # 7. Anti-dither check
        if not side_lock.check_side_change(final_side):
            logger.debug("Side locked, skipping")
            continue

        # 8. Cooldown check
        if not cooldown_manager.check():
            logger.debug("Cooldown active, skipping")
            continue

        # 9. Получение дополнительных данных
        current_price = get_current_price()
        atr = get_atr()
        book = get_order_book()
        pivots = get_pivots()

        # 10. Risk calculation
        risk_sizer = RiskPositionSizer(
            account_balance=ACCOUNT_BALANCE,
            risk_percent=RISK_PCT
        )

        position = risk_sizer.calculate(
            entry=current_price,
            sl_atr_mult=SL_MULT,
            tp_atr_mults=TP_MULTS,
            atr=atr
        )

        # 11. Формирование финального сигнала
        final_signal = {
            "symbol": SYMBOL,
            "side": final_side,
            "confidence": confidence,
            "entry": current_price,
            "sl": position['sl'],
            "tp1": position['tp1'],
            "tp2": position['tp2'],
            "tp3": position['tp3'],
            "lot": position['lot'],
            "timestamp": time.time(),
            "sources": {
                "orderflow": combined['orderflow'],
                "ta": combined['ta']
            },
            "context": {
                "atr": atr,
                "book": book,
                "pivots": pivots
            }
        }

        # 12. Отправка в Go Gateway
        send_to_gateway(final_signal)

        # 13. Уведомление в Telegram
        send_telegram_notification(final_signal)

        # 14. Сохранение в Parquet (для ML)
        save_to_parquet(final_signal)

        logger.info(f"Signal aggregated: {final_side} @ {current_price}, conf={confidence:.2f}")
```

---

## 🚀 Execution Layer

### Go Gateway

#### Назначение

Центральный API сервер для управления ордерами, отправки уведомлений и интеграции с внешними системами.

#### Технические детали

**Язык**: Go 1.22+  
**Port**: 8090  
**Entry point**: `go-gateway/main.go`

#### API Endpoints

##### POST /orders/push

Добавление ордера в очередь (от Aggregated Hub).

**Request**:

```json
{
	"symbol": "XAUUSD",
	"side": "LONG",
	"entry": 2055.5,
	"sl": 2050.0,
	"tp1": 2060.0,
	"tp2": 2065.0,
	"tp3": 2070.0,
	"lot": 0.05,
	"confidence": 0.75
}
```

**Response**:

```json
{
	"status": "queued",
	"order_id": "abc123"
}
```

##### GET /orders/poll

Получение ордеров из очереди (для MT5 EA).

**Response**:

```json
{
	"orders": [
		{
			"order_id": "abc123",
			"symbol": "XAUUSD",
			"side": "LONG",
			"entry": 2055.5,
			"sl": 2050.0,
			"tp1": 2060.0
		}
	]
}
```

##### POST /orders/confirm

Подтверждение исполнения ордера (от MT5 EA).

**Request**:

```json
{
	"order_id": "abc123",
	"ticket": 12345678,
	"status": "filled"
}
```

##### POST /notify

Отправка уведомления в Telegram (от OBI Service).

**Request**:

```json
{
	"message": "OBI Signal: LONG @ 2055.50",
	"chart_url": "http://py-obi-service:8088/render/obi.png"
}
```

##### GET /healthz

Health check.

#### Order Queue Implementation

```go
type OrderQueue struct {
    orders []Order
    mu     sync.RWMutex
}

func (q *OrderQueue) Push(order Order) string {
    q.mu.Lock()
    defer q.mu.Unlock()

    order.ID = generateID()
    order.Status = "queued"
    order.CreatedAt = time.Now()

    q.orders = append(q.orders, order)

    log.Printf("Order queued: %s %s @ %.2f", order.Symbol, order.Side, order.Entry)

    return order.ID
}

func (q *OrderQueue) Poll() []Order {
    q.mu.RLock()
    defer q.mu.RUnlock()

    // Возвращаем только pending orders
    pending := []Order{}
    for _, order := range q.orders {
        if order.Status == "queued" {
            pending = append(pending, order)
        }
    }

    return pending
}

func (q *OrderQueue) Confirm(orderID string, ticket int64) error {
    q.mu.Lock()
    defer q.mu.Unlock()

    for i, order := range q.orders {
        if order.ID == orderID {
            q.orders[i].Status = "filled"
            q.orders[i].Ticket = ticket
            q.orders[i].FilledAt = time.Now()

            log.Printf("Order confirmed: %s, ticket: %d", orderID, ticket)
            return nil
        }
    }

    return errors.New("order not found")
}
```

#### Telegram Integration

```go
type TelegramBot struct {
    token  string
    chatID string
    client *http.Client
}

func (b *TelegramBot) SendMessage(text string) error {
    url := fmt.Sprintf(
        "https://api.telegram.org/bot%s/sendMessage",
        b.token,
    )

    payload := map[string]interface{}{
        "chat_id": b.chatID,
        "text":    text,
        "parse_mode": "Markdown",
    }

    body, _ := json.Marshal(payload)

    resp, err := b.client.Post(url, "application/json", bytes.NewBuffer(body))
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != 200 {
        return fmt.Errorf("telegram API error: %d", resp.StatusCode)
    }

    return nil
}

func (b *TelegramBot) SendPhoto(photoURL string, caption string) error {
    url := fmt.Sprintf(
        "https://api.telegram.org/bot%s/sendPhoto",
        b.token,
    )

    payload := map[string]interface{}{
        "chat_id": b.chatID,
        "photo":   photoURL,
        "caption": caption,
    }

    // ... аналогично SendMessage
}
```

---

### Paper Executor

#### Назначение

Виртуальное исполнение ордеров для тестирования стратегий без реальных денег.

#### Технические детали

**Язык**: Python 3.11+  
**Entry point**: `python-worker/paper_executor.py`

#### Функциональность

```python
class PaperExecutor:
    def __init__(self):
        self.positions = {}
        self.balance = INITIAL_BALANCE
        self.equity = INITIAL_BALANCE
        self.trades = []

    def execute_order(self, order):
        """Виртуальное исполнение ордера"""
        position_id = generate_position_id()

        position = {
            "id": position_id,
            "symbol": order['symbol'],
            "side": order['side'],
            "entry": order['entry'],
            "sl": order['sl'],
            "tp1": order['tp1'],
            "tp2": order['tp2'],
            "tp3": order['tp3'],
            "lot": order['lot'],
            "open_time": time.time(),
            "status": "open"
        }

        self.positions[position_id] = position

        logger.info(f"Paper position opened: {position_id} {order['side']} {order['lot']} @ {order['entry']}")

        # Publish to stream
        self.publish_position(position)

        return position_id

    def update_positions(self, current_price):
        """Обновление открытых позиций"""
        for pos_id, pos in list(self.positions.items()):
            if pos['status'] != 'open':
                continue

            # Проверка SL/TP
            closed, reason = self.check_close_conditions(pos, current_price)

            if closed:
                self.close_position(pos_id, current_price, reason)

    def check_close_conditions(self, position, current_price):
        """Проверка условий закрытия позиции"""
        if position['side'] == 'LONG':
            # Stop Loss
            if current_price <= position['sl']:
                return True, 'SL'

            # Take Profits
            if current_price >= position['tp3']:
                return True, 'TP3'
            elif current_price >= position['tp2']:
                return True, 'TP2'
            elif current_price >= position['tp1']:
                return True, 'TP1'

        else:  # SHORT
            if current_price >= position['sl']:
                return True, 'SL'

            if current_price <= position['tp3']:
                return True, 'TP3'
            elif current_price <= position['tp2']:
                return True, 'TP2'
            elif current_price <= position['tp1']:
                return True, 'TP1'

        return False, None

    def close_position(self, pos_id, close_price, reason):
        """Закрытие позиции"""
        pos = self.positions[pos_id]

        # Расчет P&L
        if pos['side'] == 'LONG':
            pnl = (close_price - pos['entry']) * pos['lot'] * CONTRACT_SIZE
        else:
            pnl = (pos['entry'] - close_price) * pos['lot'] * CONTRACT_SIZE

        # Обновление баланса
        self.balance += pnl
        self.equity = self.balance

        # Сохранение trade
        trade = {
            **pos,
            "close_price": close_price,
            "close_time": time.time(),
            "pnl": pnl,
            "reason": reason
        }

        self.trades.append(trade)

        # Обновление позиции
        self.positions[pos_id]['status'] = 'closed'
        self.positions[pos_id]['close_price'] = close_price
        self.positions[pos_id]['pnl'] = pnl

        logger.info(f"Position closed: {pos_id} @ {close_price}, P&L: {pnl:.2f}, Reason: {reason}")

        # Publish
        self.publish_trade(trade)

        # Save to Parquet
        self.save_to_parquet(trade)
```

---

## 📱 Telegram Layer

### Telegram Worker

#### Назначение

Многопоточное прослушивание Telegram каналов и публикация сообщений в Redis.

#### Технические детали

**Язык**: Python 3.11+ (Telethon)  
**Entry point**: `telegram-worker/multithreaded_worker.py`

#### Multi-threading Architecture

```python
class MultiThreadedTelegramWorker:
    def __init__(self, max_threads=5, channels_per_thread=20):
        self.max_threads = max_threads
        self.channels_per_thread = channels_per_thread
        self.threads = []

    def start(self):
        """Запуск multi-threaded worker"""
        # Загрузка каналов из Redis
        channels = self.load_channels_from_redis()

        # Разделение каналов по потокам
        channel_chunks = self.split_channels(channels, self.channels_per_thread)

        # Создание и запуск потоков
        for i, chunk in enumerate(channel_chunks):
            thread = TelegramListenerThread(
                thread_id=i,
                channels=chunk,
                redis_url=REDIS_URL
            )
            thread.start()
            self.threads.append(thread)

        logger.info(f"Started {len(self.threads)} Telegram listener threads")

        # Ожидание
        for thread in self.threads:
            thread.join()
```

#### Event Handling

```python
class TelegramListenerThread(threading.Thread):
    def __init__(self, thread_id, channels, redis_url):
        super().__init__()
        self.thread_id = thread_id
        self.channels = channels
        self.redis = redis.from_url(redis_url)
        self.client = None

    def run(self):
        """Основной цикл потока"""
        # Создание Telegram client
        self.client = TelegramClient(
            f'session_{self.thread_id}',
            api_id=API_ID,
            api_hash=API_HASH
        )

        # Подключение
        self.client.start(phone=PHONE)

        # Подписка на каналы
        for channel in self.channels:
            self.subscribe_to_channel(channel)

        # Запуск event loop
        self.client.run_until_disconnected()

    @client.on(events.NewMessage())
    async def handle_new_message(self, event):
        """Обработка нового сообщения"""
        # Получение информации
        channel = event.chat.username
        message_id = event.message.id
        text = event.message.text
        timestamp = event.message.date

        # Формирование записи
        record = {
            "channel": channel,
            "message_id": message_id,
            "text": text,
            "timestamp": timestamp.isoformat(),
            "raw": event.message.to_dict()
        }

        # Публикация в Redis
        self.redis.xadd(
            RAW_STREAM,
            record,
            maxlen=10000
        )

        logger.debug(f"[Thread {self.thread_id}] Message from {channel}: {text[:50]}")
```

---

## 🔍 Monitoring Layer

### Prometheus

Сбор метрик со всех сервисов.

#### Metrics Examples

```
# Go Workers
binance_ws_messages_total{worker="1m"} 125000
binance_ws_errors_total{worker="1m"} 3
redis_publish_duration_seconds{worker="1m",quantile="0.99"} 0.002

# Python Workers
orderflow_signals_generated_total{symbol="XAUUSD"} 47
orderflow_delta_zscore{symbol="XAUUSD"} 3.25
orderflow_processing_duration_seconds{quantile="0.95"} 0.015

# Redis
redis_connected_clients 42
redis_used_memory_bytes 5368709120
redis_stream_length{stream="candles:data"} 98543
```

### Grafana

Визуализация метрик через дашборды.

---

**Итого: 30+ микросервисов, каждый со своей четкой ответственностью и API.**
