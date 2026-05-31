# CryptoOrderflowService - Детальная документация

## Обзор

**CryptoOrderflowService** - это асинхронный воркер на базе `redis.asyncio`, который является ядром системы обработки крипто-тиков. Сервис читает тики и книги заявок из Redis Streams, применяет цепочку детекторов order flow и генерирует торговые сигналы.

**Расположение**: `python-worker/services/crypto_orderflow_service.py`

**Наследование**: От базового класса `BaseOrderFlowHandler`

**Архитектура**: Многопоточная обработка по символам с использованием Consumer Groups Redis Streams

## Архитектурные принципы

### 1. Многопоточная обработка
- Каждый символ обрабатывается в отдельной паре задач (тики + книги)
- Consumer Groups Redis обеспечивают автоматическое распределение нагрузки между инстансами
- Graceful shutdown с корректной отменой всех задач

### 2. Fail-Open архитектура
- Сервис продолжает работу при сбоях отдельных компонентов
- Все исключения логируются, но не останавливают обработку
- Fallback значения для критических параметров

### 3. Конфигурационная гибкость
- Динамическая загрузка символов из Redis Set `crypto:symbols`
- Per-symbol конфигурация в хэшах `config:orderflow:<symbol>`
- Горячая перезагрузка конфигурации без перезапуска

## Детальная структура класса

### Основные атрибуты

#### Redis подключения
```python
self.main: aioredis.Redis     # Основное Redis подключение для метаданных
self.ticks: aioredis.Redis    # Redis для стримов тиков (может быть отдельным)
self.notify_client: aioredis.Redis  # Redis для уведомлений (опционально отдельный)
```

#### Состояние сервиса
```python
self.symbol_contexts: Dict[str, SymbolRuntime]  # Контекст по символам
self.symbol_tasks: Dict[str, Tuple[asyncio.Task, asyncio.Task]]  # Задачи по символам
self._refresh_task: Optional[asyncio.Task]      # Задача периодического обновления
self._shutdown: bool                           # Флаг завершения работы
```

#### Идентификаторы Consumer Groups
```python
self.consumer_id_ticks: str  # Уникальный ID для чтения тиков
self.consumer_id_books: str  # Уникальный ID для чтения книг
```

#### Streams и очереди
```python
self.notify_stream: str                    # Stream для уведомлений (notify:telegram)
self.raw_signal_stream: str               # Stream для сырых сигналов (signals:crypto:raw)
self.orders_queue: str                    # Очередь ордеров (orders:queue)
self.cryptoorderflow_signal_stream_template: str  # Шаблон для структурированных сигналов
```

#### Компоненты
```python
self.atr_cache: ATRCache                  # Кеш ATR значений
self.conf_scorer: ConfidenceScorer        # Скорер уверенности сигналов
self.force_trail_after_tp1: Optional[bool] # Флаг трейлинга после TP1
```

### Класс SymbolRuntime

**SymbolRuntime** - контекст обработки для каждого символа, содержащий все необходимое состояние.

#### Атрибуты SymbolRuntime

**Идентификация:**
```python
symbol: str           # Название символа (BTCUSDT, ETHUSDT)
config: Dict[str, Any] # Конфигурация обработки
```

**Streams и группы:**
```python
tick_stream: str      # Stream тиков (stream:tick_<symbol>)
book_stream: str      # Stream книги (stream:book_<symbol>)
tick_group: str       # Consumer group для тиков
book_group: str       # Consumer group для книги
```

**Детекторы:**
```python
delta_detector: DeltaSpikeDetector
obi_detector: OBIDetector
absorption_detector: AbsorptionDetector
iceberg_detector: IcebergDetector
```

**Буферы и состояние:**
```python
tick_buffer: Deque[Dict[str, Any]]    # Кольцевой буфер тиков
last_book: Optional[Dict[str, Any]]   # Последняя книга заявок
last_obi_event: Optional[Dict[str, Any]]      # Последнее событие OBI
last_iceberg_event: Optional[Dict[str, Any]]  # Последнее событие Iceberg
```

**Тайминги и cooldown:**
```python
last_signal_ts: int   # Timestamp последнего сигнала (ms)
```

**Телеметрия:**
```python
tick_count: int       # Счетчик обработанных тиков
delta_triggers: int   # Счетчик срабатываний delta детектора
signal_count: int     # Счетчик сгенерированных сигналов
last_metrics_ts: float # Timestamp последней телеметрии
```

#### Метод apply_config()

```python
def apply_config(self, new_config: Dict[str, Any]) -> None:
    """
    Обновляет конфиг и перезагружает детекторы без потери истории тиков.
    """
    prev_ticks: List[Dict[str, Any]] = list(self.tick_buffer) if hasattr(self, "tick_buffer") else []
    self.config = new_config.copy()
    self.tick_buffer = deque(prev_ticks, maxlen=self.config["tick_buffer"])

    # Переинициализация детекторов с новыми параметрами
    self.delta_detector = DeltaSpikeDetector(
        window=self.config["delta_window"],
        z_threshold=self.config["delta_z_threshold"],
        min_abs_volume=self.config["delta_abs_min"],
    )
    # ... остальные детекторы
```

## Детальная логика методов

### Инициализация (__init__)

#### Этапы инициализации:

1. **Настройка Redis подключений**
   ```python
   # Основное подключение
   self.main = aioredis.from_url(
       self.redis_dsn,
       decode_responses=True,
       socket_connect_timeout=10,
       socket_timeout=30,
       max_connections=200
   )

   # Подключение для тиков (может быть отдельным)
   resolved_ticks_dsn = ticks_dsn or os.getenv("REDIS_TICKS_URL") or redis_dsn
   self.ticks = aioredis.from_url(resolved_ticks_dsn, ...)
   ```

2. **Генерация уникальных идентификаторов**
   ```python
   rnd = random.randint(1000, 9999)
   self.consumer_id_ticks = f"crypto-of-ticks-{os.getpid()}-{rnd}"
   self.consumer_id_books = f"crypto-of-books-{os.getpid()}-{rnd}"
   ```

3. **Инициализация компонентов**
   ```python
   self.atr_cache = get_atr_cache()
   self.conf_scorer = ConfidenceScorer(...)
   ```

### Основной цикл (run_forever)

```python
async def run_forever(self) -> None:
    """
    Основной цикл сервиса. Останавливается по сигналу отмены.
    """
    await self.load_dynamic_symbols()  # Загрузка символов
    self._refresh_task = asyncio.create_task(self._refresh_loop(), name="crypto-of-refresh")

    try:
        while True:
            await asyncio.sleep(3600)  # Бесконечный цикл с ежечасным пробуждением
    except asyncio.CancelledError:
        logger.info("🛑 Получен сигнал остановки run_forever")
        raise
    finally:
        await self.shutdown()
```

### Загрузка символов (load_dynamic_symbols)

```python
async def load_dynamic_symbols(self) -> None:
    symbols = set(sym.upper() for sym in DEFAULT_SYMBOLS)
    redis_symbols = await self.main.smembers("crypto:symbols")
    symbols.update(sym.upper() for sym in redis_symbols)

    for symbol in sorted(symbols):
        config = await self._build_symbol_config(symbol)
        tick_stream, book_stream = await self._resolve_streams(symbol)

        runtime = self.symbol_contexts.get(symbol)
        if runtime is None:
            runtime = SymbolRuntime(symbol=symbol, config=config)
            self.symbol_contexts[symbol] = runtime
        else:
            runtime.apply_config(config)

        # Обновление ссылок на стримы
        runtime.tick_stream = tick_stream
        runtime.book_stream = book_stream

        # Запуск задач если не запущены
        if symbol not in self.symbol_tasks:
            tick_task = asyncio.create_task(self.consume_ticks(symbol))
            book_task = asyncio.create_task(self.consume_books(symbol))
            self.symbol_tasks[symbol] = (tick_task, book_task)
```

### Обработка тиков (consume_ticks)

```python
async def consume_ticks(self, symbol: str) -> None:
    while True:
        runtime = self.symbol_contexts.get(symbol)
        if runtime is None:
            await asyncio.sleep(1)
            continue

        # Убеждаемся что consumer group существует
        await self._ensure_group(self.ticks, runtime.tick_stream, runtime.tick_group)

        # Чтение пакета сообщений
        messages = await self.ticks.xreadgroup(
            groupname=runtime.tick_group,
            consumername=self.consumer_id_ticks,
            streams={runtime.tick_stream: ">"},
            count=runtime.config.get("read_count", 200),
            block=runtime.config.get("read_block_ms", 1000),
        )

        for stream_name, entries in messages:
            for msg_id, payload in entries:
                try:
                    # Парсинг и обработка тика
                    tick = self._parse_tick_payload(payload)
                    runtime.tick_buffer.append(tick)
                    signal = self._handle_tick(runtime, tick)

                    if signal:
                        await self.publish_signal(runtime, signal)

                except Exception:
                    logger.exception("Ошибка обработки тика")
                finally:
                    # ВАЖНО: XACK всегда в finally для предотвращения переполнения pending
                    await self.ticks.xack(stream_name, runtime.tick_group, msg_id)
```

### Парсинг payload тика (_parse_tick_payload)

```python
def _parse_tick_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
    if "data" in payload:
        nested = json.loads(payload["data"])
    else:
        nested = {}

    merged = {**payload, **nested}

    tick = {
        "symbol": merged.get("symbol"),
        "ts": _safe_int(merged.get("ts") or merged.get("event_time")),
        "price": merged.get("price") or merged.get("last") or merged.get("mid"),
        "qty": _safe_float(merged.get("qty") or merged.get("volume")),
        "side": str(merged.get("side") or merged.get("trade_side") or "BUY").upper(),
        "bid": merged.get("bid"),
        "ask": merged.get("ask"),
        "written_at": _safe_int(merged.get("written_at")),
    }

    # Вычисление производных полей
    if tick["bid"] and tick["ask"]:
        tick["mid"] = (tick["bid"] + tick["ask"]) / 2

    return tick
```

### Основная логика обработки (_handle_tick)

#### Этапы обработки:

1. **Валидация входных данных**
   ```python
   if not tick or not isinstance(tick, dict):
       return None
   runtime.tick_count += 1
   ```

2. **Нормализация количества**
   ```python
   if "qty" not in tick and "volume" in tick:
       tick["qty"] = tick.get("volume")
   if tick.get("qty") is None and tick.get("volume") is None:
       tick["qty"] = 0.0
   ```

3. **Применение Delta детектора**
   ```python
   delta_event = runtime.delta_detector.push(tick)
   if not delta_event:
       self._log_metrics(runtime)
       return None
   runtime.delta_triggers += 1
   ```

4. **Определение направления**
   ```python
   direction = "LONG" if delta_event["delta"] >= 0 else "SHORT"
   price = _safe_float(tick.get("price")) or _safe_float(tick.get("last")) or _safe_float(tick.get("mid"))
   ```

5. **Сбор подтверждений от других детекторов**
   ```python
   confirmations: List[str] = []
   indicators: Dict[str, Any] = {
       "delta": delta_event.get("delta", 0.0),
       "delta_z": delta_event.get("z", 0.0),
   }

   # OBI подтверждение
   if runtime.last_obi_event:
       obi_dir = runtime.last_obi_event.get("direction")
       if obi_dir and obi_dir.upper() == direction:
           obi_val = runtime.last_obi_event.get("obi", 0.0)
           confirmations.append(f"obi={obi_val:.2f}")
           indicators["obi"] = obi_val

   # Absorption подтверждение
   absorption = runtime.absorption_detector.push(tick, runtime.last_book, price)
   if absorption and absorption.get("side"):
       indicators["absorption_volume"] = absorption.get("volume")
       if absorption["side"].upper() == direction:
           confirmations.append(f"absorption={absorption['volume']:.2f}")

   # Iceberg подтверждение
   if runtime.last_iceberg_event:
       side = runtime.last_iceberg_event.get("side")
       if (side == "bid" and direction == "LONG") or (side == "ask" and direction == "SHORT"):
           refresh = runtime.last_iceberg_event.get("refresh", 0)
           confirmations.append(f"iceberg_refresh={refresh}")
           indicators["iceberg_refresh"] = refresh
   ```

6. **Проверка минимальных условий**
   ```python
   delta_abs = abs(delta_event.get("delta", 0.0))
   min_delta = runtime.config["delta_abs_min_confirm"]
   min_confirms = runtime.config["min_confirmations"]

   if delta_abs < min_delta and len(confirmations) < min_confirms:
       return None
   ```

7. **Проверка cooldown**
   ```python
   now_ms = int(time.time() * 1000)
   cooldown_ms = runtime.config["signal_cooldown_sec"] * 1000
   time_since_last = now_ms - runtime.last_signal_ts

   if time_since_last < cooldown_ms:
       return None
   runtime.last_signal_ts = now_ms
   ```

8. **Расчет уверенности сигнала**
   ```python
   confidence = self._compute_confidence(runtime, indicators, confirmations, side=direction, kind=primary_reason)
   ```

9. **Фильтр по минимальной уверенности**
   ```python
   min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", os.getenv("SIGNAL_MIN_CONF", "80")))
   min_conf = min_conf_pct / 100.0
   if confidence < min_conf:
       return None
   ```

10. **Формирование сигнала**
    ```python
    signal_id = f"crypto-of:{runtime.symbol}:{now_ms}"
    primary_reason = "delta_spike"
    if confirmations:
        primary_reason = confirmations[0].split("=", 1)[0]

    payload = {
        "signal_id": signal_id,
        "symbol": runtime.symbol,
        "direction": direction,
        "entry": price,
        "delta": delta_event.get("delta"),
        "delta_z": delta_event.get("z"),
        "confirmations": confirmations,
        "reason": primary_reason,
        "generated_at": now_ms,
        "ts": tick.get("ts"),
        "indicators": indicators,
        # ... остальные поля
    }
    ```

### Публикация сигнала (publish_signal)

#### Этапы публикации:

1. **Валидация входных данных**
   ```python
   if direction not in {"LONG", "SHORT"}:
       return
   ```

2. **Расчет уровней (SL/TP/Lot/ATR)**
   ```python
   sl, tp_levels, lot, atr = self._calculate_levels(runtime, entry, direction, indicators, trail_profile=trail_profile)
   ```

3. **Проверка ATR Gate (опционально)**
   ```python
   if gate_mode in {"SHADOW", "ENFORCE"} and trail_profile == "rocket_v1":
       passed, reason, gate_meta = await self._check_fees_aware_gate(runtime, atr, entry, tp1_share=tp1_share_actual)
       if not passed and gate_mode == "ENFORCE":
           return  # Вето сигнала
   ```

4. **Формирование payload'ов для разных каналов**
   ```python
   # Enriched signal для raw stream
   enriched_signal = self._build_enriched_signal(...)

   # Audit payload для orderflow stream
   audit_payload = self._build_audit_payload(...)

   # Telegram payload
   telegram_payload = self._build_telegram_payload(...)
   ```

5. **Публикация в каналы**
   ```python
   # Telegram (с rate limiting)
   await self._publish_telegram(telegram_payload)

   # Raw и audit streams
   publisher = AsyncSignalPublisher(...)
   await publisher.publish(enriched_signal)
   await publisher.publish(audit_payload)

   # Orders queue (опционально)
   if self._should_publish_orders(runtime, signal):
       await self._publish_orders_queue(runtime, signal)
   ```

### Обработка книги заявок (consume_books)

```python
async def consume_books(self, symbol: str) -> None:
    while True:
        runtime = self.symbol_contexts.get(symbol)
        if runtime is None:
            await asyncio.sleep(1)
            continue

        await self._ensure_group(self.ticks, runtime.book_stream, runtime.book_group)

        messages = await self.ticks.xreadgroup(
            groupname=runtime.book_group,
            consumername=self.consumer_id_books,
            streams={runtime.book_stream: ">"},
            count=runtime.config.get("read_count", 200),
            block=runtime.config.get("read_block_ms", 1000),
        )

        for stream_name, entries in messages:
            for msg_id, payload in entries:
                try:
                    book = self._parse_book_payload(payload, symbol)
                    runtime.last_book = book

                    # Обновление детекторов
                    obi_event = runtime.obi_detector.push(book)
                    iceberg_event = runtime.iceberg_detector.push(book)

                    # Сохранение событий для использования в _handle_tick
                    runtime.last_obi_event = serialize_obi(obi_event)
                    runtime.last_iceberg_event = serialize_iceberg(iceberg_event)

                finally:
                    await self.ticks.xack(stream_name, runtime.book_group, msg_id)
```

### Расчет уровней (_calculate_levels)

```python
def _calculate_levels(self, runtime: SymbolRuntime, entry: float, direction: str,
                     indicators: Dict[str, Any], trail_profile: str = "rocket_v1") -> Tuple[float, List[float], float, float]:
    """
    Расчет стоп-лосса, тейк-профитов, размера позиции и ATR.

    Returns:
        sl, tp_levels, lot, atr
    """
    # Получение ATR
    atr = self._get_atr_for_symbol(runtime.symbol, runtime.config)

    # Расчет SL
    sl_distance = atr * runtime.config.get("sl_atr_multiplier", 1.0)
    sl = entry - sl_distance if direction == "LONG" else entry + sl_distance

    # Расчет TP уровней
    tp_ratios = parse_tp_ratio(runtime.config.get("tp_ratio", "1:2:3"))
    tp_levels = []
    for ratio in tp_ratios:
        distance = atr * ratio
        tp = entry + distance if direction == "LONG" else entry - distance
        tp_levels.append(tp)

    # Расчет размера позиции
    lot = self._calculate_position_size(runtime, entry, sl, atr)

    return sl, tp_levels, lot, atr
```

### Расчет уверенности (_compute_confidence)

```python
def _compute_confidence(self, runtime: SymbolRuntime, indicators: Dict[str, Any],
                       confirmations: List[str], side: str, kind: str) -> float:
    """
    Расчет уверенности сигнала на основе индикаторов и подтверждений.
    """
    # Использование ConfidenceScorer
    score = self.conf_scorer.score_signal(
        indicators=indicators,
        confirmations=confirmations,
        side=side,
        kind=kind,
        symbol=runtime.symbol
    )

    return normalize_confidence_pct(score)
```

### Периодический refresh (_refresh_loop)

```python
async def _refresh_loop(self) -> None:
    """
    Периодическое обновление списка символов и их конфигурации.
    """
    while not self._shutdown:
        try:
            await self.load_dynamic_symbols()
            await asyncio.sleep(self.refresh_interval)
        except Exception as e:
            logger.exception("Ошибка в refresh loop")
            await asyncio.sleep(10)  # Краткий сон при ошибке
```

### Graceful shutdown

```python
async def shutdown(self) -> None:
    """
    Корректное завершение работы сервиса.
    """
    logger.info("🛑 Начинаем graceful shutdown CryptoOrderflowService")

    self._shutdown = True

    # Отмена refresh задачи
    if self._refresh_task:
        self._refresh_task.cancel()
        try:
            await self._refresh_task
        except asyncio.CancelledError:
            pass

    # Отмена задач по символам
    for symbol, (tick_task, book_task) in self.symbol_tasks.items():
        logger.info(f"Отменяем задачи для {symbol}")
        tick_task.cancel()
        book_task.cancel()

    # Ожидание завершения
    await asyncio.gather(*[task for tasks in self.symbol_tasks.values() for task in tasks], return_exceptions=True)

    # Закрытие Redis подключений
    await self._close_redis(self.main)
    await self._close_redis(self.ticks)
    if hasattr(self, 'notify_client') and self.notify_client != self.main:
        await self._close_redis(self.notify_client)

    logger.info("✅ CryptoOrderflowService завершен")
```

## Конфигурационные параметры

### Глобальные переменные окружения

- `CRYPTO_OF_REFRESH_SEC`: период обновления символов (default: 30)
- `CRYPTO_OF_LOG_LEVEL`: уровень логирования (default: INFO)
- `CRYPTO_OF_DEBUG_DELTAS`: флаг подробного логирования дельты
- `REDIS_TICKS_URL`: URL отдельного Redis для тиков
- `CRYPTO_NOTIFY_STREAM`: stream для уведомлений (default: notify:telegram)
- `CRYPTO_RAW_STREAM`: stream для сырых сигналов (default: signals:crypto:raw)
- `CRYPTO_SIGNAL_MIN_CONF`: минимальная уверенность сигнала (%)
- `ATR_TF`: таймфрейм ATR (default: 1m)
- `ATR_REDIS_STALENESS_MULT`: множитель проверки свежести ATR

### Per-symbol конфигурация (config:orderflow:<symbol>)

**Детекторы:**
- `delta_window`: размер окна для delta (default: 60)
- `delta_z_threshold`: порог z-score (default: 3.0)
- `delta_abs_min`: минимальный абсолютный объем (default: 0.0)
- `obi_depth`: глубина книги для OBI (default: 10)
- `obi_threshold`: порог OBI (default: 0.6)
- `absorption_min_volume`: мин объем поглощения (default: 1.0)
- `iceberg_refresh`: порог обновлений айсберга (default: 3)

**Сигналы:**
- `min_confirmations`: мин количество подтверждений (default: 0)
- `signal_cooldown_sec`: cooldown между сигналами (default: 60)
- `delta_abs_min_confirm`: мин абсолютная дельта для подтверждения

**Уровни:**
- `sl_atr_multiplier`: множитель ATR для SL (default: 1.0)
- `tp_ratio`: соотношение TP уровней (default: "1:2:3")
- `trail_profile`: профиль трейлинга (default: "rocket_v1")

**Производительность:**
- `tick_buffer`: размер буфера тиков (default: 200)
- `read_count`: размер пакета чтения (default: 200)
- `read_block_ms`: таймаут блокировки чтения (default: 1000)

## Мониторинг и метрики

### Prometheus метрики

```python
atr_gate_veto_total = Counter(
    'atr_gate_veto_total',
    'Total signals vetoed by ATR gate',
    ['symbol', 'reason', 'mode']
)

tp1_net_margin_bps_gauge = Gauge(
    'tp1_net_margin_bps',
    'Net profit margin at TP1 after fees and buffer (bps)',
    ['symbol']
)

signals_total = Counter(
    'signals_total',
    'Total number of signals processed by the worker',
    ['symbol', 'handler']
)
```

### Внутренняя телеметрия

Каждый SymbolRuntime содержит счетчики:
- `tick_count`: общее количество обработанных тиков
- `delta_triggers`: количество срабатываний delta детектора
- `signal_count`: количество сгенерированных сигналов

### Логирование

**Уровни логирования:**
- `DEBUG`: подробная информация о каждом тике (если `DEBUG_DELTAS=true`)
- `INFO`: генерация сигналов, инициализация компонентов
- `WARNING`: некритические ошибки, валидация данных
- `ERROR`: критические ошибки обработки

**Структурированные логи:**
```json
{
  "timestamp": "2024-01-10T12:00:00.123Z",
  "level": "INFO",
  "logger": "crypto_orderflow_service",
  "symbol": "BTCUSDT",
  "signal_id": "crypto-of:BTCUSDT:1704888000123",
  "confidence": 0.85,
  "reason": "delta_spike",
  "delta": 4.8,
  "delta_z": 2.9
}
```

## Обработка ошибок

### Fail-Open стратегия

1. **Сетевые ошибки Redis**: повторные попытки с экспоненциальной задержкой
2. **Поврежденные данные**: использование безопасных парсеров (`_safe_float`, `_safe_int`)
3. **Отсутствие ATR**: fallback на локальный расчет
4. **Сбои детекторов**: продолжение работы без конкретного детектора

### Graceful degradation

- При недоступности notify Redis: сигналы все равно генерируются, но не отправляются в Telegram
- При сбое confidence scorer: используется дефолтная уверенность
- При ошибке в расчетах уровней: пропуск сигнала с логированием

## Производительность

### Оптимизации

1. **Буферы**: кольцевые буферы ограниченного размера предотвращают OOM
2. **Пакетная обработка**: чтение множественных сообщений за раз
3. **Кеширование ATR**: 15-секундный кеш для снижения нагрузки на Redis
4. **Async операции**: все I/O операции асинхронные

### Масштабирование

- **Горизонтальное**: запуск нескольких инстансов (Consumer Groups автоматически балансируют)
- **Вертикальное**: увеличение ресурсов для высоколиквидных символов
- **По символам**: разделение символов между инстансами

### Бутылочные горлышки

1. **Redis Streams**: `XPENDING` для мониторинга backlog
2. **ATR расчет**: нагрузка на trade_back сервис
3. **Confidence scoring**: CPU intensive операция
4. **Публикация сигналов**: множественные I/O операции

## Тестирование

### Модульные тесты

**Тестируемые компоненты:**
- `_parse_tick_payload`: различные форматы payload
- `_handle_tick`: логика генерации сигналов
- `_calculate_levels`: расчет SL/TP
- Детекторы: `push` методы с mock данными

**Фикстуры:**
- Валидные/поврежденные тики
- Различные конфигурации символов
- Граничные случаи (нулевые объемы, экстремальные цены)

### Интеграционные тесты

**Сценарии:**
- End-to-end: от публикации тика до сигнала
- Многопоточная обработка нескольких символов
- Обработка большого количества тиков (нагрузочное тестирование)
- Failover: отключение Redis, восстановление

### Нагрузочное тестирование

**Метрики производительности:**
- Латентность обработки тика: < 100ms
- Пропускная способность: > 1000 тиков/сек на символ
- Memory usage: < 500MB при 10 символах
- CPU usage: < 80% при пиковой нагрузке

## Troubleshooting

### Распространенные проблемы

1. **Нет сигналов**
   - Проверить пороги детекторов
   - Проверить cooldown
   - Проверить минимальную уверенность

2. **Переполнение pending в Redis**
   - Проверить что XACK вызывается
   - Проверить обработку исключений
   - Увеличить количество инстансов

3. **Задержки обработки**
   - Проверить `XPENDING` по consumer groups
   - Проверить сетевую задержку до Redis
   - Оптимизировать `read_count` и `read_block_ms`

4. **Ошибки ATR**
   - Проверить доступность `ATR:<symbol>:<tf>` в Redis
   - Проверить свежесть данных (lastCloseTime)
   - Включить локальный расчет как fallback

### Debug режим

```bash
export CRYPTO_OF_DEBUG_DELTAS=true
export CRYPTO_OF_LOG_LEVEL=DEBUG
```

В debug режиме логируются:
- Каждый тик с delta расчетом
- Причина фильтрации сигналов
- Детальная информация о confidence scoring
- Время выполнения операций

## Архитектурные решения

### Почему Redis Streams?

1. **Durability**: сообщения не теряются при сбоях
2. **Consumer Groups**: автоматическое распределение нагрузки
3. **Pending tracking**: возможность мониторинга backlog
4. **Exactly-once semantics**: через XACK подтверждения

### Почему асинхронная архитектура?

1. **High concurrency**: тысячи одновременных соединений
2. **Resource efficiency**: низкое потребление памяти на соединение
3. **Scalability**: легкое горизонтальное масштабирование
4. **Fault tolerance**: graceful handling сетевых ошибок

### Почему fail-open?

1. **Reliability**: система продолжает работать при частичных сбоях
2. **User experience**: лучше частичная функциональность чем полный отказ
3. **Debugging**: проблемы не маскируются, но не останавливают систему
4. **Production readiness**: устойчивость к неожиданным условиям

## Заключение

CryptoOrderflowService представляет собой сложную, но надежную систему реального времени для обработки крипто-тиков. Архитектура обеспечивает высокую производительность, отказоустойчивость и масштабируемость при сохранении простоты сопровождения и отладки.
