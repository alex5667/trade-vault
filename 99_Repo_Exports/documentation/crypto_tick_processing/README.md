# Документация по обработке крипто-тиков

Документ описывает всю цепочку обработки криптовалютных тиков: от поступления данных в Redis Streams до генерации сигналов и, при необходимости, постановки ордеров. Материал рассчитан на разработчиков и девопсов, обслуживающих `crypto_orderflow_service`.

## Оглавление

- [1. Введение](#1-введение)
- [2. Контекст системы](#2-контекст-системы)
- [3. Договорённости по данным](#3-договорённости-по-данным)
- [4. Жизненный цикл символа](#4-жизненный-цикл-символа)
- [5. Пайплайн обработки тиков](#5-пайплайн-обработки-тиков)
- [6. Обработка книги заявок](#6-обработка-книги-заявок)
- [7. Детекторы и аналитика](detectors.md)
- [8. Формирование и публикация сигнала](#8-формирование-и-публикация-сигнала)
- [9. Конфигурация и управление параметрами](#9-конфигурация-и-управление-параметрами)
- [10. Надёжность и отказоустойчивость](#10-надёжность-и-отказоустойчивость)
- [11. Производительность и масштабирование](#11-производительность-и-масштабирование)
- [12. Тестирование](#12-тестирование)
- [13. Мониторинг и алертинг](#13-мониторинг-и-алертинг)
- [14. Эксплуатация и чек-листы](#14-эксплуатация-и-чек-листы)
- [15. Troubleshooting](#15-troubleshooting)
- [16. Архитектура сервисов](#16-архитектура-сервисов)
- [17. Приложения](#17-приложения)

## Структура документации

Документация разделена на логические блоки для удобства навигации:

```
documentation/crypto_tick_processing/
├── README.md                    # Общий обзор и архитектура
├── detectors.md                 # Детекторы сигналов (объединенный)
├── services/                    # Документация по сервисам
│   ├── crypto_orderflow_service.md
│   ├── signal_performance_tracker.md
│   ├── trade_monitor_service.md
│   ├── reporting_service.md
│   └── telegram_worker.md
├── components/                  # Вспомогательные компоненты
│   ├── atr_cache.md
│   ├── pnl_calculator.md
│   ├── signal_publisher.md
│   └── sync_signal_publisher.md
├── configuration/               # Конфигурация
│   └── environment_variables.md
├── handlers/                    # Обработчики сигналов
│   └── crypto_orderflow_handler.md
└── monitoring/                  # Мониторинг
    └── metrics.md
```

---

## 1. Введение

- **Цель**: единое, последовательное описание процесса обработки крипто-тиков, пригодное для онбординга и поддержки.
- **Объём**: охватывает микросервис `CryptoOrderflowService`, вспомогательные обработчики, источники и потребители данных.
- **Результат**: разработчик должен понимать, какие компоненты задействованы, как устроены данные, в какой последовательности выполняются шаги, и как реагировать на нестандартные ситуации.

---

## 2. Контекст системы

- **Источник данных**: Binance Futures (USDT-M). WebSocket-коннектор пишет сырые тики и стаканы в Redis Streams.
- **Основной сервис**: `CryptoOrderflowService` — асинхронный воркер на `redis.asyncio`, запускает отдельные задачи для тиков и книги на каждый символ.
- **Вспомогательная логика**: `CryptoOrderFlowHandler` — high-level обработчик, интегрируется с общим orderflow-фреймворком (`BaseOrderFlowHandler`).
- **Выходные каналы**:
  - `notify:telegram` — текстовые уведомления для трейдеров/аналитиков (сигналы и отчёты).
  - `signals:orderflow:<symbol>` — структурированные сигналы для аналитики.
  - `signals:audit:<symbol>` — расширенный audit payload с контекстом (OBI, weak progress, env).
  - `stream:manual-signals` — дубликаты сигналов для ручных каналов и автопушки ордеров.
  - `orders:queue` — очередь команд автоторговли (включается конфигурацией).
- **Downstream-сервисы**:
  - `signal_performance_tracker` — читает сигналы, создаёт виртуальные позиции, обновляет статистику в `stats:{strategy}:{symbol}:{tf}`.
  - `ReportingService` и `PeriodicReporter` — формируют периодические отчёты каждое N-е сообщение (по умолчанию каждое 100-е), публикуют их в `notify:telegram` с `type=report`.
  - `telegram-worker` — обрабатывает `notify:telegram`, отправляет сигналы и отчёты в Telegram-бот.
- **Инфраструктура Redis**: два подключения — `main` (операционный Redis) и `ticks` (хранилище стримов тиков/книг).

Сводная схема:

```
[Binance WS] → [Stream Writer] → Redis Streams (tick/book)
                                │
                                └─► CryptoOrderflowService / CryptoOrderFlowHandler
                                        │
                                        ├─► Детекторы (delta, OBI, absorption, iceberg)
                                        │
                                        ├─► Publish сигналов:
                                        │   • notify:telegram (type=signal)
                                        │   • signals:orderflow:<symbol>
                                        │   • signals:audit:<symbol>
                                        │   • stream:manual-signals
                                        │
                                        └─► Downstream:
                                            • signal_performance_tracker → stats:*
                                            • ReportingService → notify:telegram (type=report)
                                            • telegram-worker → Telegram Bot
```

---

## 3. Договорённости по данным

### 3.1 Формат сообщений в стримах

- **Тики** — хранятся в `stream:tick_<symbol>`.
- **Книга** — в `stream:book_<symbol>`.
- Сообщение состоит из полей записи Redis и вложенного JSON в поле `data`. При парсинге они мержатся.

Пример тика:

```json
{
	"id": "1712130845289-0",
	"fields": {
		"symbol": "BTCUSDT",
		"ts": "1712130845289",
		"price": "95643.5",
		"qty": "0.42",
		"side": "BUY",
		"data": "{\"bid\":95643.4,\"ask\":95643.6,\"is_buyer_maker\":false}"
	}
}
```

### 3.2 Нормализованный тик

- Формируется методом `_parse_tick_payload` и содержит:
  - `symbol`, `ts`, `price`, `bid`, `ask`, `mid`
  - `qty` (float), `side` (`BUY` / `SELL`)
  - `is_buyer_maker`, `written_at`
  - производные поля (например, `mid`, вычисленный из `bid/ask`)

**Важно:** Все временные метки (`ts`, `written_at`) хранятся в формате Unix timestamp в миллисекундах (UTC). Это стандарт проекта для всех данных, записываемых в Redis.

### 3.3 Нормализованная книга

- Содержит:
  - `symbol`, `ts`
  - `bids`, `asks` как список уровней `[price, qty]`
  - служебные идентификаторы (`first_id`, `final_id`, `prev_final`)

**Важно:** Поле `ts` в книге также использует Unix timestamp в миллисекундах (UTC).

---

## 4. Жизненный цикл символа

### 4.1 Инициализация и обновление

- Список символов = дефолт (`BTCUSDT`, `ETHUSDT`) + содержимое множества `crypto:symbols`.
- Для каждого символа строится конфигурация (`OrderFlowConfig` + overrides из `config:orderflow:<symbol>`).
- Создаётся/обновляется `SymbolRuntime`: хранит детекторы, буфер тиков, ссылки на потоки.
- Запускаются две асинхронные задачи: `consume_ticks` и `consume_books`.

Код, отвечающий за загрузку символов:

```263:330:python-worker/services/crypto_orderflow_service.py
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
        runtime.tick_stream = tick_stream
        runtime.book_stream = book_stream
        if symbol not in self.symbol_tasks:
            tick_task = asyncio.create_task(self.consume_ticks(symbol))
            book_task = asyncio.create_task(self.consume_books(symbol))
            self.symbol_tasks[symbol] = (tick_task, book_task)
```

### 4.2 Периодический refresh

- Фоновая задача `_refresh_loop` переинициализирует список символов с периодом `CRYPTO_OF_REFRESH_SEC`.
- При удалении символа из множества сервис корректно останавливает связанные задачи и очищает состояние.

---

## 5. Пайплайн обработки тиков

### 5.1 Последовательность шагов

1. **Подготовка consumer group** — `_ensure_group` создаёт группу, при необходимости пересоздаёт.
2. **Чтение партии сообщений** — `XREADGROUP` с параметрами `count` и `block` из конфигурации.
3. **Парсинг** — `_parse_tick_payload` объединяет поля, приводит типы, вычисляет производные значения.
4. **Буферизация** — тик добавляется в `tick_buffer` (FIFO, ограничен `tick_buffer` в конфиге).
5. **Детектирование** — `_handle_tick` запускает цепочку детекторов, собирает подтверждения.
6. **Публикация сигнала** — при успехе вызывается `publish_signal`.
7. **Подтверждение сообщения** — `XACK` гарантирует отсутствие повторной обработки.

### 5.2 Потоковый код

```332:395:python-worker/services/crypto_orderflow_service.py
async def consume_ticks(self, symbol: str) -> None:
    while True:
        runtime = self.symbol_contexts.get(symbol)
        if runtime is None:
            await asyncio.sleep(1)
            continue
        await self._ensure_group(self.ticks, stream, group)
        messages = await self.ticks.xreadgroup(
            groupname=group,
            consumername=self.consumer_id_ticks,
            streams={stream: ">"},
            count=runtime.config.get("read_count", 200),
            block=runtime.config.get("read_block_ms", 1000),
        )
        for stream_name, entries in messages:
            for msg_id, payload in entries:
                try:
                    tick = self._parse_tick_payload(payload)
                    runtime.tick_buffer.append(tick)
                    signal = self._handle_tick(runtime, tick)
                    if signal:
                        await self.publish_signal(runtime, signal)
                except Exception:
                    logger.exception("Ошибка обработки тика")
                finally:
                    await self.ticks.xack(stream_name, group, msg_id)
```

### 5.3 Парсинг тик-пейлоада

```645:688:python-worker/services/crypto_orderflow_service.py
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
    # дополнительные вычисления: mid, is_buyer_maker и т.д.
    return tick
```

### 5.4 Получение ATR для анализа

- Базовый обработчик (`BaseOrderFlowHandler._get_atr`) сначала пытается загрузить волатильность из Redis-хэша `ATR:<SYMBOL>:<TF>`, который в реальном времени обновляется сервисом `trade_back`.
- Таймфрейм нормализуется к формату `M1`, `M5`, `M15` и т.д. (значение берётся из `ATR_TF`, по умолчанию `1m` → `M1`). Полученное значение кэшируется локально на 15 секунд, чтобы снизить нагрузку на Redis.
- Проверяется свежесть данных: поле `lastCloseTime` в хэше сравнивается с текущим временем. Если свеча устарела (возраст > `ATR_REDIS_STALENESS_MULT × длительность таймфрейма`, минимум одна длительность), значение отбрасывается.
- Если трекер вернул валидный ATR, оно используется в дальнейших расчётах (Z-score delta, weak progress, уровни TP/SL). При недоступности данных выполняется fallback:
  1. проверка legacy-ключа `ta:last:atr:<symbol>` (для обратной совместимости);
  2. локальный онлайн-расчёт из тиков через `signals/atr.py`.
- Такое каскадирование делает обработку устойчивой к сбоям в сервисе агрегации свечей и упрощает интеграцию с централизованным ATR-трекером из `trade_back`.

---

## 6. Обработка книги заявок

- Отдельный воркер `consume_books` синхронно дополняет состояние детекторов данными из стакана.
- События OBI и Iceberg сохраняются в `SymbolRuntime` и используются при обработке следующих тиков.

```400:475:python-worker/services/crypto_orderflow_service.py
async def consume_books(self, symbol: str) -> None:
    while True:
        runtime = self.symbol_contexts.get(symbol)
        if runtime is None:
            await asyncio.sleep(1)
            continue
        await self._ensure_group(self.ticks, stream, group)
        messages = await self.ticks.xreadgroup(
            groupname=group,
            consumername=self.consumer_id_books,
            streams={stream: ">"},
            count=runtime.config.get("read_count", 200),
            block=runtime.config.get("read_block_ms", 1000),
        )
        for stream_name, entries in messages:
            for msg_id, payload in entries:
                try:
                    book = self._parse_book_payload(payload, symbol)
                    runtime.last_book = book
                    obi_event = runtime.obi_detector.push(book)
                    iceberg_event = runtime.iceberg_detector.push(book)
                    runtime.last_obi_event = serialize_obi(obi_event)
                    runtime.last_iceberg_event = serialize_iceberg(iceberg_event)
                finally:
                    await self.ticks.xack(stream_name, group, msg_id)
```

---

## 7. Детекторы и аналитика

### 7.1 DeltaSpikeDetector

- Анализирует суммарный агрессивный объём в окне `delta_window`.
- Условия сигнала:
  - значение `|delta|` ≥ `delta_abs_min`
  - z-score ≥ `delta_z_threshold`

### 7.2 OBIDetector

- Оценивает дисбаланс стакана на глубине `obi_depth`.
- Событие хранится с полями `direction`, `obi`, `ts`.

### 7.3 AbsorptionDetector

- Сопоставляет поток тиков с последним стаканом, выявляет "поглощение".
- Триггер при объёме ≥ `absorption_min_volume` и совпадении стороны.

### 7.4 IcebergDetector

- Отслеживает повторные обновления уровня стакана с лимитными заявками.
- Параметры: `iceberg_refresh`, `iceberg_duration`.

### 7.5 Логика объединения

```478:559:python-worker/services/crypto_orderflow_service.py
def _handle_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    delta_event = runtime.delta_detector.push(tick)
    if not delta_event:
        return None
    direction = "LONG" if delta_event["delta"] >= 0 else "SHORT"
    confirmations = collect_confirmations(runtime, direction, tick)
    if not is_confirmed(delta_event, confirmations, runtime.config):
        return None
    if not cooldown_passed(runtime, tick_ts):
        return None
    payload = build_signal_payload(runtime, tick, delta_event, confirmations)
    return payload
```

---

## 8. Формирование и публикация сигнала

### 8.1 Структура сигнала

- Минимальный набор полей:
  - `signal_id`, `symbol`, `direction`, `entry`
  - `delta`, `delta_z`, `confirmations`, `reason`
  - `tick_ts`, `tick_qty`, `indicators`
  - доп. контекст из `last_book` (`best_bid`, `best_ask`, `book_ts`)

Пример:

```json
{
	"signal_id": "crypto-of:BTCUSDT:1712130845301",
	"symbol": "BTCUSDT",
	"direction": "LONG",
	"entry": 95643.5,
	"delta": 4.8,
	"delta_z": 2.9,
	"confirmations": ["obi=0.55", "iceberg_refresh=3"],
	"reason": "obi",
	"tick_ts": 1712130845289,
	"tick_qty": 0.42,
	"book_ts": 1712130845200,
	"best_bid": 95643.4,
	"best_ask": 95643.6
}
```

### 8.2 Каналы публикации

`CryptoOrderFlowHandler` (наследник `BaseOrderFlowHandler`) публикует сигнал сразу в несколько каналов:

1. **`notify:telegram`** — форматированное сообщение для трейдеров/ботов:

   - Формат: Redis Stream с полями `sid`, `symbol`, `side`, `text`, `entry`, `sl`, `tp_levels`, и т.д.
   - Используется `UnifiedSignalFormatter.format_redis_payload()` для формирования.
   - Счётчик `notify:telegram:signal_counter` контролирует частоту отправки.
   - Параметр `CRYPTO_NOTIFY_SIGNAL_EVERY_N` задаёт троттлинг (по умолчанию `1`, т.е. отправляется каждый сигнал; если `>1`, отправляется только каждый `N`-й).
   - `maxlen=500`, `approximate=True`.

2. **`signals:orderflow:<symbol>`** — структурированные данные для аналитики:

   - Формат: JSON payload через `UnifiedSignalFormatter.format_audit_payload()`.
   - Содержит: `sid`, `symbol`, `side`, `entry`, `sl`, `tp_levels`, `lot`, `reason`, `confidence`, `atr`, `indicators`, и т.д.
   - `maxlen=1000`, `approximate=True`.

3. **`signals:audit:<symbol>`** — расширенный audit payload:

   - Включает дополнительный контекст: `obi`, `weak_progress`, `env` (ACCOUNT_DEPOSIT_USD, ACCOUNT_LEVERAGE, RISK_PERCENT).
   - Используется для полного аудита и анализа эффективности сигналов.
   - `maxlen=200000`, `approximate=True`.

4. **`stream:manual-signals`** — дубликаты для ручных каналов (через хук `_after_signal_published`):

   - Публикуется только если `ENABLE_MANUAL_SIGNAL_STREAM=true`.
   - Содержит те же данные, что и основной сигнал, плюс `audit_context` с OBI и weak_progress.
   - Используется для интеграции с ручными торговыми каналами и автопушкой ордеров.
   - `maxlen=2000`, `approximate=True`.

5. **`orders:queue`** — очередь команд автоторговли (опционально):

   - Включается параметром `orders_queue_enabled`.
   - Формат команды:

   ```json
   {
   	"id": "order-{symbol}-{ts}",
   	"sid": "signal-{symbol}-{ts}",
   	"symbol": "BTCUSDT",
   	"type": "market",
   	"direction": "LONG",
   	"source": "crypto_orderflow_service",
   	"reason": "delta_spike"
   }
   ```

6. **Snapshot storage** — `signal:snap:{sid}`:
   - Сохраняется в Redis Key с TTL (по умолчанию 6 часов).
   - Содержит полный snapshot сигнала для быстрого доступа без чтения стримов.

Пример кода публикации из `BaseOrderFlowHandler._publish_signal`:

```802:974:python-worker/handlers/base_orderflow_handler.py
def _publish_signal(self, side: str, context: SignalContext, note: str, emoji: str = "🚨") -> None:
    # Формирование сигнала через UnifiedSignalFormatter
    signal = create_signal(...)

    # Публикация в notify:telegram
    redis_payload = UnifiedSignalFormatter.format_redis_payload(signal)
    self.dual_redis.xadd(self.notify_stream, redis_data, maxlen=500)

    # Публикация в signals:orderflow:<symbol>
    signal_payload = UnifiedSignalFormatter.format_audit_payload(signal, extra_context={...})
    simple_redis.xadd(self.orderflow_signal_stream, {"data": json.dumps(signal_payload)}, maxlen=1000)

    # Сохранение snapshot
    self.redis_client.setex(snap_key, self.snap_ttl, json.dumps(signal_snapshot))

    # Audit stream
    self.redis_client.xadd(self.audit_signal_stream, {"data": json.dumps(audit_payload)}, maxlen=200000)

    # Хук для дополнительной обработки (в CryptoOrderFlowHandler дублирует в stream:manual-signals)
    self._after_signal_published(signal, redis_data, signal_payload)
```

### 8.2.1 Примеры сигналов для каждого стрима

#### Пример 1: `signals:orderflow:BTCUSDT`

**Структура сообщения в Redis Stream:**

```json
{
	"id": "1734567890123-0",
	"fields": {
		"data": "{\"sid\":\"BTCUSDT:LONG:9500000:1734567890123\",\"symbol\":\"BTCUSDT\",\"side\":\"LONG\",\"entry\":95643.5,\"sl\":95143.5,\"tp_levels\":[96143.5,96643.5,97143.5],\"lot\":0.1,\"source\":\"OrderFlow\",\"reason\":\"Delta spike (z=2.9) + OBI=0.55\",\"confidence\":85.0,\"atr\":500.0,\"ts\":1734567890123,\"indicators\":{\"delta\":4.8,\"z_delta\":2.9,\"obi\":0.55,\"weak_progress\":false,\"atr\":500.0,\"delta_window_len\":120},\"metadata\":{\"contract_size\":1.0,\"lot_step\":0.001,\"price_decimals\":2,\"volume_decimals\":3},\"trail_after_tp1\":true,\"trail_profile\":\"rocket_v1\",\"extra_context\":{\"obi\":0.55,\"weak_progress\":false},\"ts_human\":\"2024-12-18T10:30:00.123000+00:00\"}"
	}
}
```

**Распарсенный JSON из поля `data`:**

```json
{
	"sid": "BTCUSDT:LONG:9500000:1734567890123",
	"symbol": "BTCUSDT",
	"side": "LONG",
	"entry": 95643.5,
	"sl": 95143.5,
	"tp_levels": [96143.5, 96643.5, 97143.5],
	"lot": 0.1,
	"source": "OrderFlow",
	"reason": "Delta spike (z=2.9) + OBI=0.55",
	"confidence": 85.0,
	"atr": 500.0,
	"ts": 1734567890123,
	"indicators": {
		"delta": 4.8,
		"z_delta": 2.9,
		"obi": 0.55,
		"weak_progress": false,
		"atr": 500.0,
		"delta_window_len": 120
	},
	"metadata": {
		"contract_size": 1.0,
		"lot_step": 0.001,
		"price_decimals": 2,
		"volume_decimals": 3
	},
	"trail_after_tp1": true,
	"trail_profile": "rocket_v1",
	"extra_context": {
		"obi": 0.55,
		"weak_progress": false
	},
	"ts_human": "2024-12-18T10:30:00.123000+00:00"
}
```

**Примечание:** В `extra_context` содержатся только `obi` и `weak_progress` (без `env`).

---

#### Пример 2: `signals:audit:BTCUSDT`

**Структура сообщения в Redis Stream:**

```json
{
	"id": "1734567890123-0",
	"fields": {
		"data": "{\"sid\":\"BTCUSDT:LONG:9500000:1734567890123\",\"symbol\":\"BTCUSDT\",\"side\":\"LONG\",\"entry\":95643.5,\"sl\":95143.5,\"tp_levels\":[96143.5,96643.5,97143.5],\"lot\":0.1,\"source\":\"OrderFlow\",\"reason\":\"Delta spike (z=2.9) + OBI=0.55\",\"confidence\":85.0,\"atr\":500.0,\"ts\":1734567890123,\"indicators\":{\"delta\":4.8,\"z_delta\":2.9,\"obi\":0.55,\"weak_progress\":false,\"atr\":500.0,\"delta_window_len\":120},\"metadata\":{\"contract_size\":1.0,\"lot_step\":0.001,\"price_decimals\":2,\"volume_decimals\":3},\"trail_after_tp1\":true,\"trail_profile\":\"rocket_v1\",\"extra_context\":{\"obi\":0.55,\"weak_progress\":false,\"env\":{\"ACCOUNT_DEPOSIT_USD\":\"10000\",\"ACCOUNT_LEVERAGE\":\"100\",\"RISK_PERCENT\":\"5.0\"}},\"ts_human\":\"2024-12-18T10:30:00.123000+00:00\"}"
	}
}
```

**Распарсенный JSON из поля `data`:**

```json
{
	"sid": "BTCUSDT:LONG:9500000:1734567890123",
	"symbol": "BTCUSDT",
	"side": "LONG",
	"entry": 95643.5,
	"sl": 95143.5,
	"tp_levels": [96143.5, 96643.5, 97143.5],
	"lot": 0.1,
	"source": "OrderFlow",
	"reason": "Delta spike (z=2.9) + OBI=0.55",
	"confidence": 85.0,
	"atr": 500.0,
	"ts": 1734567890123,
	"indicators": {
		"delta": 4.8,
		"z_delta": 2.9,
		"obi": 0.55,
		"weak_progress": false,
		"atr": 500.0,
		"delta_window_len": 120
	},
	"metadata": {
		"contract_size": 1.0,
		"lot_step": 0.001,
		"price_decimals": 2,
		"volume_decimals": 3
	},
	"trail_after_tp1": true,
	"trail_profile": "rocket_v1",
	"extra_context": {
		"obi": 0.55,
		"weak_progress": false,
		"env": {
			"ACCOUNT_DEPOSIT_USD": "10000",
			"ACCOUNT_LEVERAGE": "100",
			"RISK_PERCENT": "5.0"
		}
	},
	"ts_human": "2024-12-18T10:30:00.123000+00:00"
}
```

**Примечание:** Отличие от `signals:orderflow` — в `extra_context` добавлено поле `env` с переменными окружения (ACCOUNT_DEPOSIT_USD, ACCOUNT_LEVERAGE, RISK_PERCENT).

---

#### Пример 3: `stream:manual-signals`

**Структура сообщения в Redis Stream:**

```json
{
	"id": "1734567890123-0",
	"fields": {
		"data": "{\"sid\":\"BTCUSDT:LONG:9500000:1734567890123\",\"ts\":1734567890123,\"symbol\":\"BTCUSDT\",\"side\":\"LONG\",\"entry\":95643.5,\"sl\":95143.5,\"tp_levels\":[96143.5,96643.5,97143.5],\"lot\":0.1,\"reason\":\"Delta spike (z=2.9) + OBI=0.55\",\"source\":\"crypto-orderflow\",\"confidence\":85.0,\"atr\":500.0,\"trail_after_tp1\":true,\"trail_profile\":\"rocket_v1\",\"indicators\":{\"delta\":4.8,\"z_delta\":2.9,\"obi\":0.55,\"weak_progress\":false,\"atr\":500.0,\"delta_window_len\":120},\"metadata\":{\"contract_size\":1.0,\"lot_step\":0.001,\"price_decimals\":2,\"volume_decimals\":3},\"audit_context\":{\"obi\":0.55,\"weak_progress\":false}}"
	}
}
```

**Распарсенный JSON из поля `data`:**

```json
{
	"sid": "BTCUSDT:LONG:9500000:1734567890123",
	"ts": 1734567890123,
	"symbol": "BTCUSDT",
	"side": "LONG",
	"entry": 95643.5,
	"sl": 95143.5,
	"tp_levels": [96143.5, 96643.5, 97143.5],
	"lot": 0.1,
	"reason": "Delta spike (z=2.9) + OBI=0.55",
	"source": "crypto-orderflow",
	"confidence": 85.0,
	"atr": 500.0,
	"trail_after_tp1": true,
	"trail_profile": "rocket_v1",
	"indicators": {
		"delta": 4.8,
		"z_delta": 2.9,
		"obi": 0.55,
		"weak_progress": false,
		"atr": 500.0,
		"delta_window_len": 120
	},
	"metadata": {
		"contract_size": 1.0,
		"lot_step": 0.001,
		"price_decimals": 2,
		"volume_decimals": 3
	},
	"audit_context": {
		"obi": 0.55,
		"weak_progress": false
	}
}
```

**Примечание:**

- Поле `source` всегда равно `"crypto-orderflow"`.
- Вместо `extra_context` используется `audit_context` с полями `obi` и `weak_progress`.
- Отсутствует поле `ts_human` (только `ts` в миллисекундах).
- Публикуется только для криптовалют через хук `_after_signal_published` в `CryptoOrderFlowHandler`.

---

### 8.3 Поток обработки сигналов и формирования отчётов

После публикации сигнала запускается цепочка downstream-обработки:

#### 8.3.1 Signal Performance Tracker

**Назначение**: отслеживает эффективность сигналов, создаёт виртуальные позиции, обновляет статистику.

**Процесс**:

1. Читает сигналы из `signals:orderflow:*` и `signals:audit:*` через consumer group.
2. Создаёт виртуальную позицию в `TradeMonitor` для каждого сигнала.
3. Отслеживает тики из `stream:tick_*` для обновления P&L позиций.
4. Обрабатывает события из `events:trades` (TP1_HIT, SL_HIT, TRAILING_STARTED).
5. Обновляет статистику в Redis Hash `stats:{strategy}:{symbol}:{tf}`:
   - `total_trades`, `wins`, `losses`, `winrate`
   - `tp1_hits`, `tp2_hits`, `tp3_hits`
   - `tp1_then_sl`, `tp2_then_sl`, `tp3_then_sl` (упущенная прибыль)
   - `total_pnl`, `avg_pnl`

**Конфигурация**:

- `TRACKER_SYMBOLS` — список отслеживаемых символов
- `STRATEGY_WHITELIST` — фильтр стратегий
- `REPORT_TRIGGER_COUNT` — количество сигналов/сделок для отправки отчёта (по умолчанию 100)

#### 8.3.2 ReportingService

**Назначение**: формирует агрегированные отчёты и отправляет их в Telegram.

**Процесс**:

1. Отчёты отправляются автоматически каждое N-е сообщение/сигнал (настраивается через `REPORT_TRIGGER_COUNT`, по умолчанию каждое 100-е).
2. `PeriodicReporter` отслеживает счётчик обработанных сигналов и при достижении порога вызывает `ReportingService`.
3. Собирает статистику через `StatsAggregator`:
   - Общая сводка по всем стратегиям
   - Детализация по каждой стратегии и символу
   - Разбивка по источникам сигналов (OrderFlow, AggregatedHub-V2, и т.д.)
4. Формирует HTML-отчёт с метриками:
   - Общие показатели (сделок, winrate, P/L)
   - TP метрики (TP1/TP2/TP3 hit rates)
   - Упущенная прибыль (TP→SL статистика)
   - Разбивка по источникам
5. Публикует отчёт в `notify:telegram` с полем `type=report`:

   ```json
   {
   	"type": "report",
   	"text": "<b>Периодическая сводка</b>\n...",
   	"source": "ReportingService",
   	"timestamp": "1731149405123"
   }
   ```

**Методы**:

- `send_periodic_report()` — периодический отчёт, вызывается `PeriodicReporter` при достижении порога `REPORT_TRIGGER_COUNT`
- `send_strategy_report(strategy, symbol, tf)` — детальный отчёт по стратегии
- `notify_periodic_summary(stats, period)` — гибкая периодическая сводка

**Примечание**: Отчёты отправляются автоматически каждое N-е сообщение (по умолчанию каждое 100-е) через `PeriodicReporter`, который отслеживает счётчик обработанных сигналов и вызывает `ReportingService` при достижении порога.

#### 8.3.3 Telegram Worker

**Назначение**: обрабатывает сообщения из `notify:telegram` и отправляет их в Telegram-бот.

**Процесс**:

1. Подписывается на `notify:telegram` через consumer group `notify-group`.
2. Читает сообщения и определяет тип:
   - **`type=signal`** — торговый сигнал:
     - Парсит поля (`symbol`, `side`, `entry`, `sl`, `tp_levels`, и т.д.)
     - Форматирует сообщение через `notify_parsed_signal()`
     - Отправляет в Telegram-канал
   - **`type=report`** — отчёт:
     - Извлекает HTML-текст из поля `text`
     - Отправляет напрямую через `send_html_to_telegram()`
     - Обновляет Grafana annotations (если настроено)
3. Подтверждает обработку через `XACK`.
4. При ошибках выполняет повторные попытки с экспоненциальным backoff.

**Конфигурация**:

- `TELEGRAM_BOT_TOKEN` — токен бота
- `TELEGRAM_CHAT_ID` — ID канала/чата
- `NOTIFY_GROUP` — имя consumer group (по умолчанию `notify-group`)
- `NOTIFY_MAX_RETRIES` — максимальное количество повторов (по умолчанию 5)

#### 8.3.4 Полная последовательность

```
1. CryptoOrderFlowHandler публикует сигнал:
   ├─► notify:telegram (type=signal)
   ├─► signals:orderflow:<symbol>
   ├─► signals:audit:<symbol>
   └─► stream:manual-signals

2. Signal Performance Tracker:
   ├─► Читает signals:orderflow:* и signals:audit:*
   ├─► Создаёт виртуальную позицию
   ├─► Отслеживает тики и события
   └─► Обновляет stats:{strategy}:{symbol}:{tf}

3. PeriodicReporter (каждое N-е сообщение):
   ├─► Отслеживает счётчик сигналов
   ├─► При достижении порога вызывает ReportingService
   ├─► Собирает статистику через StatsAggregator
   ├─► Формирует HTML-отчёт
   └─► Публикует в notify:telegram (type=report)

4. Telegram Worker:
   ├─► Читает notify:telegram
   ├─► Определяет тип (signal/report)
   └─► Отправляет в Telegram-бот
```

**Метрики и мониторинг**:

- `signals_processed_total` — количество обработанных сигналов
- `stats_report_latency_ms` — задержка формирования отчёта
- `reports_published_total` — количество опубликованных отчётов
- `telegram_send_errors_total` — ошибки отправки в Telegram
- Lag consumer групп (`XPENDING`) для всех стримов

---

## 9. Конфигурация и управление параметрами

### 9.1 Источники конфигурации

- `DEFAULT_CONFIG` внутри сервиса (значения по умолчанию).
- `OrderFlowConfig` из `core.instrument_config` — специфика инструмента.
- Overrides в Redis Hash `config:orderflow:<symbol>`.

### 9.2 Ключевые параметры

| Параметр               | Назначение                   | Значение по умолчанию | Примечания                   |
| ---------------------- | ---------------------------- | --------------------- | ---------------------------- |
| `delta_window`         | окно для z-score             | 120                   | количество тиков             |
| `delta_z_threshold`    | порог z-score                | 2.5                   | чем выше, тем реже сигналы   |
| `delta_abs_min`        | мин. абсолютный объём        | 0.75                  | в контрактах                 |
| `min_confirmations`    | минимум подтверждений        | 1                     | OBI/Iceberg/Absorption       |
| `tick_buffer`          | размер буфера тиков          | 300                   | FIFO                         |
| `read_count`           | батч XREADGROUP              | 200                   | можно снижать при низком QPS |
| `read_block_ms`        | таймаут XREADGROUP           | 1000                  | миллисекунды                 |
| `signal_cooldown_sec`  | задержка между сигналами     | 45                    | на символ                    |
| `orders_queue_enabled` | публикация в очередь ордеров | False                 | переключается на лету        |

### 9.3 Переменные окружения

| Переменная                       | Назначение                                      | Пример                           |
| -------------------------------- | ----------------------------------------------- | -------------------------------- |
| `CRYPTO_OF_REFRESH_SEC`          | период обновления символов                      | `30`                             |
| `CRYPTO_OF_LOG_LEVEL`            | уровень логгера                                 | `INFO`                           |
| `CRYPTO_NOTIFY_STREAM`           | стрим уведомлений                               | `notify:telegram`                |
| `CRYPTO_RAW_STREAM`              | стрим сигналов (legacy)                         | `signals:crypto:raw`             |
| `ORDERS_QUEUE`                   | очередь ордеров                                 | `orders:queue`                   |
| `REDIS_URL` / `REDIS_TICKS_URL`  | DSN Redis                                       | `redis://localhost:6379/0`       |
| `ATR_SOURCE`                     | источник ATR (`ticks` / `redis` / `auto`)       | `auto`                           |
| `ATR_TF`                         | таймфрейм ATR, нормализуется в `M1/M5/...`      | `1m`                             |
| `ATR_REDIS_STALENESS_MULT`       | множитель допустимой «старости» `lastCloseTime` | `3`                              |
| `MANUAL_SIGNAL_STREAM`           | стрим для ручных каналов                        | `stream:manual-signals`          |
| `ENABLE_MANUAL_SIGNAL_STREAM`    | включить дублирование в manual stream           | `true`                           |
| `ORDERFLOW_SIGNAL_STREAM`        | стрим сигналов для аналитики                    | `signals:orderflow:<symbol>`     |
| `SIGNAL_AUDIT_STREAM`            | стрим audit payload                             | `signals:audit:<symbol>`         |
| `NOTIFY_SIGNAL_COUNTER_KEY`      | ключ счётчика для контроля частоты              | `notify:telegram:signal_counter` |
| `CRYPTO_NOTIFY_SIGNAL_EVERY_N`          | отправлять только каждый N-й сигнал             | `1` (все сигналы)                |
| `TRACKER_SYMBOLS`                | список символов для отслеживания                | `BTCUSDT,ETHUSDT`                |
| `STRATEGY_WHITELIST`             | фильтр стратегий                                | `cryptoorderflow`                |
| `REPORT_TRIGGER_COUNT`           | количество сигналов/сделок для отправки отчёта  | `100`                            |
| `PERIODIC_REPORT_WINDOW_SECONDS` | окно времени для сбора статистики (секунды)     | `3600` (1 час)                   |
| `PERIODIC_REPORT_RECENT_LIMIT`   | максимальное количество записей для анализа     | `500`                            |
| `TELEGRAM_BOT_TOKEN`             | токен Telegram-бота                             | (из секретов)                    |
| `TELEGRAM_CHAT_ID`               | ID Telegram-канала/чата                         | (из секретов)                    |
| `NOTIFY_GROUP`                   | consumer group для notify:telegram              | `notify-group`                   |
| `NOTIFY_MAX_RETRIES`             | максимальное количество повторов                | `5`                              |

> **Примечание.** При указании `ATR_SOURCE=redis` рекомендуется удостовериться, что в Redis присутствуют ключи `ATR:<symbol>:<tf>`, иначе обработчик перейдёт к fallback-режиму и расчёт из тиков.

---

## 10. Надёжность и отказоустойчивость

### 10.1 Управление соединениями Redis

- **Singleton pattern с connection pool**: Функция `get_redis()` (Python) и `timeutil.GetCurrentTimestampMs()` (Go) используют singleton pattern для переиспользования соединений.
  - При первом вызове создаётся connection pool с настройками:
    - `max_connections=100` — максимальный размер пула
    - `socket_keepalive=True` — поддержание соединения
    - `health_check_interval=30` — проверка здоровья каждые 30 секунд
  - Последующие вызовы переиспользуют существующее соединение через pool.
  - Автоматическое переподключение при разрыве соединения (проверка через `ping()`).
  - Thread-safe создание соединения с использованием блокировок.
- **Преимущества**:
  - Устранение множественных переподключений при инициализации сервисов.
  - Снижение нагрузки на Redis за счёт переиспользования соединений.
  - Автоматическое восстановление при сетевых сбоях.

### 10.2 Обработка ошибок Redis

- `ResponseError` с `NOGROUP` → пересоздание группы и повтор чтения.
- Временные сетевые ошибки → логирование + краткий `sleep`.
- `xack` всегда вызывается в `finally`, чтобы pending-список не раздувался.

### 10.3 Резервные значения

- `_safe_float` и `_safe_int` гарантируют стабильность даже при некорректных данных.
- `_ensure_list_levels` защищает от повреждённых списков стакана.

### 10.4 Контроль состояния

- `tick_buffer` ограничивает память и сохраняет историю при обновлении конфига.
- `last_signal_ts` хранится внутри `SymbolRuntime` для контроля cooldown.

### 10.5 Устойчивость ATR-потока

- Ошибки чтения хэшей `ATR:<symbol>:<tf>` логируются один раз, чтобы избежать спама при временных сбоях Redis.
- Для каждого таймфрейма выполняется проверка свежести `lastCloseTime`; устаревшие значения отбрасываются автоматически, что исключает использование «застывшего» ATR.
- При отсутствии централизованных данных срабатывает fallback на локальный расчёт из тиков, благодаря чему обработчик продолжает работу без вмешательства оператора.

---

## 11. Производительность и масштабирование

- Асинхронные задачи по символам позволяют масштабировать сервис горизонтально (добавлением инстансов).
- Consumer Groups Redis Streams автоматически распределяют сообщения между инстансами.
- Рекомендации:
  - Для высоколиквидных инструментов увеличивать `read_count` и `tick_buffer`.
  - При росте задержек — масштабировать по горизонтали, разделяя символы между воркерами.
  - Следить за временем между `ts` и `written_at` (метрика задержки поставки данных).

---

## 12. Тестирование

### 12.1 Юнит-тесты

- Парсинг тиков (`_parse_tick_payload`) с различными форматами и ошибками.
- Логика `_handle_tick`: сценарии без подтверждений, с одним подтверждением, с нарушением cooldown.
- Проверка `_publish_orders_queue` на корректный payload.

### 12.2 Интеграционные тесты

- Локальный Redis + искусственно наполненные стримы.
- Генерация тиков и книг, проверка состояний детекторов и выходных сообщений.

### 12.3 Нагрузочные тесты

- Использовать генераторы тиков (`documentation/ticks/`) для симуляции реального потока.
- Собирать метрики:
  - среднее время обработки тика;
  - количество сигналов в минуту;
  - размер pending-очередей в Redis.

---

## 13. Мониторинг и алертинг

- **Логи**:
  - `crypto_orderflow_service` фиксирует инициализацию, ошибки чтения, публикации, отклонённые сигналы.
  - `signal_performance_tracker` логирует обработку сигналов, обновление статистики, формирование отчётов.
  - `telegram-worker` логирует отправку сообщений, ошибки доставки, повторные попытки.
- **Метрики**:
  - Pending entries (`XPENDING`) по каждому стриму и группе:
    - `stream:tick_*`, `stream:book_*` — задержки обработки тиков/книг
    - `signals:orderflow:*` — задержки обработки сигналов tracker'ом
    - `notify:telegram` — задержки отправки в Telegram
  - `XLEN` стримов — рост говорит о задержках:
    - `notify:telegram` — должен быть < 100
    - `signals:orderflow:*` — должен быть < 1000
    - `signals:audit:*` — может быть больше (до 200k)
  - Количество сигналов и распределение направлений:
    - `signals:orderflow:*` — общее количество сигналов
    - `stats:*` — статистика по стратегиям и символам
  - Метрики отчётности:
    - `stats_report_latency_ms` — задержка формирования отчёта
    - `reports_published_total` — количество опубликованных отчётов
    - `telegram_send_errors_total` — ошибки отправки в Telegram
- **Алерты**:
  - Ошибка публикации в Telegram/очередь ордеров.
  - Несоответствие количества тиков/книг вход/выход.
  - Частые пересоздания consumer group.
  - Отчёты не публикуются при отсутствии сигналов (проверка счётчика `REPORT_TRIGGER_COUNT`).
  - Lag consumer групп превышает порог (например, > 1000 сообщений).
  - Telegram worker не обрабатывает сообщения (рост `XPENDING` для `notify-group`).

---

## 14. Эксплуатация и чек-листы

### 14.1 Перед запуском

- [ ] Проверить доступность Redis (`ping`).
- [ ] Убедиться, что символы перечислены в `crypto:symbols`.
- [ ] Подтвердить наличие конфигов в `config:orderflow:<symbol>` (при необходимости).
- [ ] Настроить лог уровень (`CRYPTO_OF_LOG_LEVEL`).
- [ ] Протестировать публикацию в `notify:telegram` на тестовом payload.

### 14.2 Рутинная проверка

- [ ] Мониторить `XPENDING` для всех групп:
  - `stream:tick_*`, `stream:book_*` — обработка тиков/книг
  - `signals:orderflow:*` — обработка сигналов tracker'ом
  - `notify:telegram` (группа `notify-group`) — отправка в Telegram
- [ ] Сравнивать количество входных сообщений с количеством обработанных.
- [ ] Проверять свежесть `last_signal_ts` для активных символов.
- [ ] Контролировать задержку между `tick_ts` и фактическим временем обработки.
- [ ] Проверять обновление статистики в `stats:{strategy}:{symbol}:{tf}` после сигналов.
- [ ] Убедиться, что отчёты публикуются каждое N-е сообщение (проверить логи `periodic_reporter` и счётчик `REPORT_TRIGGER_COUNT`).
- [ ] Проверять, что сообщения из `notify:telegram` обрабатываются (lag группы `notify-group` < 100).

### 14.3 Перед добавлением нового символа

- [ ] Добавить символ в `crypto:symbols`.
- [ ] Проверить наличие пресета в `OrderFlowConfig`.
- [ ] Протестировать overrides (если нужны).
- [ ] Рост нагрузки оценить предварительным нагрузочным тестом.

---

## 15. Troubleshooting

| Проблема                      | Симптомы                                                   | Диагностика                                                                                                                  | Решение                                                                                                    |
| ----------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Не создаётся consumer group   | Логи `NOGROUP` повторяются                                 | Проверить права Redis, существование стрима                                                                                  | Создать стрим заранее (`XADD mystream * init 1`)                                                           |
| Отсутствуют сигналы           | `last_signal_ts` не обновляется                            | Проверить `delta_z_threshold`, `min_confirmations`, cooldown                                                                 | Снизить пороги или проверить источники подтверждений                                                       |
| Переполнение pending          | `XPENDING` растёт                                          | Убедиться, что `xack` вызывается успешно                                                                                     | Проверить логи на исключения, рестартовать воркер                                                          |
| Нет данных в Telegram         | Логи `Не удалось опубликовать`                             | Проверить наличие стрима, права, maxlen                                                                                      | Увеличить `maxlen`, проверить подключение                                                                  |
| Очередь ордеров пустая        | `orders_queue_enabled` включен, но сообщений нет           | Проверить payload сигнала, reason                                                                                            | Убедиться, что `tick_ts` присутствует, нет исключений при LPUSH                                            |
| ATR не обновляется            | `atr` в сигнале ≈ оценочному, отсутствуют значения в Redis | Проверить ключ `ATR:<symbol>:<tf>` и свежесть `lastCloseTime`, убедиться что `trade_back` atr-worker активен                 | Восстановить atr-worker, при необходимости временно оставить `ATR_SOURCE=ticks`                            |
| Отчёты не приходят            | Отчёты не появляются в Telegram                            | Проверить `periodic_reporter` работает, `REPORT_TRIGGER_COUNT` настроен, счётчик сигналов обновляется, `stats:*` обновляются | Проверить логи `periodic_reporter`, убедиться что счётчик достигает порога и `ReportingService` вызывается |
| Telegram worker не отправляет | Сообщения накапливаются в `notify:telegram`                | Проверить `XPENDING` для группы `notify-group`, логи `telegram-worker`, токен и chat_id                                      | Проверить подключение к Telegram API, перезапустить worker                                                 |
| Статистика не обновляется     | `stats:*` не меняются после сигналов                       | Проверить что `signal_performance_tracker` читает `signals:orderflow:*`, нет ошибок в логах                                  | Проверить consumer group lag, убедиться что tracker обрабатывает сигналы                                   |
| Дублирование сигналов         | Сигналы появляются дважды в Telegram                       | Проверить `ENABLE_MANUAL_SIGNAL_STREAM` и настройки `notify:telegram:signal_counter`                                         | Отключить дублирование или настроить фильтрацию в telegram-worker                                          |

---

## 16. Детальное описание сервисов

### 16.1 CryptoOrderflowService - Основной сервис обработки тиков

**Расположение**: `python-worker/services/crypto_orderflow_service.py`

**Назначение**: Асинхронный воркер, читающий тики и книги заявок из Redis Streams, применяющий детекторы order flow и публикующий торговые сигналы.

#### Основные переменные и параметры:

**Константы и настройки по умолчанию:**

- `DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT"]` - базовый список символов
- `CRYPTO_OF_REFRESH_SEC: int = 300` - период обновления списка символов (5 минут)
- `CRYPTO_OF_LOG_LEVEL: str = "INFO"` - уровень логирования
- `DEBUG_DELTAS: bool` - флаг подробного логирования дельты (из переменной окружения)
- `ATR_TF: str = "1m"` - таймфрейм для ATR по умолчанию
- `ATR_REDIS_STALENESS_MULT: float = 2.0` - множитель для проверки свежести ATR
- `REPORT_TRIGGER_COUNT: int = 100` - количество сигналов для триггера отчета

**Класс SymbolRuntime:**

```python
@dataclass
class SymbolRuntime:
    symbol: str
    config: Dict[str, Any]
    tick_buffer: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=200))
    last_signal_ts: float = 0.0
    last_book: Optional[Dict[str, Any]] = None

    # Детекторы
    delta_detector: DeltaSpikeDetector
    obi_detector: OBIDetector
    absorption_detector: AbsorptionDetector
    iceberg_detector: IcebergDetector

    # События от детекторов
    last_obi_event: Optional[Dict[str, Any]] = None
    last_iceberg_event: Optional[Dict[str, Any]] = None

    # Ссылки на стримы и задачи
    tick_stream: Optional[str] = None
    book_stream: Optional[str] = None
    tick_task: Optional[asyncio.Task] = None
    book_task: Optional[asyncio.Task] = None
```

**Методы класса CryptoOrderflowService:**

**`__init__(self, redis_dsn: str, ticks_dsn: Optional[str] = None)`**

- Инициализация подключений Redis (main и ticks)
- Создание экземпляров детекторов
- Настройка consumer groups
- Инициализация Prometheus метрик

**`async def run_forever(self) -> None`**

- Основной цикл работы сервиса
- Загрузка динамических символов
- Запуск периодического refresh
- Обработка сигналов завершения

**`async def consume_ticks(self, symbol: str) -> None`**

- Чтение тиков из `stream:tick_<symbol>`
- Парсинг payload и буферизация
- Применение детекторов и генерация сигналов
- XACK подтверждение обработки

**`async def consume_books(self, symbol: str) -> None`**

- Чтение обновлений книги заявок из `stream:book_<symbol>`
- Обновление состояния детекторов OBI и Iceberg
- Сериализация событий для использования в обработке тиков

**`def _handle_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]`**

- Основная логика обработки тика
- Сбор подтверждений от всех детекторов
- Проверка условий генерации сигнала (min_confirmations, cooldown)
- Возврат структурированного сигнала или None

**`async def publish_signal(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None`**

- Публикация сигнала в несколько каналов:
  - `notify:telegram` (type=signal)
  - `signals:orderflow:<symbol>`
  - `signals:audit:<symbol>`
  - `stream:manual-signals` (опционально)
- Управление очередью ордеров (если включено)

#### Детекторы (классы в core/crypto_orderflow_detectors.py):

**DeltaSpikeDetector:**

- `window: int = 60` - размер окна для расчета статистики
- `z_threshold: float = 3.0` - порог z-score
- `min_abs_volume: float = 0.0` - минимальный абсолютный объем
- Метод `classify_tick()`: определяет направление сделки (+ для покупок, - для продаж)
- Метод `push()`: добавляет тик и проверяет условия всплеска

**OBIDetector:**

- `depth: int = 10` - глубина книги для анализа
- `min_obi: float = 0.6` - минимальный дисбаланс
- Метод `push()`: анализирует книгу и возвращает событие OBI

**AbsorptionDetector:**

- `min_volume: float = 1.0` - минимальный объем поглощения
- `max_ticks_behind: int = 5` - максимальное отставание в тиках
- Метод `push()`: ищет поглощение агрессивного потока лимитными заявками

**IcebergDetector:**

- `refresh_threshold: int = 3` - порог обновлений уровня
- `duration_window: int = 30` - временное окно анализа
- Метод `push()`: отслеживает повторные обновления уровня

#### Логика работы:

1. **Инициализация**: Загрузка символов, создание SymbolRuntime для каждого
2. **Чтение тиков**: Consumer group читает из Redis Streams, парсит payload
3. **Буферизация**: Тики добавляются в кольцевой буфер ограниченного размера
4. **Детектирование**: Каждый тик проходит через цепочку детекторов
5. **Генерация сигнала**: При наличии достаточного количества подтверждений создается сигнал
6. **Публикация**: Сигнал отправляется во все настроенные каналы
7. **Подтверждение**: Обработка тика подтверждается через XACK

### 16.2 CryptoOrderFlowHandler - Обработчик сигналов

**Расположение**: `python-worker/handlers/crypto_orderflow_handler.py`

**Назначение**: High-level обработчик сигналов order flow, интегрирующий логику с общим фреймворком сигналов.

#### Основные компоненты:

**Миксины:**

- `CryptoOrderFlowInitMixin` - инициализация и конфигурация
- `CryptoOrderFlowL2StalenessMixin` - проверка свежести данных L2
- `CryptoOrderFlowGenerateMixin` - генерация сигналов
- `CryptoOrderFlowGeometryMixin` - геометрический анализ

**Ключевые методы:**

**`_classify_delta(self, tick: Tick) -> float`**

- Улучшенная классификация дельты для крипты
- Использует `is_buyer_maker` и сравнение с mid для точного определения агрессии

**`_get_source_name(self) -> str`**

- Возвращает `"crypto_orderflow"`

**`_get_strategy_key(self) -> str`**

- Возвращает `"cryptoorderflow"`

**`on_l3_event(self, ev) -> None`**

- Обработка событий L3 order book

**`on_book_update(self, snap) -> None`**

- Обработка обновлений книги заявок

### 16.3 SignalPerformanceTracker - Трекер производительности сигналов

**Расположение**: `python-worker/services/signal_performance_tracker.py`

**Назначение**: Координирует отслеживание эффективности сигналов, обновление статистики и генерацию отчетов.

#### Основные компоненты:

**Класс SignalPerformanceTracker:**

**Переменные состояния:**

- `symbol_runtimes: Dict[str, SymbolRuntime]` - состояние по символам
- `stats_aggregator: StatsAggregator` - агрегатор статистики
- `reporting_service: ReportingService` - сервис отчетов
- `trade_monitor: TradeMonitorService` - монитор позиций

**Потоки выполнения:**

- Поток сигналов: чтение из `signals:orderflow:*`
- Поток тиков: чтение из `stream:tick_*` для обновления P&L
- Поток отчетов: периодическая генерация отчетов

**Методы:**

**`async def run_signal_consumer(self, symbol: str) -> None`**

- Читает сигналы из `signals:orderflow:<symbol>`
- Создает виртуальные позиции в TradeMonitor
- Запускает отслеживание P&L

**`async def run_tick_consumer(self, symbol: str) -> None`**

- Читает тики из `stream:tick_<symbol>`
- Обновляет позиции в реальном времени
- Вычисляет P&L и проверяет условия закрытия

**`async def run_periodic_tasks(self) -> None`**

- Генерирует периодические отчеты
- Очищает устаревшие данные
- Обновляет метрики

### 16.4 TradeMonitorService - Монитор позиций

**Расположение**: `python-worker/services/trade_monitor.py`

**Назначение**: Управляет виртуальными позициями, отслеживает их состояние и рассчитывает P&L.

#### Ключевые компоненты:

**Класс TradeMonitor:**

**Переменные:**

- `positions: Dict[str, Position]` - активные позиции
- `closed_positions: List[Position]` - закрытые позиции
- `symbol_specs: Dict[str, SymbolSpec]` - спецификации символов

**Методы:**

**`create_position(self, signal: Dict[str, Any]) -> Position`**

- Создает виртуальную позицию на основе сигнала
- Устанавливает уровни entry, SL, TP

**`update_pnl(self, symbol: str, tick: Dict[str, Any]) -> None`**

- Обновляет P&L позиции на основе текущих тиков
- Проверяет условия закрытия (TP/SL hit)

**`close_position(self, position_id: str, reason: str) -> None`**

- Закрывает позицию с указанием причины
- Обновляет статистику

### 16.5 ReportingService - Сервис отчетов

**Расположение**: `python-worker/services/reporting_service.py`

**Назначение**: Генерирует агрегированные отчеты по торговым сигналам и отправляет уведомления.

#### Основные методы:

**`generate_report(self, strategy: str, symbol: str, timeframe: str) -> Dict[str, Any]`**

- Формирует отчет по стратегии/символу/таймфрейму
- Включает метрики winrate, P/L, TP hit rates

**`send_telegram_notification(self, message: str, message_type: str = "report") -> bool`**

- Отправляет сообщение в Telegram через Redis stream

**`_aggregate_stats(self, raw_stats: Dict[str, Any]) -> Dict[str, Any]`**

- Агрегирует сырые статистические данные
- Вычисляет производные метрики

### 16.6 Telegram Worker - Обработчик Telegram уведомлений

**Расположение**: `telegram-worker/multithreaded_worker.py`

**Назначение**: Читает сообщения из Redis stream `notify:telegram` и отправляет их в Telegram каналы.

#### Основные компоненты:

**Класс MultithreadedTelegramWorker:**

**Переменные:**

- `message_queue: asyncio.Queue` - очередь сообщений (maxsize=1000)
- `channel_groups: List[ChannelGroup]` - группы каналов для многопоточной обработки
- `stats: Dict[str, Any]` - статистика работы
- `running: bool` - флаг работы воркера

**Методы:**

**`async def start(self) -> None`**

- Инициализация Telethon клиента
- Авторизация и подписка на каналы
- Запуск потоков обработки

**`async def process_message_queue(self) -> None`**

- Основной цикл обработки сообщений
- Чтение из Redis stream `notify:telegram`
- Отправка в соответствующие Telegram каналы

**`async def health_check(self) -> None`**

- Проверка здоровья соединения
- Переподключение при необходимости
- Мониторинг активности

### 16.7 StatsAggregator - Агрегатор статистики

**Расположение**: `python-worker/services/stats_aggregator.py`

**Назначение**: Собирает и агрегирует статистику по сигналам и позициям из Redis.

#### Ключевые методы:

**`get_strategy_stats(self, strategy: str, symbol: str, timeframe: str) -> Dict[str, Any]`**

- Получает статистику по стратегии
- Включает wins, losses, P/L, winrate

**`update_stats(self, signal: Dict[str, Any], result: Dict[str, Any]) -> None`**

- Обновляет статистику после закрытия позиции
- Инкрементирует счетчики wins/losses

### 16.8 PeriodicReporter - Периодический репортер

**Расположение**: `python-worker/services/periodic_reporter.py`

**Назначение**: Генерирует периодические отчеты каждые N сигналов или по расписанию.

#### Переменные:\*\*

- `REPORT_TRIGGER_COUNT: int = 100` - количество сигналов для триггера
- `report_counter: int = 0` - счетчик сигналов

**Методы:**

**`async def check_and_report(self) -> None`**

- Проверяет условия генерации отчета
- Вызывает ReportingService при достижении порога

### 16.9 ATR Cache и связанные компоненты

**Расположение**: `python-worker/utils/atr_cache.py`

**Назначение**: Кеширование и получение значений Average True Range для расчета риск-менеджмента.

#### Класс ATRCache:\*\*

**Переменные:**

- `_cache: Dict[str, Tuple[float, float]]` - кеш ATR значений (значение, timestamp)
- `_cache_ttl: float = 15.0` - время жизни кеша в секундах

**Методы:**

**`async def get_atr(self, symbol: str, tf: str = "1m") -> Optional[float]`**

- Получает ATR из Redis хеша `ATR:<symbol>:<tf>`
- Проверяет свежесть данных
- Возвращает None при отсутствии или устаревании данных

### 16.10 Redis Stream Consumer Helper

**Расположение**: `python-worker/core/redis_stream_consumer.py`

**Назначение**: Утилиты для работы с Redis Streams и consumer groups.

#### Класс AsyncRedisStreamHelper:\*\*

**Методы:**

**`async def ensure_group(self, stream: str, group: str) -> None`**

- Создает consumer group если не существует
- Обрабатывает ошибки NOGROUP

**`async def read_messages(self, stream: str, group: str, consumer: str, count: int = 200, block: int = 1000) -> List[Dict[str, Any]]`**

- Читает сообщения из stream с помощью XREADGROUP
- Возвращает список сообщений

### 16.11 Signal Publisher - Публикатор сигналов

**Расположение**: `python-worker/services/async_signal_publisher.py`

**Назначение**: Асинхронная публикация сигналов в несколько каналов с подтверждением.

#### Класс AsyncSignalPublisher:\*\*

**Переменные:**

- `sinks: List[StreamSink]` - список приемников (Redis streams)
- `max_retries: int = 3` - максимальное количество повторных попыток

**Методы:**

**`async def publish(self, signal: Dict[str, Any]) -> bool`**

- Публикует сигнал во все настроенные приемники
- Обрабатывает ошибки и повторные попытки

### 16.12 PNL Calculator - Калькулятор прибыли/убытков

**Расположение**: `python-worker/services/pnl_math.py`

**Назначение**: Расчет позиционного размера, комиссий и ожидаемой прибыли.

#### Функции:\*\*

**`calculate_position_size(entry_price: float, sl_price: float, risk_amount: float, symbol_info: Dict[str, Any]) -> float`**

- Рассчитывает размер позиции на основе риска и стоп-лосса
- Учитывает спецификации символа (contract size, tick size)

**`calculate_expected_pnl(position_size: float, entry_price: float, exit_price: float, symbol_info: Dict[str, Any]) -> float`**

- Вычисляет ожидаемую прибыль/убыток
- Учитывает комиссии и спред

### 16.13 TP Configuration - Конфигурация тейк-профитов

**Расположение**: `python-worker/services/tp_config.py`

**Назначение**: Управление уровнями тейк-профита и их расчет.

#### Функции:\*\*

**`parse_tp_ratio(tp_config: str) -> List[float]`**

- Парсит конфигурацию TP (например, "1:2:3" -> [1.0, 2.0, 3.0])
- Поддерживает различные форматы

**`calculate_tp_levels(entry_price: float, atr: float, direction: str, tp_ratios: List[float]) -> List[float]`**

- Вычисляет абсолютные уровни TP на основе ATR
- Учитывает направление позиции (LONG/SHORT)

---

## 17. Приложения

### 17.1 Пример end-to-end сценария

**Фаза 1: Генерация сигнала**

1. Binance отправляет трейд `BTCUSDT`.
2. Коннектор пушит запись в `stream:tick_BTCUSDT`.
3. `CryptoOrderFlowHandler.consume_ticks` читает тик, парсит payload, кладёт в буфер.
4. `DeltaSpikeDetector` фиксирует всплеск `delta=4.8`, `z=2.9`.
5. `OBIDetector` сообщает `direction=LONG`, `obi=0.55`.
6. `_handle_tick` собирает подтверждения, проверяет cooldown, формирует сигнал.
7. `_publish_signal` (из `BaseOrderFlowHandler`) публикует сигнал:
   - `notify:telegram` (type=signal) — форматированное сообщение
   - `signals:orderflow:BTCUSDT` — структурированные данные
   - `signals:audit:BTCUSDT` — расширенный audit payload
   - `signal:snap:{sid}` — snapshot для быстрого доступа
8. `_after_signal_published` (хук в `CryptoOrderFlowHandler`) дублирует в `stream:manual-signals`.
9. `_publish_orders_queue` (если включено) создаёт команду на покупку в `orders:queue`.

**Фаза 2: Обработка и отслеживание**

1. `signal_performance_tracker` читает сигнал из `signals:orderflow:BTCUSDT`.
2. Создаёт виртуальную позицию в `TradeMonitor` с `sid`, `symbol`, `side`, `entry`, `sl`, `tp_levels`.
3. Начинает отслеживать тики из `stream:tick_BTCUSDT` для обновления P&L.
4. При достижении TP1 публикует событие `TP1_HIT` в `events:trades`.
5. Обновляет статистику в `stats:cryptoorderflow:BTCUSDT:tick`:
   - Инкрементирует `tp1_hits`
   - Обновляет `total_pnl`, `wins` (если позиция закрыта в прибыль)

**Фаза 3: Формирование отчёта**

1. При достижении порога (каждое 100-е сообщение по умолчанию) `PeriodicReporter` вызывает `ReportingService`.
2. `ReportingService` собирает статистику через `StatsAggregator`:
   - Читает `stats:*` для всех стратегий и символов
   - Агрегирует метрики (winrate, P/L, TP hit rates)
   - Формирует разбивку по источникам сигналов
3. Формирует HTML-отчёт с полными метриками.
4. Публикует отчёт в `notify:telegram` с `type=report`.

**Фаза 4: Доставка в Telegram**

1. `telegram-worker` читает сообщение из `notify:telegram`.
2. Определяет тип: `type=report`.
3. Извлекает HTML-текст и отправляет через `send_html_to_telegram()`.
4. Подтверждает обработку через `XACK`.
5. Пользователь получает отчёт в Telegram-канале.

**Временная шкала**:

- Генерация сигнала: < 100 мс
- Обработка tracker: < 1 с
- Формирование отчёта: < 5 мин (при достижении порога каждое N-е сообщение)
- Доставка в Telegram: < 30 с

### 17.2 Высокоуровневая логика обработчика

```63:123:python-worker/handlers/crypto_orderflow_handler.py
def _classify_delta(self, tick: Tick) -> float:
    if tick.flags & 1:
        if tick.last and tick.bid and tick.ask:
            mid = (tick.bid + tick.ask) / 2
            if tick.last > mid:
                return +tick.volume
            else:
                return -tick.volume
    return super()._classify_delta(tick)
```

- Метод улучшает классификацию delta для крипты, опираясь на агрессивные сделки, что делает сигналы более точные.

### 17.3 Глоссарий

- **Delta Spike** — резкое изменение агрессивного объёма.
- **OBI (Order Book Imbalance)** — дисбаланс объёма на заявках покупки/продажи.
- **Absorption** — поглощение агрессивного потока лимитами.
- **Iceberg** — скрытая лимитная заявка с повторным пополнением объёма.
- **Cooldown** — минимальное время между сигналами для одного символа.

---

**Последнее обновление: 2026-01-21**

**Изменения:**

- Добавлена информация о singleton pattern для Redis соединений (раздел 10.1).
- Уточнено, что все временные метки используют UTC формат (раздел 3.2, 3.3).
- Обновлена логика отправки отчётов: теперь отчёты отправляются каждое N-е сообщение (по умолчанию каждое 100-е) через `PeriodicReporter`, вместо расписания по времени (разделы 8.3.1, 8.3.2, 9.3).
- Добавлен подробный раздел 16 "Детальное описание сервисов" с описанием всех компонентов системы, их переменных, методов и логики работы.
- Реструктурирована документация: объединены детекторы в единый файл `detectors.md`, общее количество файлов сокращено до 10 для удобства навигации.

Документ обновляется при каждом изменении пайплайна, детекторов или конфигурационных параметров. При добавлении новых источников/выходов необходимо расширять соответствующие разделы и примеры.
