# CryptoOrderFlowHandler: Полная документация потоков сигналов

## Содержание

1. [Обзор](#обзор)
2. [Стримы публикации сигналов](#стримы-публикации-сигналов)
3. [Структура payload](#структура-payload)
4. [Сервисы-потребители](#сервисы-потребители)
5. [Consumer Groups](#consumer-groups)
6. [Полная схема потока данных](#полная-схема-потока-данных)
7. [Конфигурация](#конфигурация)
8. [Диагностика и мониторинг](#диагностика-и-мониторинг)
9. [Примеры использования](#примеры-использования)

---

## Обзор

`CryptoOrderFlowHandler` — специализированный обработчик Order Flow для криптовалют (BTC, ETH, BNB и т.д.), который наследует базовую логику от `BaseOrderFlowHandler` и переопределяет специфичные для крипты параметры.

### Основные отличия от Forex обработчика

- **Классификация Delta**: Использует анализ trades вместо bid/ask
- **Оценка ATR**: Более волатильные значения (0.15-0.70% от цены)
- **Default HLC**: Рассчитывается на основе типичной дневной волатильности крипты (~4%)
- **Дополнительные сигналы**: Ловит моментные OBI spikes (≥0.7)

### Жизненный цикл сигнала

```
Тик → Обработка → Генерация сигнала → Публикация в 4 стрима → Downstream обработка
```

---

## Стримы публикации сигналов

`CryptoOrderFlowHandler` публикует каждый сгенерированный сигнал в **4 Redis Streams** одновременно:

### 1. `notify:telegram` — Уведомления для Telegram

**Назначение**: Форматированные сообщения для отправки трейдерам через Telegram-бот.

**Параметры стрима**:

- `maxlen`: 500
- `approximate`: True
- **Троттлинг**: Контролируется через `CRYPTO_NOTIFY_SIGNAL_EVERY_N` и счетчик `notify:telegram:signal_counter`

**Формат публикации**:

```python
# Из BaseOrderFlowHandler._publish_signal()
redis_payload = UnifiedSignalFormatter.format_redis_payload(signal)
redis_data = {}
for key, value in redis_payload.items():
    if isinstance(value, (dict, list)):
        redis_data[key] = json.dumps(value)
    else:
        redis_data[key] = str(value)

# Проверка троттлинга
counter_value = self.dual_redis.incr(self.notify_signal_counter_key)
if counter_value % self.notify_signal_every_n != 0:
    send_to_notify = False

if send_to_notify:
    self.dual_redis.xadd(
        self.notify_stream,  # notify:telegram
        redis_data,
        maxlen=500,
        approximate=True
    )
```

**Структура payload**:

```json
{
 "sid": "BTCUSD:LONG:9500000:1734567890123",
 "symbol": "BTCUSD",
 "side": "LONG",
 "entry": "95000.00",
 "sl": "94500.00",
 "tp_levels": "96000.00,97000.00,98000.00",
 "lot": "0.10",
 "source": "OrderFlow",
 "reason": "Absorption (weak progress + delta spike)",
 "confidence": "85.0",
 "atr": "500.00",
 "ts": "1734567890123",
 "trail_after_tp1": "true",
 "trail_profile": "rocket_v1",
 "text": "🚨 BTCUSD LONG\nEntry: 95000.00\nSL: 94500.00\nTP: 96000.00, 97000.00, 98000.00\n...",
 "ind_z_delta": "4.5",
 "ind_obi": "0.65",
 "ind_weak_progress": "true",
 "ind_atr": "500.00",
 "ind_delta_window_len": "300"
}
```

**Потребитель**: `telegram-worker/notify_worker.py`

---

### 2. `signals:orderflow:{symbol}` — Структурированные данные для аналитики

**Назначение**: Структурированные данные сигнала для дальнейшей обработки и агрегации.

**Параметры стрима**:

- `maxlen`: 1000
- `approximate`: True
- **Формат**: JSON в поле `data`

**Формат публикации**:

```python
# Из BaseOrderFlowHandler._publish_signal()
signal_payload = UnifiedSignalFormatter.format_audit_payload(
    signal,
    extra_context={
        "obi": context.obi,
        "weak_progress": context.weak_progress
    }
)

simple_redis.xadd(
    self.orderflow_signal_stream,  # signals:orderflow:BTCUSD
    {"data": json.dumps(signal_payload)},
    maxlen=1000,
    approximate=True
)
```

**Структура payload**:

```json
{
  "data": "{
    \"sid\": \"BTCUSD:LONG:9500000:1734567890123\",
    \"symbol\": \"BTCUSD\",
    \"side\": \"LONG\",
    \"entry\": 95000.00,
    \"sl\": 94500.00,
    \"tp_levels\": [96000.00, 97000.00, 98000.00],
    \"lot\": 0.10,
    \"source\": \"OrderFlow\",
    \"reason\": \"Absorption (weak progress + delta spike)\",
    \"confidence\": 85.0,
    \"atr\": 500.00,
    \"ts\": 1734567890123,
    \"indicators\": {
      \"z_delta\": 4.5,
      \"obi\": 0.65,
      \"weak_progress\": true,
      \"atr\": 500.00,
      \"delta_window_len\": 300
    },
    \"trail_after_tp1\": true,
    \"trail_profile\": \"rocket_v1\",
    \"extra_context\": {
      \"obi\": 0.65,
      \"weak_progress\": true
    }
  }"
}
```

**Потребители**:

- `AggregatedSignalHubV2` — агрегация сигналов
- `SignalPerformanceTracker` — отслеживание эффективности

---

### 3. `signals:audit:{symbol}` — Расширенный audit payload

**Назначение**: Полный audit trail с дополнительным контекстом для анализа эффективности сигналов.

**Параметры стрима**:

- `maxlen`: 200000 (большой размер для долгосрочного хранения)
- `approximate`: True
- **Формат**: JSON в поле `data`

**Формат публикации**:

```python
# Из BaseOrderFlowHandler._publish_signal()
audit_env = {
    "ACCOUNT_DEPOSIT_USD": os.getenv("ACCOUNT_DEPOSIT_USD", ""),
    "ACCOUNT_LEVERAGE": os.getenv("ACCOUNT_LEVERAGE", ""),
    "RISK_PERCENT": os.getenv("RISK_PERCENT", ""),
}

audit_payload = UnifiedSignalFormatter.format_audit_payload(
    signal,
    extra_context={
        "obi": context.obi,
        "weak_progress": context.weak_progress,
        "env": audit_env
    }
)

self.redis_client.xadd(
    self.audit_signal_stream,  # signals:audit:BTCUSD
    {"data": json.dumps(audit_payload)},
    maxlen=200000,
    approximate=True
)
```

**Структура payload**:

```json
{
  "data": "{
    \"sid\": \"BTCUSD:LONG:9500000:1734567890123\",
    \"symbol\": \"BTCUSD\",
    \"side\": \"LONG\",
    \"entry\": 95000.00,
    \"sl\": 94500.00,
    \"tp_levels\": [96000.00, 97000.00, 98000.00],
    \"lot\": 0.10,
    \"source\": \"OrderFlow\",
    \"reason\": \"Absorption (weak progress + delta spike)\",
    \"confidence\": 85.0,
    \"atr\": 500.00,
    \"ts\": 1734567890123,
    \"indicators\": {...},
    \"trail_after_tp1\": true,
    \"trail_profile\": \"rocket_v1\",
    \"extra_context\": {
      \"obi\": 0.65,
      \"weak_progress\": true,
      \"env\": {
        \"ACCOUNT_DEPOSIT_USD\": \"10000\",
        \"ACCOUNT_LEVERAGE\": \"100\",
        \"RISK_PERCENT\": \"5.0\"
      }
    }
  }"
}
```

**Потребители**: Аналитические сервисы, системы отчетности, долгосрочный анализ эффективности.

---

### 4. `stream:manual-signals` — Дубликаты для ручных каналов

**Назначение**: Дубликат сигналов специально для интеграции с ручными торговыми каналами и автопушкой ордеров.

**Особенности**:

- Публикуется **только для криптовалют** через хук `_after_signal_published()`
- Включается через `ENABLE_MANUAL_SIGNAL_STREAM=true`
- Содержит дополнительный `audit_context` с OBI и weak_progress

**Параметры стрима**:

- `maxlen`: 2000
- `approximate`: True
- **Формат**: JSON в поле `data`

**Формат публикации**:

```python
# Из CryptoOrderFlowHandler._after_signal_published()
if not self.manual_signal_enabled or not self.manual_signal_stream:
    return

manual_payload = {
    "sid": signal.sid,
    "ts": signal.ts,
    "symbol": signal.symbol,
    "side": signal.side,
    "entry": signal.entry,
    "sl": signal.sl,
    "tp_levels": signal.tp_levels,
    "lot": signal.lot,
    "reason": signal.reason,
    "source": "crypto-orderflow",
    "confidence": signal.confidence,
    "atr": signal.atr,
    "trail_after_tp1": signal.trail_after_tp1,
    "trail_profile": signal.trail_profile,
    "indicators": signal.indicators,
}

if signal.metadata:
    manual_payload["metadata"] = signal.metadata

# Добавляем базовый audit контекст для downstream обработки
manual_payload["audit_context"] = {
    "obi": audit_payload.get("extra_context", {}).get("obi"),
    "weak_progress": audit_payload.get("extra_context", {}).get("weak_progress"),
}

self.dual_redis.xadd(
    self.manual_signal_stream,  # stream:manual-signals
    {"data": json.dumps(manual_payload)},
    maxlen=2000,
    approximate=True
)
```

**Структура payload**:

```json
{
  "data": "{
    \"sid\": \"BTCUSD:LONG:9500000:1734567890123\",
    \"ts\": 1734567890123,
    \"symbol\": \"BTCUSD\",
    \"side\": \"LONG\",
    \"entry\": 95000.00,
    \"sl\": 94500.00,
    \"tp_levels\": [96000.00, 97000.00, 98000.00],
    \"lot\": 0.10,
    \"reason\": \"Absorption (weak progress + delta spike)\",
    \"source\": \"crypto-orderflow\",
    \"confidence\": 85.0,
    \"atr\": 500.00,
    \"trail_after_tp1\": true,
    \"trail_profile\": \"rocket_v1\",
    \"indicators\": {
      \"z_delta\": 4.5,
      \"obi\": 0.65,
      \"weak_progress\": true,
      \"atr\": 500.00,
      \"delta_window_len\": 300
    },
    \"metadata\": {
      \"contract_size\": 1.0,
      \"lot_step\": 0.001,
      \"price_decimals\": 2,
      \"volume_decimals\": 3
    },
    \"audit_context\": {
      \"obi\": 0.65,
      \"weak_progress\": true
    }
  }"
}
```

**Потребитель**: `AggregatedSignalHubV2` (consumer group: `hub-v2-crypto`)

**Примечание**: Автопубликация в `orders:auto-push` закомментирована до запуска:

```python
# ⚙️ Автопубликация в поток автогенерации ордеров (оставлено закомментированным до запуска)
# auto_order_payload = UnifiedSignalFormatter.format_order_push_payload(signal)
# self.dual_redis.xadd(
#     self.auto_order_stream,
#     {"data": json.dumps(auto_order_payload)},
#     maxlen=5000,
#     approximate=True
# )
```

---

### 5. `signal:snap:{sid}` — Snapshot для быстрого доступа

**Назначение**: Сохранение полного snapshot сигнала в Redis Key для быстрого доступа без чтения стримов.

**Параметры**:

- **TTL**: По умолчанию 6 часов (21600 секунд)
- **Ключ**: `signal:snap:{sid}`
- **Формат**: JSON

**Формат сохранения**:

```python
# Из BaseOrderFlowHandler._publish_signal()
snap_key = self.snap_prefix + signal.sid  # signal:snap:{sid}
signal_snapshot = redis_data.copy()
signal_snapshot['trail_after_tp1'] = str(signal.trail_after_tp1).lower()
signal_snapshot['trail_profile'] = signal.trail_profile

self.redis_client.setex(
    snap_key,
    self.snap_ttl,  # 21600 секунд (6 часов)
    json.dumps(signal_snapshot)
)
```

**Использование**: `TP1TrailingOrchestrator` читает snapshot для управления трейлингом после TP1.

---

## Структура payload

### Signal Object (внутренний формат)

```python
@dataclass
class Signal:
    sid: str                        # Уникальный ID сигнала
    symbol: str                     # Символ инструмента
    side: str                       # LONG | SHORT
    entry: float                    # Цена входа
    sl: float                       # Stop Loss
    tp_levels: List[float]          # Take Profit уровни
    lot: float                      # Размер позиции
    source: str                     # Источник (OrderFlow, TA, etc)
    reason: str                     # Причина/описание
    confidence: float               # Уверенность (0-100)
    atr: float                      # Текущий ATR
    ts: int                         # Timestamp (milliseconds)
    indicators: Dict[str, Any]      # Дополнительные индикаторы
    metadata: Optional[Dict]        # Метаданные (symbol specs)
    trail_after_tp1: bool           # Включить трейлинг после TP1
    trail_profile: str              # Профиль трейлинга
```

### Indicators (внутри Signal)

```python
indicators = {
    "z_delta": float,              # Z-score Delta (обычно -5.0 до +5.0)
    "obi": float,                   # Order Book Imbalance (-1.0 до +1.0)
    "weak_progress": bool,          # Слабое движение цены
    "atr": float,                   # Текущий ATR
    "delta_window_len": int         # Размер окна Delta
}
```

### Профили трейлинга

Выбор профиля зависит от силы сигнала (z_delta):

| z_delta | trail_after_tp1 | trail_profile    | Описание                |
| ------- | --------------- | ---------------- | ----------------------- |
| ≥ 6.0   | true            | `rocket_v1`      | ATR × 0.6 (агрессивный) |
| ≥ 5.0   | true            | `lock_and_trail` | ATR × 0.8 (средний)     |
| ≥ 4.5   | true            | `lock_and_trail` | ATR × 0.8 (средний)     |
| < 4.5   | false           | `rocket_v1`      | Трейлинг отключен       |

---

## Сервисы-потребители

### 1. telegram-worker/notify_worker.py

**Назначение**: Отправка уведомлений в Telegram-бот.

**Читает**: `notify:telegram`

**Consumer Group**: `notify-group`

**Consumer Name**: `notify-consumer-{pid}`

**Функциональность**:

- Чтение сообщений через `XREADGROUP`
- Парсинг payload и извлечение текста сообщения
- Отправка в Telegram через бота
- Подтверждение обработки через `XACK`
- Обработка отчетов (`type=report`) отдельно от сигналов

**Код обработки**:

```python
# Из notify_worker.py
async def handle_message(entry: Dict[str, Any], stream_name: str = None, message_id: str = None) -> bool:
    # Приоритет 1: Обработка отчетов
    msg_type = entry.get("type")
    if msg_type == "report":
        text = entry.get("text", "")
        await send_html_to_telegram(text)
        return True

    # Обработка сигналов
    text = entry.get("text", "")
    if not text:
        return False

    await send_telegram_message(text)
    return True
```

**Конфигурация**:

- `NOTIFY_GROUP`: `notify-group` (по умолчанию)
- `NOTIFY_CONSUMER`: `notify-consumer-{pid}`
- `NOTIFY_MAX_RETRIES`: 5

---

### 2. AggregatedSignalHubV2

**Назначение**: Агрегация сигналов из разных источников с добавлением cluster-score из DOM.

**Читает**:

- `signals:orderflow:{symbol}` — от `CryptoOrderFlowHandler`
- `stream:manual-signals` — дубликат крипто-сигналов
- `stream:tick_{symbol}` — тики для анализа
- `book:levels:{symbol}` — DOM для cluster-score

**Consumer Groups**:

- `hub-{symbol}` — для `signals:orderflow:{symbol}`
- `hub-v2-crypto` — для `stream:manual-signals`

**Функциональность**:

1. **Чтение сигналов** из разных стримов
2. **Анализ тиков** через:
   - `MicrostructureSpikeDetectorPro` — true bid/ask delta
   - `MicrostructureSpikeDetector` (legacy) — классический детектор
3. **Cluster анализ** через `SmartClusterAnalyzer` (DOM stacked/absorption)
4. **Weighted confidence blending**:

   ```python
   w_delta_pro = 0.50    # true bid/ask delta
   w_speed = 0.15        # tick speed/range
   w_cluster = 0.25      # DOM stacked/absorption
   w_legacy = 0.10       # legacy detector
   ```

5. **Фильтрация**:
   - `confidence_threshold`: 0.25 (по умолчанию)
   - `min_signal_interval_sec`: 60 (антиспам)
   - Side lock: 20 секунд (anti-dither)
6. **Публикация** через `FilteredSignalWriter`:
   - `notify:telegram` — финальные сигналы
   - `signals:aggregated:{symbol}` — для трекинга

**Конфигурация**:

```python
CRYPTO_ORDERFLOW_STREAM = "stream:manual-signals"
CRYPTO_ORDERFLOW_GROUP = "hub-v2-crypto"
HUB_CONFIDENCE_THR = "0.25"
HUB_MIN_SIG_INT_SEC = "60"
W_DELTA_PRO = "0.50"
W_SPEED = "0.15"
W_CLUSTER = "0.25"
W_LEGACY = "0.10"
```

---

### 3. FilteredSignalWriter

**Назначение**: Финальная фильтрация, risk sizing и публикация сигналов.

**Читает**: Сигналы от `AggregatedSignalHubV2` (через метод `write_and_push()`)

**Функциональность**:

1. **Cooldown проверка**:

   ```python
   def _can_emit(self) -> bool:
       return (time.time() - self.last_ts) >= self.cfg.cooldown_sec
   ```

2. **Risk sizing**:

   - Получение баланса через `/account/balance`
   - Расчет лота через `PositionSizer.size_by_atr()`
   - Расчет SL/TP на основе ATR multipliers

3. **Публикация в стримы**:

   - `notify:telegram` — с троттлингом
   - `signals:aggregated:{symbol}` — для `SignalPerformanceTracker`

4. **Сохранение snapshot** для `TP1TrailingOrchestrator`

**Конфигурация**:

```python
cooldown_sec = 300  # 5 минут
risk_pct = 1.0     # 1% риска
atr_sl_mult = 1.5  # SL = ATR × 1.5
atr_tp_mults = [2.0, 3.0, 4.0]  # TP уровни
```

---

### 4. SignalPerformanceTracker

**Назначение**: Отслеживание эффективности сигналов в реальном времени.

**Читает**:

- `signals:orderflow:{symbol}` — исходные сигналы
- `signals:aggregated:{symbol}` — агрегированные сигналы
- `stream:tick_{symbol}` — тики для отслеживания цен

**Consumer Groups**: Создаются динамически для каждого символа

**Функциональность**:

1. **Создание виртуальных позиций**:

   - Отслеживание entry, SL, TP уровней
   - Расчет P&L в реальном времени

2. **Интеграция с компонентами**:

   - `TradeMonitor` — отслеживание позиций
   - `StatsAggregator` — обновление метрик
   - `ReportingService` — генерация отчетов
   - `TP1TrailingOrchestrator` — управление трейлингом

3. **Периодические задачи**:
   - Отчеты каждые 100 сделок (настраивается через `REPORT_TRIGGER_COUNT`)
   - Ежедневные сводки (в заданный час UTC через `DAILY_SUMMARY_HOUR`)
   - Health checks

**Конфигурация**:

```python
symbols = ["XAUUSD", "BTCUSDT", "ETHUSDT"]
strategies = ["orderflow", "ta", "aggregated"]
```

---

### 5. TP1TrailingOrchestrator

**Назначение**: Управление трейлингом стоп-лосса после достижения TP1.

**Читает**: Signal snapshots из `signal:snap:{sid}`

**Функциональность**:

1. **Мониторинг позиций**:

   - Отслеживание достижения TP1
   - Активация трейлинга при `trail_after_tp1=true`

2. **Профили трейлинга**:

   - `rocket_v1`: ATR × 0.6 (агрессивный)
   - `lock_and_trail`: ATR × 0.8 (средний)
   - `wide_swing`: ATR × 1.2 (консервативный)

3. **Обновление SL**:
   - Динамический пересчет на основе профиля
   - Публикация обновлений в Redis

---

## Consumer Groups

### Таблица Consumer Groups

| Стрим                         | Consumer Group          | Consumer Name                 | Сервис                             |
| ----------------------------- | ----------------------- | ----------------------------- | ---------------------------------- |
| `notify:telegram`             | `notify-group`          | `notify-consumer-{pid}`       | `telegram-worker`                  |
| `signals:orderflow:{symbol}`  | `{symbol}-signal-group` | `{symbol}-handler-{pid}-{ts}` | `CryptoOrderFlowHandler` (создает) |
| `signals:orderflow:{symbol}`  | `hub-{symbol}`          | `hub-{ts}`                    | `AggregatedSignalHub`              |
| `stream:manual-signals`       | `hub-v2-crypto`         | `hub-v2-crypto-{ts}`          | `AggregatedSignalHubV2`            |
| `signals:aggregated:{symbol}` | (динамический)          | (динамический)                | `SignalPerformanceTracker`         |

### Создание Consumer Groups

**Автоматическое создание**:

```python
# Из BaseOrderFlowHandler
stream_helper = SyncRedisStreamHelper(self.redis_client, self.group, consumer_name)
stream_helper.ensure_groups([self.tick_stream, self.book_stream])
```

**Ручное создание**:

```bash
# Создание consumer group для notify:telegram
redis-cli XGROUP CREATE notify:telegram notify-group $ MKSTREAM

# Создание consumer group для signals:orderflow:BTCUSD
redis-cli XGROUP CREATE signals:orderflow:BTCUSD hub-BTCUSD $ MKSTREAM

# Создание consumer group для stream:manual-signals
redis-cli XGROUP CREATE stream:manual-signals hub-v2-crypto $ MKSTREAM
```

### Обработка NOGROUP ошибок

Все сервисы автоматически обрабатывают `NOGROUP` ошибки:

```python
# Из BaseOrderFlowHandler._run_loop()
except Exception as e:
    error_str = str(e).upper()
    if "NOGROUP" in error_str:
        print(f"⚠️ Обнаружен NOGROUP для стримов {self.symbol}, пересоздаём consumer groups...")
        stream_helper.ensure_groups([self.tick_stream, self.book_stream], recreate=False)
        time.sleep(2)
```

---

## Полная схема потока данных

### Этап 1: Генерация сигнала

```
┌─────────────────────────────────────────┐
│  CryptoOrderFlowHandler                  │
│  (обрабатывает тики и order book)        │
└──────────────┬──────────────────────────┘
               │
               ├─→ Анализ тиков
               │   ├─→ Delta классификация
               │   ├─→ Z-score расчет
               │   ├─→ OBI анализ
               │   └─→ Weak progress детекция
               │
               ├─→ ATR Timeframe Calibration (NEW)
               │   ├─→ Выбор оптимального TF из (1m, 5m, 15m)
               │   ├─→ Проверка freshness и волатильности
               │   └─→ Fallback на config default
               │
               └─→ Генерация сигнала
                   ├─→ Unified ATR Gate (Floor + Fees)
                   │   ├─→ Проверка min ATR (ATR Floor Policy)
                   │   ├─→ Проверка fees coverage (Fees Aware Policy)
                   │   └─→ Veto если ATR < threshold
                   │
                   └─→ _publish_signal()
```

### Этап 2: Публикация в стримы

```
_publish_signal()
    │
    ├─→ notify:telegram ────────────────┐
    │   (maxlen=500, троттлинг)          │
    │                                     │
    ├─→ signals:orderflow:{symbol} ────┤
    │   (maxlen=1000)                     │
    │                                     │
    ├─→ signals:audit:{symbol} ─────────┤
    │   (maxlen=200000)                   │
    │                                     │
    ├─→ signal:snap:{sid} ──────────────┤
    │   (TTL=6 часов)                     │
    │                                     │
    └─→ _after_signal_published() ───────┤
        └─→ stream:manual-signals ───────┘
            (maxlen=2000, только крипта)
```

### Этап 3: Агрегация

```
┌─────────────────────────────────────────┐
│  AggregatedSignalHubV2                  │
│  (читает из signals:orderflow и         │
│   stream:manual-signals)                │
└──────────────┬──────────────────────────┘
               │
               ├─→ Анализ тиков
               │   ├─→ Pro detector (true delta)
               │   ├─→ Legacy detector
               │   └─→ Cluster analyzer (DOM)
               │
               ├─→ Weighted confidence blending
               │   ├─→ w_delta_pro: 0.50
               │   ├─→ w_speed: 0.15
               │   ├─→ w_cluster: 0.25
               │   └─→ w_legacy: 0.10
               │
               ├─→ Фильтрация
               │   ├─→ confidence_threshold: 0.25
               │   ├─→ min_signal_interval: 60s
               │   └─→ side_lock: 20s
               │
               └─→ FilteredSignalWriter.write_and_push()
```

### Этап 4: Фильтрация и публикация

```
┌─────────────────────────────────────────┐
│  FilteredSignalWriter                    │
│  (финальная фильтрация и risk sizing)     │
└──────────────┬──────────────────────────┘
               │
               ├─→ Cooldown проверка
               ├─→ Risk sizing
               │   ├─→ Получение баланса
               │   ├─→ Расчет лота
               │   └─→ Расчет SL/TP
               │
               └─→ Публикация
                   ├─→ notify:telegram ────→ telegram-worker
                   └─→ signals:aggregated:{symbol} ──→ SignalPerformanceTracker
```

### Этап 5: Downstream обработка

```
┌─────────────────────────────────────────┐
│  telegram-worker                        │
│  (отправка в Telegram)                   │
└──────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  SignalPerformanceTracker                │
│  (отслеживание эффективности)            │
│                                          │
│  ├─→ TradeMonitor                       │
│  │   └─→ Виртуальные позиции            │
│  │                                      │
│  ├─→ StatsAggregator                    │
│  │   └─→ Метрики в stats:{strategy}:{symbol}:{tf} │
│  │                                      │
│  ├─→ ReportingService                   │
│  │   └─→ Отчеты каждые 100 сделок      │
│  │                                      │
│  └─→ TP1TrailingOrchestrator           │
│      └─→ Управление трейлингом          │
└──────────────────────────────────────────┘
```

### Полная визуальная схема

```
┌─────────────────────────────────────────────────────────────────┐
│                    CryptoOrderFlowHandler                        │
│  [Тики] → [Обработка] → [Генерация сигнала] → [Публикация]     │
└────────────┬────────────┬────────────┬────────────┬────────────┘
             │            │            │            │
             ▼            ▼            ▼            ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │notify:      │ │signals:     │ │signals:     │ │signal:      │
    │telegram     │ │orderflow:   │ │audit:       │ │snap:{sid}   │
    │             │ │{symbol}     │ │{symbol}     │ │             │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │                │                │
           │               │                │                │
           ▼               ▼                ▼                ▼
    ┌─────────────┐ ┌─────────────────────────────────────────────┐
    │telegram-    │ │      AggregatedSignalHubV2                  │
    │worker       │ │  [Агрегация] → [Blending] → [Фильтрация]    │
    └─────────────┘ └──────────────┬──────────────────────────────┘
                                    │
                                    ▼
                            ┌─────────────────┐
                            │FilteredSignal   │
                            │Writer           │
                            └────────┬────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
                    ▼                ▼                ▼
            ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
            │notify:      │  │signals:     │  │signal:      │
            │telegram     │  │aggregated:  │  │snap:{sid}   │
            │             │  │{symbol}     │  │             │
            └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
                   │                │                │
                   ▼                ▼                ▼
            ┌─────────────┐  ┌─────────────────────────────┐
            │telegram-    │  │SignalPerformanceTracker        │
            │worker       │  │                              │
            └─────────────┘  │  ├─→ TradeMonitor            │
                             │  ├─→ StatsAggregator         │
                             │  ├─→ ReportingService        │
                             │  └─→ TP1TrailingOrchestrator │
                             └──────────────────────────────┘
```

---

## Конфигурация

### Переменные окружения для CryptoOrderFlowHandler

```bash
# Стримы
MANUAL_SIGNAL_STREAM="stream:manual-signals"
ENABLE_MANUAL_SIGNAL_STREAM="true"
AUTO_ORDER_STREAM="orders:auto-push"  # (закомментировано)

# Символ-специфичные настройки
BTCUSD_TICK_STREAM="stream:tick_BTCUSD"
BTCUSD_BOOK_STREAM="stream:book_BTCUSD"
BTCUSD_GROUP="btcusd-signal-group"
BTCUSD_CONSUMER="btcusd-handler"

# Notify настройки
NOTIFY_STREAM="notify:telegram"
NOTIFY_SIGNAL_COUNTER_KEY="notify:telegram:signal_counter"
CRYPTO_NOTIFY_SIGNAL_EVERY_N="1"  # троттлинг (1 = каждый сигнал)

# Signal streams
ORDERFLOW_SIGNAL_STREAM="signals:orderflow:{symbol}"  # или явно signals:orderflow:BTCUSD
SIGNAL_AUDIT_STREAM="signals:audit:{symbol}"

# Snapshot
SNAP_PREFIX="signal:snap:"
SNAP_TTL="21600"  # 6 часов

# ATR Calibration (NEW)
ATR_TF_CALIB_ENABLE="1"
ATR_TF_CANDIDATES="1m,5m,15m"
ATR_TF_MIN_ATR_BPS="5.0"
ATR_TF_MAX_ATR_BPS="50.0"

# Unified ATR Gate
ATR_GATE_MODE="ENFORCE"  # OFF | SHADOW | ENFORCE
DEBUG_VETO="1"          # Включить логирование причин отсева

```

### Переменные окружения для AggregatedSignalHubV2

```bash
# Символ
SYMBOL="BTCUSD"

# Redis URLs
REDIS_URL="redis://scanner-redis-worker-1:6379/0"
REDIS_TICKS_URL="redis://redis-ticks:6379/0"

# Crypto stream
CRYPTO_ORDERFLOW_STREAM="stream:manual-signals"
CRYPTO_ORDERFLOW_GROUP="hub-v2-crypto"

# Thresholds
HUB_CONFIDENCE_THR="0.25"
HUB_MIN_SIG_INT_SEC="60"
HUB_SIDE_LOCK_SEC="20"

# Weights для confidence blending
W_DELTA_PRO="0.50"
W_SPEED="0.15"
W_CLUSTER="0.25"
W_LEGACY="0.10"

# Writer config
MIN_CONF="60.0"
HUB_COOLDOWN="300"
RISK_PCT="1.0"
SL_MULT="1.5"
TP_MULTS="2.0,3.0,4.0"
```

### Переменные окружения для FilteredSignalWriter

```bash
# Cooldown и risk
HUB_COOLDOWN="300"  # 5 минут
RISK_PCT="1.0"
SL_MULT="1.5"
TP_MULTS="2.0,3.0,4.0"

# Notify
NOTIFY_STREAM="notify:telegram"
NOTIFY_SIGNAL_COUNTER_KEY="notify:telegram:signal_counter"
CRYPTO_NOTIFY_SIGNAL_EVERY_N="1"

# Gateway
GATEWAY_URL="http://scanner-go-gateway:8090"
GATEWAY_PUSH_PATH="/orders/push"
BALANCE_PATH="/account/balance"
```

### Переменные окружения для SignalPerformanceTracker

```bash
# Redis
REDIS_URL="redis://scanner-redis-worker-1:6379/0"
REDIS_TICKS_URL="redis://redis-ticks:6379/0"

# Symbols и strategies
SYMBOLS="XAUUSD,BTCUSDT,ETHUSDT"
STRATEGIES="orderflow,ta,aggregated"

# Health
TRACKER_HEALTH_KEY="health:signal_performance_tracker"
TRACKER_HEALTH_TTL="300"
```

---

## Диагностика и мониторинг

### Проверка стримов

```bash
# Проверка notify:telegram
redis-cli XINFO STREAM notify:telegram
redis-cli XLEN notify:telegram
redis-cli XINFO GROUPS notify:telegram

# Проверка signals:orderflow:BTCUSD
redis-cli XINFO STREAM signals:orderflow:BTCUSD
redis-cli XLEN signals:orderflow:BTCUSD
redis-cli XINFO GROUPS signals:orderflow:BTCUSD

# Проверка stream:manual-signals
redis-cli XINFO STREAM stream:manual-signals
redis-cli XLEN stream:manual-signals
redis-cli XINFO GROUPS stream:manual-signals

# Проверка signals:aggregated:BTCUSD
redis-cli XINFO STREAM signals:aggregated:BTCUSD
redis-cli XLEN signals:aggregated:BTCUSD
```

### Проверка Consumer Groups

```bash
# Список consumer groups для notify:telegram
redis-cli XINFO GROUPS notify:telegram

# Список consumers в группе
redis-cli XINFO CONSUMERS notify:telegram notify-group

# Pending messages (необработанные)
redis-cli XPENDING notify:telegram notify-group
```

### Чтение последних сообщений

```bash
# Последние 10 сообщений из notify:telegram
redis-cli XREVRANGE notify:telegram + - COUNT 10

# Последние 10 сообщений из signals:orderflow:BTCUSD
redis-cli XREVRANGE signals:orderflow:BTCUSD + - COUNT 10

# Чтение через consumer group (без ACK)
redis-cli XREADGROUP GROUP notify-group consumer-1 COUNT 10 STREAMS notify:telegram >
```

### Проверка snapshots

```bash
# Проверка snapshot для конкретного сигнала
redis-cli GET signal:snap:BTCUSD:LONG:9500000:1734567890123

# TTL snapshot
redis-cli TTL signal:snap:BTCUSD:LONG:9500000:1734567890123

# Поиск всех snapshots (через SCAN)
redis-cli --scan --pattern "signal:snap:*"
```

### Мониторинг метрик

```bash
# Статистика SignalPerformanceTracker
redis-cli HGETALL stats:orderflow:BTCUSD:1m

# Health check
redis-cli GET health:signal_performance_tracker
```

### Логирование

Все сервисы логируют ключевые события:

```python
# CryptoOrderFlowHandler
print(f"📤 Сигнал опубликован: {signal.sid} | {side} @ {context.price:.2f}")
print(f"📸 Snapshot saved: {snap_key} (TTL={self.snap_ttl}s)")

# AggregatedSignalHubV2
log.info("📨 Signal published to %s: %s", self.cfg.notify_stream, sid)

# FilteredSignalWriter
self.log.info("📨 Signal published to %s: %s", self.cfg.notify_stream, sid)
```

---

## Примеры использования

### Пример 1: Генерация сигнала

```python
# CryptoOrderFlowHandler обрабатывает тик
tick = Tick(
    ts=1734567890123,
    bid=94999.50,
    ask=95000.50,
    last=95000.00,
    volume=0.5,
    flags=1  # Trade flag
)

# Классификация Delta (для крипты)
delta = handler._classify_delta(tick)  # +0.5 (покупка)

# Генерация контекста
context = SignalContext(
    ts=1734567890123,
    price=95000.00,
    z_delta=4.5,  # Сильный сигнал
    weak_progress=True,
    obi=0.65,
    atr=500.00,
    pivots={...},
    delta_window=deque([...]),
    current_delta=0.5
)

# Генерация сигнала
handler._generate_signals(context)
# → Вызывает _publish_signal("LONG", context, "Absorption...", "🛡️")
```

### Пример 2: Публикация в стримы

```python
# _publish_signal() создает Signal объект
signal = create_signal(
    symbol="BTCUSD",
    side="LONG",
    entry=95000.00,
    sl=94500.00,
    tp_levels=[96000.00, 97000.00, 98000.00],
    lot=0.10,
    source="OrderFlow",
    reason="Absorption (weak progress + delta spike)",
    confidence=85.0,
    atr=500.00,
    ts=1734567890123,
    indicators={
        "z_delta": 4.5,
        "obi": 0.65,
        "weak_progress": True,
        "atr": 500.00,
        "delta_window_len": 300
    }
)

# Публикация в notify:telegram
redis_payload = UnifiedSignalFormatter.format_redis_payload(signal)
dual_redis.xadd("notify:telegram", redis_data, maxlen=500)

# Публикация в signals:orderflow:BTCUSD
signal_payload = UnifiedSignalFormatter.format_audit_payload(signal, extra_context={...})
simple_redis.xadd("signals:orderflow:BTCUSD", {"data": json.dumps(signal_payload)}, maxlen=1000)

# Публикация в signals:audit:BTCUSD
audit_payload = UnifiedSignalFormatter.format_audit_payload(signal, extra_context={...})
redis_client.xadd("signals:audit:BTCUSD", {"data": json.dumps(audit_payload)}, maxlen=200000)

# Сохранение snapshot
redis_client.setex("signal:snap:BTCUSD:LONG:9500000:1734567890123", 21600, json.dumps(snapshot))

# Дубликат в stream:manual-signals (только для крипты)
manual_payload = {...}
dual_redis.xadd("stream:manual-signals", {"data": json.dumps(manual_payload)}, maxlen=2000)
```

### Пример 3: Чтение через Consumer Group

```python
# telegram-worker читает из notify:telegram
messages = redis.xreadgroup(
    "notify-group",
    "notify-consumer-12345",
    {"notify:telegram": ">"},
    count=10,
    block=1000
)

for stream, items in messages:
    for msg_id, fields in items:
        text = fields.get("text", "")
        await send_telegram_message(text)
        redis.xack("notify:telegram", "notify-group", msg_id)
```

### Пример 4: Агрегация в AggregatedSignalHubV2

```python
# Чтение из stream:manual-signals
messages = redis.xreadgroup(
    "hub-v2-crypto",
    "hub-v2-crypto-12345",
    {"stream:manual-signals": ">"},
    count=20,
    block=1000
)

for stream, items in messages:
    for msg_id, fields in items:
        data = json.loads(fields["data"])

        # Анализ тиков
        pro_score = det_pro.update(tick_data)
        legacy_score = det_legacy.update(tick_data)
        cluster_score = cluster.analyze(book_data)

        # Weighted blending
        conf = (
            w_delta_pro * pro_score +
            w_speed * speed_score +
            w_cluster * cluster_score +
            w_legacy * legacy_score
        )

        # Фильтрация
        if conf >= confidence_threshold:
            writer.write_and_push(
                symbol="BTCUSD",
                side=data["side"],
                entry=data["entry"],
                atr=data["atr"],
                confidence=conf,
                reason=data["reason"],
                source="AggregatedHub-V2"
            )

        redis.xack("stream:manual-signals", "hub-v2-crypto", msg_id)
```

---

## Заключение

`CryptoOrderFlowHandler` является ключевым компонентом системы генерации сигналов для криптовалют. Он публикует сигналы в 4 основных стрима, которые затем обрабатываются различными downstream сервисами для агрегации, фильтрации, отправки уведомлений и отслеживания эффективности.

Все стримы используют Redis Streams с consumer groups для гарантированной доставки и обработки без потерь. Система спроектирована для масштабирования и надежности.

---

**Дата создания**: 2025-01-XX  
**Последнее обновление**: 2025-11-26  
**Версия**: 1.0  
**Автор**: AI Assistant
