# TP1 Trailing System Documentation

## Обзор

Система автоматического трейлинга после достижения TP1, которая решает проблему «TP1 → SL откат».

### Проблема

В существующей системе сигналы часто достигают TP1, после чего цена откатывается и выбивает оставшиеся части позиции по SL. Это снижает общий PF (Profit Factor) и уменьшает прибыльность стратегии.

### Решение

После того, как цена достигает TP1:

1. Система получает событие `TP1_HIT` из Redis stream `events:trades`
2. Проверяет флаг `trail_after_tp1` в исходном сигнале
3. Если флаг установлен — активирует трейлинг с профилем `trail_profile`
4. Отправляет команду в go-gateway для изменения SL или включения трейлинга в MT5

## Архитектура

```
┌─────────────────┐
│  Signal Sources │
│ (OrderFlow/TA)  │  trail_after_tp1=true
└────────┬────────┘  trail_profile="rocket_v1"
         │
         ▼
    ┌────────┐
    │ Redis  │  signals:{sid}
    └────┬───┘
         │
         ▼
    ┌─────────┐
    │ Gateway │  /orders/push
    │  (Go)   │
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │   MT5   │  Opens position
    └────┬────┘  Hits TP1
         │
         ▼
    ┌──────────────┐
    │ Trade Events │  events:trades
    │  Publisher   │  {event_type: "TP1_HIT", ...}
    └──────┬───────┘
           │
           ▼
    ┌──────────────────┐
    │ TP Event Listener│  Consumer group
    └──────┬───────────┘
           │
           ▼
    ┌─────────────────────┐
    │ TP1 Trailing        │  Checks signal
    │ Orchestrator        │  Gets profile
    └──────┬──────────────┘
           │
           ▼
    ┌────────────────────┐
    │ Order Trailing     │  /orders/push
    │ Dispatcher         │  {action: "trail", mode: "ATR", atr_mult: 0.6}
    └──────┬─────────────┘
           │
           ▼
    ┌─────────┐
    │ Gateway │  Queues command for MT5
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │   MT5   │  Activates trailing stop
    └─────────┘
```

## Компоненты

### 1. Trailing Profiles (`services/trailing_profiles.py`)

Определяет профили трейлинга:

- **rocket_v1**: Агрессивный (ATR × 0.6) для сильных движений
- **lock_and_trail**: Базовый (ATR × 0.8) для защиты прибыли
- **wide_swing**: Консервативный (ATR × 1.2) для волатильных рынков
- **points_200**: Фиксированный (200 пунктов) как fallback
- **crypto_tight**: Очень агрессивный (ATR × 0.5) для крипты

```python
from services.trailing_profiles import TrailingProfile, TrailingProfilesRegistry

# Создание реестра профилей
registry = TrailingProfilesRegistry()

# Получение профиля
profile = registry.get("rocket_v1")
print(f"Mode: {profile.mode}, ATR mult: {profile.atr_mult}")

# Добавление кастомного профиля
custom = TrailingProfile(
    name="my_profile",
    mode="ATR",
    atr_mult=0.7,
    comment="Custom profile for EUR/USD"
)
registry.add(custom, save_to_redis=True)
```

### 2. TP Event Listener (`services/tp_event_listener.py`)

Слушает Redis stream `events:trades` и обрабатывает события:

- `TP1_HIT` → запускает трейлинг
- `TP2_HIT`, `TP3_HIT` → логирование
- `SL_HIT` → анализ причин
- `TRAILING_MOVE` → обновление статистики

```bash
# Запуск вручную
python -m services.tp_event_listener

# Через Docker Compose
docker-compose -f docker-compose.yml -f docker-compose.tp-trailing.yml up -d tp-event-listener
```

### 3. TP1 Trailing Orchestrator (`services/tp1_trailing_orchestrator.py`)

Оркестратор логики трейлинга:

1. Получает событие TP1_HIT
2. Извлекает исходный сигнал из Redis (`signals:{sid}`)
3. Проверяет флаг `trail_after_tp1`
4. Определяет профиль трейлинга
5. Отправляет команду в gateway

```python
from services.tp1_trailing_orchestrator import TP1TrailingOrchestrator

# Создание оркестратора
orchestrator = TP1TrailingOrchestrator()

# Обработка события
event = {
    "event_type": "TP1_HIT",
    "sid": "signal-XAUUSD-1730222790",
    "symbol": "XAUUSD",
    "position_id": "1234567",
    "price": "2769.9",
    "ts": "1730222790",
    "source": "mt5"
}

orchestrator.handle_event(event)

# Статистика
stats = orchestrator.get_stats()
print(f"TP1 hits: {stats['tp1_hits']}, Trailing started: {stats['trailing_started']}")
```

### 4. Order Trailing Dispatcher (`services/order_trailing_dispatcher.py`)

HTTP клиент для отправки команд в go-gateway:

```python
from services.order_trailing_dispatcher import OrderTrailingDispatcher
from services.trailing_profiles import TrailingProfile

dispatcher = OrderTrailingDispatcher(gateway_url="http://scanner-go-gateway:8090")

profile = TrailingProfile(name="rocket_v1", mode="ATR", atr_mult=0.6)

success = dispatcher.send_trailing_command(
    sid="signal-XAUUSD-123",
    symbol="XAUUSD",
    position_id="1234567",
    profile=profile
)
```

### 5. Trade Events Publisher (Go) (`go-gateway/internal/events/trade_events.go`)

Go модуль для публикации событий в Redis:

```go
import "scanner-gw/internal/events"

publisher := events.NewTradeEventPublisher(redisClient, "events:trades")

// Publish TP1 hit
publisher.PublishTP1Hit(
    sid,
    symbol,
    positionID,
    price,
    lot,
    "mt5"
)

// Publish trailing started
publisher.PublishTrailingStarted(
    sid,
    symbol,
    positionID,
    "rocket_v1",
    "tp1_trailing_orchestrator"
)
```

## Расширение формата сигналов

### XAUUSDSignal

```python
from core.xauusd_signal_formatter import XAUUSDSignal, XAUUSDSignalFormatter

signal = XAUUSDSignal(
    sid="signal-XAUUSD-123",
    symbol="XAUUSD",
    side="LONG",
    entry=2765.5,
    sl=2758.7,
    tp_levels=[2769.9, 2773.1, 2776.3],
    lot=0.03,
    source="OrderFlow",
    reason="Extreme delta spike",
    confidence=85.0,
    atr=2.4,
    ts=1730222790000,
    # ✅ НОВЫЕ ПОЛЯ
    trail_after_tp1=True,
    trail_profile="rocket_v1"
)

# Форматирование для Redis
payload = XAUUSDSignalFormatter.format_redis_payload(signal)
# payload['trail_after_tp1'] = True
# payload['trail_profile'] = "rocket_v1"
```

### UnifiedSignal

```python
from core.unified_signal_formatter import Signal, UnifiedSignalFormatter

signal = Signal(
    sid="XAUUSD:LONG:276550:1730222790000",
    symbol="XAUUSD",
    side="LONG",
    entry=2765.5,
    sl=2758.7,
    tp_levels=[2769.9, 2773.1, 2776.3],
    lot=0.03,
    source="TechnicalAnalysis",
    reason="RSI oversold + EMA cross",
    confidence=75.0,
    atr=2.4,
    ts=1730222790000,
    indicators={"rsi": 28.5, "ema_cross": True},
    # ✅ НОВЫЕ ПОЛЯ
    trail_after_tp1=True,
    trail_profile="lock_and_trail"
)
```

## Конфигурация

### trailing_config.json

```json
{
	"enabled": true,
	"redis_url": "redis://scanner-redis:6379/0",
	"events_stream": "events:trades",
	"consumer_group": "tp1-trailing-group",
	"batch_size": 50,
	"block_ms": 5000,
	"stats_interval_sec": 300,
	"default_profile": "rocket_v1",
	"profiles": {
		"rocket_v1": {
			"name": "rocket_v1",
			"mode": "ATR",
			"atr_mult": 0.6,
			"comment": "Tight ATR trailing for strong moves"
		}
	},
	"gateway": {
		"url": "http://scanner-go-gateway:8090",
		"timeout_sec": 3.0,
		"max_retries": 3
	}
}
```

### Environment Variables

```bash
# Redis
REDIS_URL=redis://scanner-redis:6379/0
TP_EVENTS_STREAM=events:trades
TP_EVENTS_GROUP=tp1-trailing-group

# Gateway
GATEWAY_URL=http://scanner-go-gateway:8090
GATEWAY_TIMEOUT=3.0

# Trailing
DEFAULT_TRAIL_PROFILE=rocket_v1
SIGNAL_KEY_PREFIX=signals:

# Processing
TP_EVENTS_BATCH_SIZE=50
TP_EVENTS_BLOCK_MS=5000
STATS_INTERVAL_SEC=300
```

## Использование

### 1. Запуск системы

```bash
# Запуск через docker-compose
docker-compose -f docker-compose.yml -f docker-compose.tp-trailing.yml up -d tp-event-listener

# Проверка логов
docker logs -f scanner-tp-event-listener

# Проверка здоровья
docker exec scanner-tp-event-listener python -c "
from services.tp_event_listener import TPEventListener
listener = TPEventListener()
print(listener.health_check())
"
```

### 2. Тестирование с эмулятором

```bash
# Создаём тестовый сигнал в Redis
python -c "
import redis, json, time
r = redis.from_url('redis://scanner-redis:6379/0', decode_responses=True)
signal = {
    'sid': 'test-signal-123',
    'symbol': 'XAUUSD',
    'side': 'LONG',
    'entry': 2765.5,
    'sl': 2758.7,
    'tp_levels': [2769.9, 2773.1, 2776.3],
    'trail_after_tp1': True,
    'trail_profile': 'rocket_v1',
    'ts': int(time.time() * 1000)
}
r.set('signals:test-signal-123', json.dumps(signal), ex=3600)
print('✅ Test signal created')
"

# Эмитируем TP1_HIT событие
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_only

# Эмитируем сценарий TP1 → TP2
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_then_tp2

# Эмитируем сценарий TP1 → SL
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_then_sl
```

### 3. Мониторинг

```bash
# Проверка Redis stream
redis-cli XLEN events:trades
redis-cli XREAD COUNT 10 STREAMS events:trades 0

# Проверка consumer groups
redis-cli XINFO GROUPS events:trades

# Проверка профилей
redis-cli GET trailing:profiles | jq .

# Статистика оркестратора
docker exec scanner-tp-event-listener python -c "
from services.tp1_trailing_orchestrator import TP1TrailingOrchestrator
o = TP1TrailingOrchestrator()
o.log_stats()
"
```

## Интеграция с существующими компонентами

### aggregated_signal_hub_v2.py

Уже поддерживает новый формат:

```python
from core.xauusd_signal_formatter import XAUUSDSignal

signal = XAUUSDSignal(
    # ... обычные поля ...
    trail_after_tp1=True,  # Включить трейлинг
    trail_profile="rocket_v1"  # Профиль
)

result = writer.write_and_push(...)
```

### xau_orderflow_handler.py

Добавьте при создании сигнала:

```python
signal = XAUUSDSignal(
    # ... обычные поля ...
    trail_after_tp1=True if confidence > 75 else False,
    trail_profile="rocket_v1" if abs(z_delta) > 5.0 else "lock_and_trail"
)
```

### signal-generator

При генерации сигналов:

```javascript
const signal = {
	// ... обычные поля ...
	trail_after_tp1: true,
	trail_profile: 'rocket_v1',
}
```

## Расширяемость для Real DOM

Система спроектирована для расширения:

```python
# Новый профиль на основе DOM imbalance
dom_profile = TrailingProfile(
    name="dom_dynamic",
    mode="ATR",
    atr_mult=0.5,  # Начальное значение
    comment="Dynamic trailing based on DOM absorption"
)

# Метаданные для динамического изменения
metadata = {
    "dom_absorption_ratio": 0.75,
    "stacked_levels": 3,
    "dynamic_atr_mult": True  # Флаг для dynamic adjustment
}

dispatcher.send_trailing_command(
    sid=sid,
    symbol=symbol,
    position_id=position_id,
    profile=dom_profile,
    metadata=metadata
)
```

## Метрики и анализ

События сохраняются для последующего анализа:

```python
# Получить все события по сигналу
import redis, json
r = redis.from_url('redis://scanner-redis:6379/0', decode_responses=True)

events = r.lrange('trade:events:signal-XAUUSD-123', 0, -1)
for event_json in events:
    event = json.loads(event_json)
    print(f"{event['event_type']}: {event.get('price', 'N/A')}")
```

## Troubleshooting

### Трейлинг не активируется

1. Проверьте наличие сигнала в Redis:

```bash
redis-cli GET signals:your-signal-id
```

2. Проверьте флаг `trail_after_tp1`:

```bash
redis-cli GET signals:your-signal-id | jq .trail_after_tp1
```

3. Проверьте логи listener:

```bash
docker logs scanner-tp-event-listener | grep "TP1_HIT"
```

### События не поступают

1. Проверьте stream:

```bash
redis-cli XLEN events:trades
```

2. Проверьте consumer group:

```bash
redis-cli XINFO GROUPS events:trades
```

3. Проверьте pending messages:

```bash
redis-cli XPENDING events:trades tp1-trailing-group
```

### Gateway не получает команды

1. Проверьте connectivity:

```bash
docker exec scanner-tp-event-listener curl http://scanner-go-gateway:8090/health
```

2. Проверьте логи gateway:

```bash
docker logs scanner-go-gateway | grep "trail"
```

## Performance

Оптимизация для production:

```yaml
# docker-compose.tp-trailing.yml
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
  replicas: 2 # Для высокой доступности
```

Consumer group обеспечивает:

- Load balancing между репликами
- At-least-once delivery
- Automatic failover

## См. также

- [AGGREGATED_HUB_V2_REDIS_TICKS_SETUP.md](../AGGREGATED_HUB_V2_REDIS_TICKS_SETUP.md)
- [SIGNAL_TRACKER_SETUP_COMPLETE.md](../SIGNAL_TRACKER_SETUP_COMPLETE.md)
- [ARCHITECTURE.md](../ARCHITECTURE.md)
- [CONFIGURATION.md](../CONFIGURATION.md)

## Авторы

Scanner Infrastructure Team
Version: 1.0.0
Last Updated: 2025-11-06
