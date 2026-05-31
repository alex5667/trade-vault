# TP1 Trailing System - Quick Start Guide

## 🎯 Что это?

Система автоматического трейлинга, которая активируется после достижения TP1, чтобы:

- ✅ Защитить полученную прибыль
- ✅ Выжать максимум из сильных движений («ракет»)
- ✅ Уменьшить количество сигналов с паттерном TP1→SL

## 📦 Быстрая установка

### 1. Запуск сервиса

```bash
# Запуск TP Event Listener
docker-compose -f docker-compose.yml -f docker-compose.tp-trailing.yml up -d tp-event-listener

# Проверка статуса
docker ps | grep tp-event-listener
docker logs -f scanner-tp-event-listener
```

### 2. Проверка работы

```bash
# Убедитесь что сервис подключен к Redis
docker logs scanner-tp-event-listener | grep "Connected to Redis"

# Проверьте consumer group
redis-cli XINFO GROUPS events:trades
```

## 🚀 Использование в коде

### Вариант 1: XAUUSDSignal (для XAUUSD сигналов)

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
    ts=int(time.time() * 1000),
    # 🎯 Включаем трейлинг после TP1
    trail_after_tp1=True,
    trail_profile="rocket_v1"  # Агрессивный профиль
)

# Публикация сигнала
redis_payload = XAUUSDSignalFormatter.format_redis_payload(signal)
```

### Вариант 2: UnifiedSignal (универсальный для всех инструментов)

```python
from core.unified_signal_formatter import Signal, UnifiedSignalFormatter

signal = Signal(
    sid=UnifiedSignalFormatter.create_signal_id("BTCUSD", "LONG", 50000.0, int(time.time()*1000)),
    symbol="BTCUSD",
    side="LONG",
    entry=50000.0,
    sl=49500.0,
    tp_levels=[50500.0, 51000.0, 51500.0],
    lot=0.1,
    source="TechnicalAnalysis",
    reason="RSI oversold + EMA cross",
    confidence=75.0,
    atr=250.0,
    ts=int(time.time() * 1000),
    indicators={"rsi": 28.5},
    # 🎯 Включаем трейлинг
    trail_after_tp1=True,
    trail_profile="crypto_tight"  # Для криптовалют
)
```

### Вариант 3: Автоматический выбор профиля

```python
# В вашем signal generator / handler
def choose_trail_profile(confidence, z_delta, market_regime):
    """Умный выбор профиля трейлинга."""

    # Сильные сигналы → агрессивный трейлинг
    if confidence > 80 and abs(z_delta) > 5.0:
        return "rocket_v1"  # ATR × 0.6

    # Волатильный рынок → широкий трейлинг
    elif market_regime == "choppy":
        return "wide_swing"  # ATR × 1.2

    # Крипто → очень агрессивный
    elif symbol.endswith("USD") and symbol.startswith("BTC"):
        return "crypto_tight"  # ATR × 0.5

    # По умолчанию
    else:
        return "lock_and_trail"  # ATR × 0.8


# Применение
signal = XAUUSDSignal(
    # ... обычные поля ...
    trail_after_tp1=True if confidence > 60 else False,
    trail_profile=choose_trail_profile(confidence, z_delta, market_regime)
)
```

## 📊 Профили трейлинга

| Профиль          | Режим  | ATR ×       | Описание          | Когда использовать             |
| ---------------- | ------ | ----------- | ----------------- | ------------------------------ |
| `rocket_v1`      | ATR    | 0.6         | Агрессивный       | Сильные сигналы (conf>80, z>5) |
| `lock_and_trail` | ATR    | 0.8         | Базовый           | Обычные сигналы                |
| `wide_swing`     | ATR    | 1.2         | Консервативный    | Волатильный рынок              |
| `crypto_tight`   | ATR    | 0.5         | Очень агрессивный | Криптовалюты                   |
| `points_200`     | POINTS | 200 пунктов | Фиксированный     | Когда ATR недоступен           |

## 🧪 Тестирование

### Создать тестовый сигнал

```python
import redis, json, time

r = redis.from_url('redis://scanner-redis:6379/0', decode_responses=True)

# Создаём тестовый сигнал с трейлингом
signal = {
    'sid': 'test-signal-123',
    'symbol': 'XAUUSD',
    'side': 'LONG',
    'entry': 2765.5,
    'sl': 2758.7,
    'tp_levels': [2769.9, 2773.1, 2776.3],
    'lot': 0.03,
    'trail_after_tp1': True,
    'trail_profile': 'rocket_v1',
    'ts': int(time.time() * 1000)
}

# Сохраняем в Redis (TTL 1 час)
r.set('signals:test-signal-123', json.dumps(signal), ex=3600)
print('✅ Test signal created')
```

### Эмитировать TP1 событие

```bash
# TP1 только
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_only

# TP1 → TP2
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_then_tp2

# TP1 → SL (тест отката)
python -m services.tp_event_emulator --sid test-signal-123 --scenario tp1_then_sl
```

### Проверить результат

```bash
# Проверить события
redis-cli XREAD COUNT 10 STREAMS events:trades 0

# Проверить логи
docker logs scanner-tp-event-listener | grep "test-signal-123"

# Должны увидеть:
# ✅ Trailing started: sid=test-signal-123 profile=rocket_v1 mode=ATR atr_mult=0.60
```

## 📈 Мониторинг

### Проверка статистики

```bash
# Статистика listener
docker exec scanner-tp-event-listener python -c "
from services.tp_event_listener import TPEventListener
listener = TPEventListener()
listener._log_stats()
"

# Ожидаемый вывод:
# 📊 Listener Stats: read=150 processed=150 acked=150 errors=0
# 📊 TP1 Trailing Stats: tp1_hits=10 started=8 failed=0 not_found=2 no_flag=0
```

### Проверка здоровья

```bash
# Health check
docker exec scanner-tp-event-listener python -c "
from services.tp_event_listener import TPEventListener
import json
listener = TPEventListener()
health = listener.health_check()
print(json.dumps(health, indent=2))
"
```

### Redis метрики

```bash
# Длина stream
redis-cli XLEN events:trades

# Pending messages
redis-cli XPENDING events:trades tp1-trailing-group

# Профили трейлинга
redis-cli GET trailing:profiles | jq .
```

## 🔧 Конфигурация

### Environment Variables

```bash
# В docker-compose.tp-trailing.yml или .env

# Redis
REDIS_URL=redis://scanner-redis:6379/0
TP_EVENTS_STREAM=events:trades

# Профиль по умолчанию
DEFAULT_TRAIL_PROFILE=rocket_v1

# Gateway
GATEWAY_URL=http://scanner-go-gateway:8090

# Processing
TP_EVENTS_BATCH_SIZE=50
TP_EVENTS_BLOCK_MS=5000
```

### Создание кастомного профиля

```python
from services.trailing_profiles import TrailingProfile, TrailingProfilesRegistry

# Создаём реестр
registry = TrailingProfilesRegistry()

# Добавляем свой профиль
custom = TrailingProfile(
    name="eurusd_medium",
    mode="ATR",
    atr_mult=0.7,
    comment="Medium trailing for EUR/USD"
)

registry.add(custom, save_to_redis=True)
print(f"✅ Profile added: {custom.name}")

# Использование
signal = XAUUSDSignal(
    # ...
    trail_after_tp1=True,
    trail_profile="eurusd_medium"
)
```

## 🐛 Troubleshooting

### Трейлинг не активируется

```bash
# 1. Проверьте наличие сигнала
redis-cli GET signals:your-signal-id

# 2. Проверьте флаг trail_after_tp1
redis-cli GET signals:your-signal-id | jq .trail_after_tp1

# 3. Проверьте логи
docker logs scanner-tp-event-listener | grep "your-signal-id"
```

### События не приходят

```bash
# 1. Проверьте stream
redis-cli XLEN events:trades

# 2. Проверьте consumer group
redis-cli XINFO GROUPS events:trades

# 3. Если пусто - нужно генерировать события из MT5/gateway
```

### Gateway не отвечает

```bash
# Проверьте connectivity
docker exec scanner-tp-event-listener curl http://scanner-go-gateway:8090/health

# Проверьте логи gateway
docker logs scanner-go-gateway | grep "trail"
```

## 📚 Дополнительная документация

- **Полная документация**: [documentation/ticks/TP1_TRAILING_SYSTEM.md](documentation/ticks/TP1_TRAILING_SYSTEM.md)
- **Архитектура**: [documentation/ARCHITECTURE.md](documentation/ARCHITECTURE.md)
- **Конфигурация**: [python-worker/config/trailing_config.json](python-worker/config/trailing_config.json)

## 🎓 Примеры

### Пример 1: Интеграция в aggregated_signal_hub_v2.py

```python
# В методе write_and_push
result = self.writer.write_and_push(
    symbol=self.cfg.symbol,
    side=side,
    entry=mid,
    atr=atr,
    confidence=conf,
    reason=reason,
    source="AggregatedHub-V2",
    # 🎯 Добавляем трейлинг
    trail_after_tp1=True if conf > 0.60 else False,
    trail_profile="rocket_v1" if conf > 0.80 else "lock_and_trail"
)
```

### Пример 2: Интеграция в orderflow handler

```python
# В xau_orderflow_handler.py
signal = XAUUSDSignal(
    # ... обычные поля ...
    # 🎯 Трейлинг для сильных дельта-сигналов
    trail_after_tp1=True if abs(z_delta) > 4.5 else False,
    trail_profile="rocket_v1" if abs(z_delta) > 6.0 else "lock_and_trail"
)
```

### Пример 3: Динамический выбор на основе метрик

```python
def get_trailing_config(metrics: dict, market_state: dict) -> tuple[bool, str]:
    """
    Умный выбор конфигурации трейлинга.

    Returns:
        (enable_trailing, profile_name)
    """
    confidence = metrics.get('confidence', 0)
    z_delta = metrics.get('z_delta', 0)
    volatility = market_state.get('volatility', 'normal')

    # Отключаем для слабых сигналов
    if confidence < 50:
        return (False, "")

    # Агрессивный для экстремальных сигналов
    if confidence > 85 and abs(z_delta) > 5.5:
        return (True, "rocket_v1")

    # Консервативный для волатильных условий
    if volatility == "high":
        return (True, "wide_swing")

    # Базовый для обычных сигналов
    return (True, "lock_and_trail")


# Использование
enable, profile = get_trailing_config(metrics, market_state)

signal = XAUUSDSignal(
    # ...
    trail_after_tp1=enable,
    trail_profile=profile
)
```

## 💡 Best Practices

1. **Используйте трейлинг для сильных сигналов** (conf > 60%)
2. **Выбирайте профиль по волатильности** (rocket_v1 для трендовых, wide_swing для шумных)
3. **Тестируйте на истории** перед production
4. **Мониторьте метрики** TP1→TP2 vs TP1→SL
5. **Адаптируйте профили** под разные инструменты и режимы рынка

## ⚡ Performance Tips

- Listener обрабатывает до **1000 событий/сек**
- Latency от TP1_HIT до команды в gateway: **< 100ms**
- Можно запустить несколько реплик для load balancing
- Consumer group обеспечивает at-least-once delivery

## 🎉 Готово!

Система готова к использованию. Начните с тестовых сигналов, проверьте логи, затем включите для production сигналов.

**Happy Trading! 🚀**
