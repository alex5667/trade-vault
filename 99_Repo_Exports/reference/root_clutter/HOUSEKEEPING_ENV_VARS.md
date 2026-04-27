# Переменные окружения для Housekeeping

## Обзор

Документ описывает переменные окружения для настройки механизма housekeeping в `SignalPerformanceTracker`.

## Переменные окружения

### ORDERFLOW_EXPIRY_BARS

**Описание**: Количество баров, после которого сигнал протухает, если не было входа.

**Тип**: `int`

**По умолчанию**: `60`

**Рекомендуемые значения**:
- Для 1m баров: `60` (1 час)
- Для 5m баров: `12` (1 час)
- Для 15m баров: `4` (1 час)

**Пример**:
```bash
ORDERFLOW_EXPIRY_BARS=60
```

---

### ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY

**Описание**: Максимальное количество баров, которое позиция может оставаться открытой после входа без выхода. После истечения TTL позиция финализируется как `EXPIRED_NO_TARGET`.

**Тип**: `int`

**По умолчанию**: `max(3 * ORDERFLOW_EXPIRY_BARS, 180)`

**Рекомендуемые значения**:
- Для агрессивной стратегии: `120` (2 часа)
- Для стандартной стратегии: `180` (3 часа)
- Для консервативной стратегии: `360` (6 часов)

**Пример**:
```bash
ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
```

**Важно**: Это основной параметр для предотвращения утечек памяти. Должен быть достаточно большим, чтобы не финализировать нормальные позиции, но достаточно малым, чтобы очищать зависшие.

---

### ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY

**Описание**: Fallback TTL по времени (в миллисекундах) для случаев, когда bar-индексы недоступны. Если `0`, то используется только баровый TTL.

**Тип**: `int`

**По умолчанию**: `0` (отключен)

**Рекомендуемые значения**:
- Отключен (используем баровый TTL): `0`
- 1 час: `3600000`
- 3 часа: `10800000`
- 6 часов: `21600000`

**Пример**:
```bash
# Отключен (рекомендуется)
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0

# Или fallback 3 часа
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=10800000
```

**Важно**: Рекомендуется использовать баровый TTL (`ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY`) как основной, а временной TTL оставить отключенным или использовать как fallback.

---

### ORDERFLOW_HOUSEKEEPING_EVERY_MS

**Описание**: Частота вызова housekeeping (в миллисекундах). Определяет, как часто проверяются зависшие позиции. Меньшее значение = более частые проверки = больше нагрузка.

**Тип**: `int`

**По умолчанию**: `1000` (1 секунда)

**Рекомендуемые значения**:
- Для тестирования: `0` (каждый вызов `on_bar`)
- Для production (низкая нагрузка): `1000` (1 секунда)
- Для production (высокая нагрузка): `5000` (5 секунд)

**Пример**:
```bash
ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

**Важно**: Слишком частые вызовы (< 1000 мс) могут создать дополнительную нагрузку на систему. Для большинства случаев достаточно 1-5 секунд.

---

## Примеры конфигураций

### Production (стандартная стратегия)

```yaml
environment:
  - ORDERFLOW_EXPIRY_BARS=60
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

### Production (агрессивная стратегия)

```yaml
environment:
  - ORDERFLOW_EXPIRY_BARS=30
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=90
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

### Production (консервативная стратегия)

```yaml
environment:
  - ORDERFLOW_EXPIRY_BARS=120
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=360
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=5000
```

### Development/Testing

```yaml
environment:
  - ORDERFLOW_EXPIRY_BARS=10
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=20
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=0
```

---

## Добавление в docker-compose.yml

### Пример для python-worker

```yaml
services:
  python-worker:
    image: scanner_infra-python-worker
    environment:
      # ... существующие переменные ...
      
      # NEW: Housekeeping для SignalPerformanceTracker
      - ORDERFLOW_EXPIRY_BARS=60
      - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
      - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
      - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

### Пример для crypto-orderflow-handler

```yaml
services:
  crypto-orderflow-handler:
    image: scanner_infra-python-worker
    environment:
      # ... существующие переменные ...
      
      # NEW: Housekeeping для SignalPerformanceTracker
      - ORDERFLOW_EXPIRY_BARS=60
      - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
      - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
      - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

---

## Мониторинг и отладка

### Проверка текущих значений

В логах при старте handler должны появиться значения:

```
Handler initialized: ... | expiry_bars=60 | max_lifetime_bars=180 | housekeeping_every_ms=1000
```

### Логирование финализаций

Рекомендуется добавить логирование в `_housekeep_expired`:

```python
if finalized_count > 0:
    logger.info(
        f"Housekeeping: finalized {finalized_count} expired positions "
        f"(active={len(self._states)}, lru_size={len(self._finalized_lru)})"
    )
```

### Метрики для мониторинга

1. **Количество активных states**: должно стабилизироваться
2. **Количество финализаций по TTL**: должно быть минимальным в нормальных условиях
3. **Размер LRU**: должен быть стабильным (< 4096)

---

## Troubleshooting

### Проблема: Слишком много финализаций по TTL

**Симптомы**: В логах частые сообщения "expired_no_target: held_bars=..."

**Причины**:
1. `ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY` слишком мал
2. Exit события не доходят до tracker
3. Проблемы с обработкой execution событий

**Решение**:
1. Увеличить `ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY`
2. Проверить логику обработки exit событий
3. Добавить логирование в `on_execution_event`

---

### Проблема: Утечка памяти продолжается

**Симптомы**: `len(tracker._states)` продолжает расти

**Причины**:
1. `bar_idx` не передается в методы
2. Housekeeping не вызывается (слишком большой `ORDERFLOW_HOUSEKEEPING_EVERY_MS`)
3. Позиции создаются быстрее, чем финализируются

**Решение**:
1. Убедиться, что `bar_idx` передается в `on_bar` и `on_execution_event`
2. Уменьшить `ORDERFLOW_HOUSEKEEPING_EVERY_MS` до 1000
3. Проверить логику создания позиций

---

### Проблема: Нормальные позиции финализируются преждевременно

**Симптомы**: Позиции финализируются до достижения TP/SL

**Причины**:
1. `ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY` слишком мал
2. Exit события не обрабатываются корректно

**Решение**:
1. Увеличить `ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY`
2. Проверить обработку exit событий
3. Добавить логирование в `on_execution_event`

---

## Рекомендации по настройке

### Для 1m баров (стандартный случай)

```bash
ORDERFLOW_EXPIRY_BARS=60                           # 1 час до входа
ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180        # 3 часа после входа
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0            # Отключен
ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000               # 1 секунда
```

### Для 5m баров

```bash
ORDERFLOW_EXPIRY_BARS=12                           # 1 час до входа
ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=36         # 3 часа после входа
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0            # Отключен
ORDERFLOW_HOUSEKEEPING_EVERY_MS=5000               # 5 секунд
```

### Для высокочастотной торговли (< 1m)

```bash
ORDERFLOW_EXPIRY_BARS=120                          # 2 минуты до входа (для 1s баров)
ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=600        # 10 минут после входа
ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=600000       # Fallback 10 минут
ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000               # 1 секунда
```

---

## Проверка работы

### 1. Запуск тестов

```bash
cd /home/alex/front/trade/scanner_infra
python -m pytest tests/test_expired_no_target.py -v
```

### 2. Проверка логов

```bash
# Проверить логи на наличие housekeeping
docker logs python-worker 2>&1 | grep -i "housekeeping"

# Проверить финализации
docker logs python-worker 2>&1 | grep -i "expired_no_target"
```

### 3. Мониторинг памяти

```bash
# Проверить использование памяти
docker stats python-worker

# Должно стабилизироваться после нескольких часов работы
```

---

## Заключение

Правильная настройка переменных окружения критична для корректной работы housekeeping механизма. Рекомендуется:

1. Начать с рекомендуемых значений
2. Мониторить логи и метрики
3. Настроить под конкретную стратегию
4. Регулярно проверять размер `_states`

При возникновении проблем - обращайтесь к разделу Troubleshooting.

