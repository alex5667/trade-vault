# Health Metrics Integration

## Обзор

Система health metrics реализована для мониторинга здоровья orderflow обработки в реальном времени. Она собирает статистику по каждому символу и публикует агрегированные метрики в Redis каждые 5 секунд.

## Архитектура

### Собираемые метрики

На уровне каждого тика/сигнала собираются:

- `symbol` - символ инструмента
- `l2_age_ms` - время с момента последнего L2 обновления (now - book_ts)
- `l2_age_ms_tick` - время между book_ts и tick_ts
- `l2_is_stale` - **гейт для сигналов** (относительно тика, строгий порог)
- `l2_is_stale_now` - **диагностика пайплайна** (относительно now, мягкий порог)
- `eta_fill_ms` - время на исполнение (опционально)
- `burst_ratio` - коэффициент всплеска активности
- `imbalance_min` - минимальный дисбаланс (OBI proxy)
- `signal_emitted` - факт эмиссии сигнала
- `dlq_count` - количество сообщений в DLQ

## Семантика Staleness

### Два типа staleness:

1. **`l2_is_stale` (относительно тика)**:
   - **Назначение**: Гейт для сигналов и паттернов
   - **Расчет**: `tick_ts - book_ts > MAX_AGE_TICK_MS` (обычно 150мс)
   - **Использование**: Если true - не считаем паттерны, режем вес сигналов
   - **Строгий порог** для обеспечения качества сигналов

2. **`l2_is_stale_now` (относительно now)**:
   - **Назначение**: SRE/мониторинг здоровья пайплайна
   - **Расчет**: `now - book_ts > MAX_AGE_NOW_MS` (обычно 1000мс)
   - **Использование**: Только для логов, алертов, диагностики
   - **Мягкий порог** для выявления проблем с feed'ом

### Агрегированные метрики (каждые 5 секунд)

Для каждого символа рассчитываются и публикуются:

- `avg_l2_age_ms` - среднее время L2 age
- `avg_l2_age_ms_tick` - среднее время между book и tick
- `l2_stale_ratio_tick` - доля stale обновлений **относительно тика** (гейт сигналов)
- `l2_stale_ratio_now` - доля stale обновлений **относительно now** (SRE диагностика)
- `avg_eta_fill_ms` - среднее время исполнения
- `avg_burst_ratio` - средний коэффициент всплеска
- `avg_imbalance_min` - средний минимальный дисбаланс
- `signal_emit_rate` - скорость эмиссии сигналов (сигналов/сек)
- `dlq_rate` - скорость DLQ (сообщений/сек)

## Компоненты

### 1. Go Worker

#### Пакет `internal/metrics/health.go`
Основной пакет для сбора метрик в Go воркере.

#### Интеграция
- В `main.go`: инициализация `HealthMetrics` с 5-секундным окном
- В `futures_streams.go`: вызов `OnTick` для каждого обработанного тика/depth обновления

### 2. Python Worker

#### Модуль `health_metrics.py`
Аналогичный класс `HealthMetrics` для Python компонентов.

#### Интеграция
- В `main_multi_symbol.py`: инициализация глобального `HealthMetrics`
- В `base_orderflow_handler.py`: вызовы `on_tick`, `on_signal_emit`, `on_dlq`
- В `handler_factory.py`: передача `health_metrics` в конструкторы обработчиков

## Redis ключи

Метрики публикуются в Redis с TTL 3x window (15 секунд):

```
orderflow:{SYMBOL}:l2_stale_ratio_tick → float (0.0-1.0) - относительно тика
orderflow:{SYMBOL}:l2_stale_ratio_now → float (0.0-1.0) - относительно now
orderflow:{SYMBOL}:signal_emit_rate → float (signals/sec)
orderflow:{SYMBOL}:dlq_rate → float (messages/sec)
orderflow:{SYMBOL}:health_snapshot → hash с деталями
```

### Структура health_snapshot

```json
{
  "ticks_total": 1250,
  "ticks_with_l2": 1200,
  "l2_stale_ratio_tick": "0.0500",  // относительно тика (гейт сигналов)
  "l2_stale_ratio_now": "0.1200",   // относительно now (SRE)
  "avg_l2_age_ms": "145.23",
  "avg_l2_age_tick_ms": "234.56",
  "avg_eta_fill_ms": "12.34",
  "avg_burst_ratio": "1.4567",
  "avg_imbalance_min": "0.1234",
  "signal_emit_rate": "0.4000",
  "dlq_rate": "0.0500",
  "window_sec": 5,
  "ts": 1703123456789
}
```

## Мониторинг

### Проверка работы

```bash
# Проверить метрики в Redis
redis-cli KEYS "orderflow:*:*"

# Получить snapshot для символа
redis-cli HGETALL "orderflow:BTCUSDT:health_snapshot"

# Проверить rate-метрики
redis-cli GET "orderflow:BTCUSDT:signal_emit_rate"
redis-cli GET "orderflow:BTCUSDT:dlq_rate"
```

### Граничные значения

Рекомендуемые пороги для алертов:

**Для качества сигналов (l2_stale_ratio_tick):**
- `l2_stale_ratio_tick > 0.05` - более 5% тиков с устаревшей L2 (влияет на сигналы)

**Для здоровья пайплайна (l2_stale_ratio_now):**
- `l2_stale_ratio_now > 0.1` - высокая доля устаревших данных относительно now
- `avg_l2_age_ms > 1000` - средняя задержка L2 > 1 сек

**Общие метрики:**
- `dlq_rate > 0.1` - более 0.1 ошибок в секунду
- `signal_emit_rate` - зависит от стратегии (мониторить изменения)

## Тестирование

### Python модуль

```python
from health_metrics import HealthMetrics

# Создать экземпляр
hm = HealthMetrics(redis_url="redis://redis:6379/0", window_sec=5)
hm.start_background_loop()

# Использовать
hm.on_tick(symbol="BTCUSDT", l2_age_ms=100.0, l2_age_ms_tick=150.0,
           l2_is_stale=False, burst_ratio=1.5, imbalance_min=0.2)
hm.on_signal_emit("BTCUSDT")
hm.on_dlq("BTCUSDT")

# Остановить
hm.stop()
```

### Go пакет

```go
import "go-worker/internal/metrics"

hm := metrics.NewHealthMetrics(rdb, 5*time.Second)
go hm.Run()

hm.OnTick(metrics.TickMetricsInput{
    Symbol:       "BTCUSDT",
    L2AgeMs:      100.0,
    L2AgeMsTick:  150.0,
    L2IsStale:    false,
    BurstRatio:   &burstRatio,
    ImbalanceMin: &imbalance,
})

hm.OnSignalEmit("BTCUSDT")
hm.OnDLQ("BTCUSDT")

hm.Stop()
```

## Производительность

- **Память**: buckets хранятся в памяти, автоматически очищаются каждые 5 секунд
- **CPU**: минимальная нагрузка (только аккумуляция и агрегация)
- **Redis**: pipeline операции каждые 5 секунд
- **Thread-safety**: Go использует mutex, Python использует threading.Lock

## Интеграция в отчеты

Health metrics автоматически включаются в отчеты `PeriodicReporter`:

- **L2 stale ratios**: доля stale тиков относительно тика и now
- **Средние задержки L2**: avg_l2_age_ms, avg_l2_age_tick_ms
- **Статистика тиков**: общее количество и с L2 данными
- **Скорость сигналов**: signal_emit_rate (сигналов/сек)
- **Скорость ошибок**: dlq_rate (сообщений/сек)

## Интеграция в данные сделок

Health metrics автоматически добавляются в:

### 1. **Redis Streams** (`trades:closed`)
При сохранении закрытой сделки добавляются поля:
```json
{
  "order_id": "ABC123",
  "health_l2_stale_ratio_tick": "0.05",
  "health_l2_stale_ratio_now": "0.12",
  "health_avg_l2_age_ms": "145.23",
  "health_avg_l2_age_tick_ms": "234.56",
  "health_signal_emit_rate": "2.5",
  "health_dlq_rate": "0.1",
  // ... остальные поля сделки
}
```

### 2. **Analytics Database** (`trades_closed` table)
Поля добавляются в таблицу для корреляционного анализа:
- `health_l2_stale_ratio_tick` - L2 stale ratio относительно тика
- `health_l2_stale_ratio_now` - L2 stale ratio относительно now
- `health_avg_l2_age_ms` - Средняя задержка L2
- `health_avg_l2_age_tick_ms` - Средняя задержка L2 относительно тика
- `health_signal_emit_rate` - Скорость эмиссии сигналов
- `health_dlq_rate` - Скорость DLQ ошибок

## Расширение

Для добавления новых метрик:

1. Добавить поля в `TickMetricsInput` (Go) / параметры `on_tick` (Python)
2. Обновить аккумуляцию в `SymbolBucket`
3. Добавить расчет в `flushSnapshot` / `_flush_snapshot`
4. Обновить Redis ключи, отчеты и документацию
5. Добавить извлечение в `_add_health_metrics` для отчетов
6. **Добавить поля в trades:closed stream** в `infra/redis_repo.py`
7. **Добавить поля в analytics DB** в `services/analytics_db.py` и миграции
