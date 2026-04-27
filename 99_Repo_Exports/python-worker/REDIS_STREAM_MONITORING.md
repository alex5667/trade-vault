# Redis Stream Monitoring & Health Metrics

## Обзор

Система мониторинга Redis streams предоставляет полную видимость здоровья потоков данных в реальном времени, включая метрики лагов и pending сообщений.

## Метрики

### Stream Lag Metrics (средние значения за окно)
- `orderflow:{symbol}:book_lag_ms` — средний лаг L2 book данных
- `orderflow:{symbol}:ticks_lag_ms` — средний лаг tick данных
- `orderflow:{symbol}:l3_lag_ms` — средний лаг L3 данных

### Pending Length Metrics (текущие значения)
- `orderflow:{symbol}:pending_book_len` — количество pending сообщений в book stream
- `orderflow:{symbol}:pending_ticks_len` — количество pending сообщений в ticks stream
- `orderflow:{symbol}:pending_l3_len` — количество pending сообщений в l3 stream

### Health Snapshot (JSON в Redis hash)
```json
{
  "avg_book_lag_ms": "150.5",
  "avg_ticks_lag_ms": "45.2",
  "avg_l3_lag_ms": "0.0",
  "pending_book_len": 12,
  "pending_ticks_len": 8,
  "pending_l3_len": 0,
  "ts": 1703123456789,
  "window_sec": 5
}
```

## Архитектура

### SyncRedisStreamHelper.pending_len()
- Безопасно получает количество pending сообщений по XPENDING
- Обрабатывает NOGROUP ошибки (consumer group еще не создана)
- Возвращает 0 при ошибках, чтобы не ломать main loop

### MainLoopService._sample_pending()
- Периодический сэмплинг каждые 2 секунды (настраивается через `PENDING_SAMPLE_EVERY_MS`)
- Минимально-инвазивная интеграция в основной цикл чтения

### MessageHandler.process_message_batch()
- Измеряет лаг для каждого сообщения в batch
- Передает метрики в HealthMetrics.on_stream_lag()

### HealthMetrics
- Агрегирует метрики по окнам (по умолчанию 5 секунд)
- Публикует в Redis отдельные ключи + health_snapshot hash
- TTL = window_sec * 3 для автоматической очистки

## Приоритеты обработки

### MessageHandler (book → l3 → ticks)
```
Приоритет: book=0, l3=1, ticks=2
Обработка: book → l3 → ticks (независимо от входного порядка)
```

### MainLoopService (с квотами)
```
Чтение: book(60) → l3(20) → ticks(50 если есть другие, иначе 120)
Блокировка: book до 200ms, остальные без блокировки
```

## Конфигурация

### Environment Variables
- `PENDING_SAMPLE_EVERY_MS` — интервал сэмплинга pending (default: 2000ms)

### HealthMetrics параметры
- `window_sec` — окно агрегации (default: 5 секунд)
- `redis_url` — URL Redis для публикации метрик

## Тестирование

```bash
# Запуск всех тестов Redis monitoring
pytest tests/test_redis_stream_consumer_pending.py \
       tests/test_health_metrics_streams.py \
       tests/test_message_handler_priority.py \
       tests/test_main_loop_priority_read.py -v
```

### Что тестируется:
- ✅ Парсинг XPENDING ответов (dict/tuple/unknown)
- ✅ pending_len с NOGROUP обработкой
- ✅ HealthMetrics агрегация и публикация
- ✅ Приоритет book → l3 → ticks в MessageHandler
- ✅ Квоты и порядок чтения в MainLoopService
- ✅ Pending сэмплинг в HealthMetrics

## Мониторинг и алертинг

### Рекомендуемые алерты:
- `avg_book_lag_ms > 5000` — критический лаг L2 данных
- `pending_book_len > 1000` — накопление pending в book stream
- `avg_ticks_lag_ms > 10000` — лаг tick данных

### Графики:
- Book lag trend для оценки качества L2 данных
- Pending lengths для выявления bottleneck'ов
- Корреляция лагов с pending для анализа производительности

## Безопасность

- Метрики не влияют на основной поток обработки
- Graceful handling ошибок Redis
- TTL для автоматической очистки устаревших данных
- Thread-safe агрегация в HealthMetrics
