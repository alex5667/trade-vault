# StreamWorker Integration - Единый каркас для обработки Redis Streams

## Обзор

Реализован единый `StreamWorker` для обработки Redis Streams с поддержкой:
- **Lossless режим**: сообщения не теряются при падениях
- **Realtime режим**: быстрая обработка, ACK всегда
- **Автоматический drain pending**: обработка собственных pending сообщений
- **Claim orphan pending**: забор "осиротевших" сообщений других consumer'ов
- **Retry механизм**: автоматические повторы с DLQ (Dead Letter Queue)

## Структура

### 1. `services/stream_worker.py`

Единый каркас `StreamWorker` с классом `WorkerPolicy` для настройки политики обработки.

**Основные компоненты:**
- `WorkerPolicy`: настройка режима работы (lossless/realtime), параметры retry, DLQ
- `StreamWorker`: основной воркер с поддержкой retry/pending-claim
- `ProcessFn`: тип функции-процессора `(stream, msg_id, data) -> bool`

### 2. Обновлен `signal_performance_tracker.py`

**Добавлены вспомогательные функции:**
- `_extract_json_payload()`: извлечение JSON payload из сообщения
- `_merge_data_field()`: мерж поля 'data' (JSON) с основными полями

**Добавлены processor-функции:**
- `_process_signal_message()`: обработка сигналов (lossless)
- `_process_tick_message()`: обработка тиков (realtime)
- `_process_event_message()`: обработка событий (lossless)

**Заменены listener threads:**
- `_signals_listener_thread()` → использует `StreamWorker` (lossless)
- `_ticks_listener_thread()` → использует `StreamWorker` (realtime)
- `_events_listener_thread()` → использует `StreamWorker` (lossless)

## Преимущества

### 1. Lossless для signals/events
- Сообщения не теряются при падениях
- Pending автоматически вычищаются
- "Осиротевшие" pending возвращаются в обработку

### 2. Poison-pill защита
- После N попыток сообщение отправляется в DLQ и ACK
- Поток не блокируется на проблемных сообщениях

### 3. Единая архитектура
- Один и тот же каркас для всех потоков
- Разные processors, но единая логика retry/pending-claim

### 4. Единый parsing pipeline
- `_extract_json_payload`, `_merge_data_field`
- Один стиль обработки, меньше багов и расхождений

## Конфигурация через ENV

### Signals (lossless)
```bash
SIGNALS_ACK_MODE=lossless
SIGNALS_READ_COUNT=20
SIGNALS_BLOCK_MS=5000
SIGNALS_DRAIN_PENDING_EVERY_S=10
SIGNALS_CLAIM_ORPHAN_EVERY_S=60
SIGNALS_MIN_IDLE_MS=60000
SIGNALS_MAX_ATTEMPTS=5
SIGNALS_DLQ_STREAM=dlq:signals
```

### Ticks (realtime)
```bash
TICKS_ACK_MODE=realtime
TICKS_READ_COUNT=200
TICKS_BLOCK_MS=1000
TICKS_DLQ_STREAM=dlq:ticks
```

### Events (lossless)
```bash
EVENTS_ACK_MODE=lossless
EVENTS_READ_COUNT=50
EVENTS_BLOCK_MS=2000
EVENTS_DRAIN_PENDING_EVERY_S=10
EVENTS_CLAIM_ORPHAN_EVERY_S=60
EVENTS_MIN_IDLE_MS=60000
EVENTS_MAX_ATTEMPTS=8
EVENTS_DLQ_STREAM=dlq:events
```

## Использование

### Пример создания воркера

```python
from services.stream_worker import StreamWorker, WorkerPolicy

policy = WorkerPolicy(
    ack_mode="lossless",
    read_count=50,
    block_ms=2000,
    drain_pending_every_s=10,
    claim_orphan_every_s=60,
    min_idle_ms=60_000,
    max_attempts=5,
    dlq_stream="dlq:my_worker",
)

worker = StreamWorker(
    name="my_worker",
    client=redis_client,
    group="my_group",
    consumer="my_consumer",
    build_streams=lambda: ["stream:1", "stream:2"],
    process=my_processor,
    policy=policy,
    logger=logger,
)

worker.run_loop(lambda: running_flag)
```

## DLQ (Dead Letter Queue)

Сообщения, которые не удалось обработать после `max_attempts` попыток, отправляются в DLQ stream.

**Формат DLQ сообщения:**
```json
{
  "ts": 1234567890,
  "worker": "signals_listener",
  "group": "signal-tracker-group",
  "consumer": "tracker-1234567890",
  "stream": "signals:ta:XAUUSD",
  "msg_id": "1234567890-0",
  "attempts": 5,
  "error": "max_attempts",
  "data": "{...original message data...}"
}
```

## Мониторинг

Health callback вызывается для каждого воркера:
```python
health_cb=lambda comp, status, extra: self._update_health_status(comp, status=status, extra=extra)
```

**Статусы:**
- `ok`: нормальная работа
- `error`: ошибка обработки
- `stopped`: воркер остановлен

**Extra данные:**
- `batch`: количество обработанных сообщений
- `streams`: количество отслеживаемых streams
- `reason`: причина ошибки (если есть)

## Логирование

Воркер логирует:
- Старт/остановку воркера
- Ошибки обработки с номером попытки
- Drain pending сообщений
- Claim orphan pending сообщений
- Ошибки Redis

## Deduplication (защита от повторной обработки)

### Проблема
При lossless режиме (pending + reclaim) возможны дубли и повторные сайд-эффекты:
- Повторное открытие позиций
- Повторные уведомления
- Повторное применение трейлинга

### Решение: RedisDeduper
Использует Redis `SET key value NX EX ttl` для атомарной проверки:
- `True` => первая обработка, можно выполнять сайд-эффекты
- `False` => дубликат, сайд-эффекты запрещены, но msg нужно ACK-нуть

### Реализация

**1. Создан `services/deduper.py`:**
- `RedisDeduper`: класс для дедупликации
- `env_int()`: helper для чтения ENV переменных

**2. Интегрирован в `signal_performance_tracker.py`:**
- `_signal_dedup_key()`: формирует ключ по `sid` или `(stream, msg_id)`
- `_event_dedup_key()`: формирует ключ по `(event_type, sid)` или `msg_id`
- Dedup gate в `_process_signal_message()` и `_process_event_message()`

**3. TTL конфигурация:**
```bash
DEDUP_PREFIX=dedup
DEDUP_SIGNALS_TTL_S=172800  # 2 дня
DEDUP_EVENTS_TTL_S=604800   # 7 дней
DEDUP_REPORT_TTL_S=21600    # 6 часов
```

**4. Статистика:**
- `dedup_signals_skipped`: количество пропущенных дубликатов сигналов
- `dedup_events_skipped`: количество пропущенных дубликатов событий
- `dedup_reports_skipped`: количество пропущенных дубликатов отчетов

### Выбор ключей дедупликации

**Signals:**
- Приоритет: `sid` (бизнес-идентификатор)
- Fallback: `(stream, msg_id)` (технический идентификатор)

**Events:**
- Приоритет: `(event_type, sid)` (событие + бизнес-идентификатор)
- Fallback: `msg_id` (технический идентификатор)

**Почему `sid` важнее `msg_id`:**
- `msg_id` уникален только внутри конкретного Redis Stream
- Один и тот же сигнал может прилетать повторно (повторная публикация, другой stream, pending-replay)
- `sid` лучше отражает "одно и то же событие" с точки зрения домена

## Совместимость

Код полностью обратно совместим:
- Все существующие функции сохранены
- Логика обработки не изменилась
- Только внутренняя реализация listener threads заменена на StreamWorker
- Добавлена защита от дубликатов (dedup)

## Тестирование

После интеграции рекомендуется:
1. Проверить обработку сигналов
2. Проверить обработку тиков
3. Проверить обработку событий (TRAILING_STARTED, SL_HIT)
4. Проверить DLQ при ошибках обработки
5. Проверить drain pending после перезапуска
6. Проверить dedup: отправить дубликат сигнала и убедиться, что он пропущен
7. Проверить статистику dedup в логах

