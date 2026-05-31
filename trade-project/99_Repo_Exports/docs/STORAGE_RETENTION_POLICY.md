# Политика хранения данных: signals:of:inputs, trades:closed, ml_replay_inputs_v1

## Обзор

Документ описывает сроки хранения и варианты хранения для ключевых Redis streams, используемых в ML pipeline и orderflow стратегии.

---

## 1. signals:of:inputs

### Назначение
Redis stream, хранящий OFInputsV1 записи (snapshot индикаторов на момент принятия решения) для golden replay и анализа.

### Структура данных
- **Тип**: Redis Stream
- **Поле**: `payload` (JSON строка с OFInputsV1)
- **Формат**: Canonical JSON (deterministic, sort_keys=True, separators=(",", ":"))

### Сроки хранения

#### Redis Stream (in-memory)
- **maxlen**: `200000` (по умолчанию) или `5000000` (если установлен `OF_INPUTS_STREAM_MAXLEN`)
- **Режим**: `approximate=True` (Redis автоматически обрезает старые записи)
- **Приблизительный срок**: 
  - При maxlen=200000: ~2-7 дней (зависит от частоты сигналов)
  - При maxlen=5000000: ~50-175 дней

#### Персистентное хранение (архив)
- **Включение**: `OF_INPUTS_PERSIST_ENABLE=1`
- **Путь**: `OF_INPUTS_PERSIST_PATH=/var/lib/trade/of_inputs_archive`
- **Батчинг**: `OF_INPUTS_PERSIST_BATCH=1000` (записей на файл)
- **Формат**: NDJSON (один JSON объект на строку)
- **Срок**: Неограничен (файловая система)

### Варианты хранения

1. **Redis Stream (реaltime)**
   - Использование: Golden replay, экспорт для обучения
   - Инструменты: `tools.export_of_inputs_ndjson`, `tools.of_engine_replay_from_inputs`
   - Ограничение: maxlen (автоматическая обрезка старых записей)

2. **Файловый архив (NDJSON)**
   - Использование: Долгосрочное хранение, бэкапы, исторический анализ
   - Формат: `.ndjson` файлы
   - Экспорт: `tools.export_of_inputs_ndjson --out /path/to/archive.ndjson`

3. **S3/Object Storage (рекомендуется для production)**
   - Периодический экспорт из Redis → S3
   - Формат: NDJSON, сжатие gzip
   - Retention: По политике компании (обычно 1-2 года для ML datasets)

### Конфигурация

```bash
# Redis stream
OF_INPUTS_STREAM=signals:of:inputs
OF_INPUTS_STREAM_MAXLEN=5000000  # или 200000 по умолчанию

# Персистентность
OF_INPUTS_PERSIST_ENABLE=1
OF_INPUTS_PERSIST_PATH=/var/lib/trade/of_inputs_archive
OF_INPUTS_PERSIST_BATCH=1000
```

### Рекомендации

- **Production**: maxlen=5000000 + персистентность в S3 (еженедельный экспорт)
- **Development**: maxlen=200000 + локальный архив
- **Golden replay**: Использовать экспортированные NDJSON файлы для детерминированности

---

## 2. trades:closed

### Назначение
Redis stream, хранящий закрытые сделки (outcomes/labels) для join с signals при построении ML datasets.

### Структура данных
- **Тип**: Redis Stream
- **Поля**: 
  - `sid` (signal ID для join)
  - `pnl`, `risk_usd` (outcomes)
  - `symbol`, `direction`, `scenario`
  - `health_*` метрики
- **Режим**: Compact mode (минимальный payload) или full mode

### Сроки хранения

#### Redis Stream (in-memory)
- **maxlen**: `50000` (по умолчанию, `TRADES_CLOSED_STREAM_MAXLEN`)
- **Режим**: `approximate=True`
- **Приблизительный срок**: ~5-15 дней (зависит от частоты закрытий)

#### ZSET Index (опционально)
- **Включение**: `ENABLE_CLOSED_ZSET_INDEX=1`
- **Retention**: `CLOSED_ZSET_RETENTION_DAYS` (по умолчанию неограничен)
- **Использование**: Быстрый поиск по временному окну (ZRANGEBYSCORE)

### Варианты хранения

1. **Redis Stream (realtime)**
   - Использование: Join с signals для dataset building, reporting
   - Инструменты: `ml_analysis.tools.build_edge_stack_dataset_from_redis`
   - Ограничение: maxlen=50000

2. **Файловый архив (NDJSON)**
   - Использование: Долгосрочное хранение outcomes
   - Формат: `.ndjson` файлы
   - Экспорт: `tools.export_trade_closed_ndjson --stream trades:closed --out /path/to/archive.ndjson`

3. **PostgreSQL/TimescaleDB (рекомендуется для production)**
   - Периодический импорт из Redis → PostgreSQL
   - Retention: Неограничен (partitioning по месяцам)
   - Использование: Исторический анализ, backtesting, compliance

### Конфигурация

```bash
# Redis stream
TRADES_CLOSED_STREAM=trades:closed
TRADES_CLOSED_STREAM_MAXLEN=50000

# Compact mode (минимальный payload, детали в order:{id})
TRADES_CLOSED_STREAM_COMPACT=1

# ZSET index (опционально)
ENABLE_CLOSED_ZSET_INDEX=1
CLOSED_ZSET_RETENTION_DAYS=90
```

### Рекомендации

- **Production**: maxlen=50000 + PostgreSQL архив (ежедневный импорт)
- **Development**: maxlen=50000 + локальный NDJSON экспорт
- **Dataset building**: Использовать Redis stream для join с ml_replay_inputs_v1

---

## 3. ml_replay_inputs_v1

### Назначение
Redis stream, хранящий ML replay inputs (feature snapshots с индикаторами + sid) для построения train datasets. Используется **вместо** `signals:of:inputs` для ML pipeline.

### Структура данных
- **Тип**: Redis Stream
- **Поле**: `payload` (JSON с indicators, sid, cfg, rule_score, etc.)
- **Ключ для join**: `sid` (canonical signal ID)

### Сроки хранения

#### Redis Stream (in-memory)
- **maxlen**: `200000` (по умолчанию, `ML_REPLAY_INPUTS_MAXLEN`)
- **Режим**: `approximate=True`
- **Приблизительный срок**: ~2-7 дней
- **Sampling**: `ML_REPLAY_INPUTS_SAMPLE=0.01` (1% по умолчанию, стабильный sampling по sid)

### Варианты хранения

1. **Redis Stream (realtime)**
   - Использование: Join с `trades:closed` для dataset building
   - Инструменты: `ml_analysis.tools.build_edge_stack_dataset_from_redis`
   - Ограничение: maxlen=200000

2. **Файловый архив (NDJSON)**
   - Использование: Долгосрочное хранение для retraining
   - Формат: `.ndjson` файлы
   - Экспорт: Через `build_edge_stack_dataset_from_redis` (output JSONL)

3. **S3/Object Storage (рекомендуется для production)**
   - Периодический экспорт из Redis → S3
   - Формат: NDJSON (после join с trades:closed)
   - Retention: 1-2 года (для ML model retraining)

### Конфигурация

```bash
# Redis stream
ML_REPLAY_STREAM=ml_replay_inputs_v1
ML_REPLAY_INPUTS_MAXLEN=200000
ML_REPLAY_INPUTS_SAMPLE=0.01  # 1% sampling

# Dataset building
ML_REPLAY_STREAM=ml_replay_inputs_v1
TRADES_CLOSED_STREAM=trades:closed
```

### Рекомендации

- **Production**: maxlen=200000 + S3 архив (после join с trades:closed)
- **Development**: maxlen=200000 + локальный JSONL экспорт
- **Dataset building**: Использовать `ml_replay_inputs_v1` (не `signals:of:inputs`) для join с `trades:closed`

---

## Сравнительная таблица

| Stream | Назначение | Redis maxlen | Приблизительный срок | Архив |
|--------|-----------|--------------|---------------------|-------|
| `signals:of:inputs` | OFInputsV1 для golden replay | 200000-5000000 | 2-175 дней | NDJSON, S3 |
| `trades:closed` | Outcomes (labels) для ML | 50000 | 5-15 дней | NDJSON, PostgreSQL |
| `ml_replay_inputs_v1` | ML features для dataset building | 200000 | 2-7 дней | JSONL (после join), S3 |

---

## Миграция: signals:of:inputs → ml_replay_inputs_v1

### Контекст
- `signals:of:inputs` используется для golden replay OF engine
- `ml_replay_inputs_v1` используется для ML dataset building (join с trades:closed)

### Когда использовать что

1. **Golden replay OF engine**: `signals:of:inputs`
   - Инструменты: `tools.of_engine_replay_from_inputs`
   - Формат: OFInputsV1 (минимальный deterministic snapshot)

2. **ML dataset building**: `ml_replay_inputs_v1`
   - Инструменты: `ml_analysis.tools.build_edge_stack_dataset_from_redis`
   - Формат: ML replay inputs (indicators + sid + cfg + rule_score)
   - Join: С `trades:closed` по `sid`

### Рекомендация
- **Не использовать** `signals:of:inputs` для ML dataset building
- **Использовать** `ml_replay_inputs_v1` для всех ML-related операций

---

## Best Practices

### 1. Мониторинг размера streams
```bash
# Проверка размера
redis-cli XLEN signals:of:inputs
redis-cli XLEN trades:closed
redis-cli XLEN ml_replay_inputs_v1
```

### 2. Экспорт перед обрезкой
- Регулярный экспорт в NDJSON/S3 перед достижением maxlen
- Автоматизация через cron/timer services

### 3. Retention policy
- **Hot data** (Redis): 2-7 дней (maxlen-based)
- **Warm data** (S3/PostgreSQL): 1-2 года
- **Cold data** (long-term archive): По политике компании

### 4. Dataset building workflow
1. Экспорт `ml_replay_inputs_v1` + `trades:closed` из Redis
2. Join по `sid` → JSONL dataset
3. Архив dataset в S3
4. Использование для training/validation

---

## Компонент
**Infra / Data Storage**

## Goal
Документировать сроки хранения и варианты хранения для ключевых Redis streams.

## Constraints
- Redis memory limits
- Disk space для архивов
- Latency для realtime операций

## Inputs/Outputs
- **Inputs**: Redis streams (signals:of:inputs, trades:closed, ml_replay_inputs_v1)
- **Outputs**: Архивы (NDJSON, JSONL, S3, PostgreSQL)

## "Done" criteria
✅ Документация создана с полным описанием retention policies и storage options

