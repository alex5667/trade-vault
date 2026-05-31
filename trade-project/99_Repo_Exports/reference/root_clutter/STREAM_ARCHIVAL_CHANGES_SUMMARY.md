# Stream Archival Implementation - Complete Changes Summary

**Дата:** 2026-01-27  
**Команда:** Trade Project Expert Team (Financial Analysts, Senior Trading Systems Analysts, Senior Python/Go/TypeScript Engineers, PostgreSQL DBAs, DevOps/SREs, Statistics Professors - 20+ years each)

---

## Executive Summary

**Цель:** Надежное архивирование критически важных Redis Streams (`entry_audit` и `events:trades`) в PostgreSQL и NDJSON.gz для долгосрочного анализа, AB-тестирования и fail-safe хранения.

**Решение:**
- PostgreSQL archiver (consumer group pattern) для structured queries
- NDJSON.gz exporter для offline replay
- Idempotent processing (ON CONFLICT DO NOTHING)
- Crash recovery (XAUTOCLAIM pending messages)
- Configurable maxlen в producers

**Статус:** ✅ Production Ready

---

## Файлы созданы

### 1. SQL Migrations

#### `python-worker/migrations/026_entry_policy_audit.sql`
```sql
CREATE TABLE IF NOT EXISTS entry_policy_audit (
  stream_id        TEXT PRIMARY KEY,  -- Redis stream ID (idempotency)
  ts_ms            BIGINT NOT NULL,
  ts               TIMESTAMPTZ NOT NULL,
  
  -- Signal identifiers
  sid              TEXT,
  symbol           TEXT,
  tf               TEXT,
  strategy         TEXT,
  source           TEXT,
  
  -- Policy decision
  decision         TEXT NOT NULL,  -- ALLOW / SHADOW / DENY / UNKNOWN
  
  -- AB testing
  arm              TEXT,
  ab_group         TEXT,
  scenario         TEXT,  -- continuation / reversal
  regime           TEXT,  -- trend / range / thin
  
  -- Quality metrics
  of_confirm_score DOUBLE PRECISION,
  coh              DOUBLE PRECISION,
  leader_conf      DOUBLE PRECISION,
  
  -- Microstructure
  spread_z         DOUBLE PRECISION,
  pressure_sps     DOUBLE PRECISION,
  obi_age_ms       BIGINT,
  
  -- Full payload
  payload_json     JSONB NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes для typical queries
CREATE INDEX entry_policy_audit_ts_idx ON entry_policy_audit (ts DESC);
CREATE INDEX entry_policy_audit_symbol_ts_idx ON entry_policy_audit (symbol, ts DESC);
CREATE INDEX entry_policy_audit_decision_ts_idx ON entry_policy_audit (decision, ts DESC);
CREATE INDEX entry_policy_audit_arm_ts_idx ON entry_policy_audit (arm, ts DESC);
CREATE INDEX entry_policy_audit_payload_gin_idx ON entry_policy_audit USING gin (payload_json);
```

**Назначение:** Хранит все entry policy решения для AB-анализа и debugging policy gates.

#### `python-worker/migrations/027_position_events.sql`
```sql
CREATE TABLE IF NOT EXISTS position_events (
  stream_id    TEXT PRIMARY KEY,  -- Redis stream ID (idempotency)
  ts_ms        BIGINT NOT NULL,
  ts           TIMESTAMPTZ NOT NULL,
  
  -- Position identifiers (MT5 uses position_id)
  position_id  TEXT,
  sid          TEXT,
  symbol       TEXT,
  
  -- Event type
  event_type   TEXT NOT NULL,  -- TP_HIT, TRAILING_MOVE, POSITION_CLOSED, etc
  
  -- Metadata (close_reason, etc)
  meta_json    JSONB,
  
  -- Full payload
  payload_json JSONB NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes для timeline queries
CREATE INDEX position_events_position_ts_idx ON position_events (position_id, ts DESC) WHERE position_id IS NOT NULL;
CREATE INDEX position_events_type_ts_idx ON position_events (event_type, ts DESC);
CREATE INDEX position_events_symbol_ts_idx ON position_events (symbol, ts DESC) WHERE symbol IS NOT NULL;
CREATE INDEX position_events_payload_gin_idx ON position_events USING gin (payload_json);
CREATE INDEX position_events_meta_gin_idx ON position_events USING gin (meta_json) WHERE meta_json IS NOT NULL;
```

**Назначение:** Timeline всех событий по позиции для анализа trailing moves и winrate/ROC статистики.

---

### 2. Stream Archiver (PostgreSQL)

#### `python-worker/services/archivers/stream_archiver.py` (ОБНОВЛЕН)

**Основные изменения:**

1. **position_id вместо order_id** (MT5 convention):
```python
def event_row(self, stream_id: str, payload: Dict[str, Any]) -> Tuple[Any, ...]:
    position_id = payload.get("position_id") or payload.get("order_id")
    # ...
```

2. **meta parsing из JSON string**:
```python
def parse_meta_json(meta: Any) -> Optional[Dict[str, Any]]:
    if isinstance(meta, str):
        try:
            return json.loads(meta)  # Parse JSON string
        except Exception:
            return {"_raw": meta[:2000]}  # Fallback
    return meta if isinstance(meta, dict) else None
```

3. **event_type в корне payload**:
```python
event_type = str(payload.get("event_type") or "UNKNOWN")
```

4. **XAUTOCLAIM для crash recovery**:
```python
async def _claim_pending(self, stream, group, consumer, min_idle_ms, count):
    # Redis 6.2+ XAUTOCLAIM recovers pending from crashed consumers
    next_start, msgs, _ = await self.r.xautoclaim(
        name=stream, groupname=group, consumername=consumer,
        min_idle_time=min_idle_ms, start_id="0-0", count=count
    )
    return msgs
```

5. **Priority: pending → new messages**:
```python
async def consume_entry_audit(self) -> None:
    while True:
        # Priority 1: claim pending (recovery)
        pending = await self._claim_pending(...)
        if pending:
            msgs = pending
        else:
            # Priority 2: read new
            resp = await self._read_new(...)
            # ...
```

6. **Batch insert + XACK только после commit**:
```python
try:
    await loop.run_in_executor(None, self.pg.insert_entry_audit, rows)
    # ВАЖНО: ack только после успешного commit
    await self.r.xack(self.entry_stream, self.entry_cg, *ack_ids)
except Exception as e:
    # Не ack'аем - останется в pending для retry
    await self.dlq(...)
```

**ENV variables:**
```bash
# PostgreSQL DSN (precedence: TRADES_DB_DSN > DATABASE_URL > PG_DSN)
TRADES_DB_DSN=postgresql://trading:trading_password@postgres:5432/scanner_analytics

# Entry audit archiver
ENTRY_AUDIT_ARCHIVE_ENABLED=1
ENTRY_AUDIT_CG=entry_audit_archiver
ENTRY_AUDIT_CONSUMER=archiver_1
ENTRY_AUDIT_BATCH=500
ENTRY_AUDIT_BLOCK_MS=2000
ENTRY_AUDIT_MIN_IDLE_MS=60000  # XAUTOCLAIM pending recovery
ENTRY_AUDIT_DLQ_STREAM=stream:dlq:entry_audit

# Position events archiver
POSITION_EVENTS_ARCHIVE_ENABLED=1
POSITION_EVENTS_CG=position_events_archiver
POSITION_EVENTS_CONSUMER=archiver_1
POSITION_EVENTS_BATCH=500
POSITION_EVENTS_BLOCK_MS=2000
POSITION_EVENTS_MIN_IDLE_MS=60000
POSITION_EVENTS_DLQ_STREAM=stream:dlq:position_events
POSITION_EVENTS_TYPES=TP_HIT,TRAILING_MOVE,POSITION_CLOSED  # пустая строка = все
```

---

### 3. Stream Exporter (NDJSON.gz)

#### `python-worker/tools/stream_exporter.py` (СОЗДАН)

**Назначение:** Fail-safe disk archive для offline replay (независимо от PostgreSQL).

**Алгоритм:**
1. XRANGE читает chunk после last exported stream_id
2. Записывает в NDJSON.gz (newline-delimited JSON + gzip)
3. Обновляет last_id в Redis (checkpoint)
4. Ротация по дням (файлы по дате первого события)
5. Retention policy (удаляет файлы старше N дней)

**Формат файлов:**
```
/var/log/trade/exports/
  stream_trade_entry_audit/
    stream_trade_entry_audit_20260127.ndjson.gz
    stream_trade_entry_audit_20260128.ndjson.gz
  events_trades/
    events_trades_20260127.ndjson.gz
```

**ENV variables:**
```bash
STREAM_EXPORT_ENABLED=1
STREAM_EXPORT_DIR=/var/log/trade/exports
STREAM_EXPORT_KEEP_DAYS=90
STREAM_EXPORT_INTERVAL_SEC=300  # каждые 5 минут
```

---

### 4. Producer Updates

#### `python-worker/services/trade_events_logger.py` (ОБНОВЛЕН)

**До:**
```python
self.events_stream_maxlen = int(os.getenv("TRADE_EVENTS_STREAM_MAXLEN", "200000"))
```

**После:**
```python
# ВАЖНО: maxlen конфигурируется через ENV для координации с archiver
self.events_stream_maxlen = int(os.getenv("TRADE_EVENTS_MAXLEN", "200000"))
```

**Обоснование:** Стандартизация ENV variable names (`TRADE_EVENTS_MAXLEN` везде).

#### `python-worker/services/smt_entry_policy_service.py` (уже использовал configurable maxlen)

Уже использует:
```python
audit_stream_maxlen=int(os.getenv("TRADE_ENTRY_AUDIT_MAXLEN", "200000"))
out_stream_maxlen=int(os.getenv("TRADE_ENTRY_MAXLEN", "20000"))
```

---

### 5. Docker Compose Integration

#### `docker-compose-python-workers.yml` (ОБНОВЛЕН)

**Добавлены сервисы:**

##### `entry-audit-archiver`
```yaml
entry-audit-archiver:
  <<: *default-python-worker
  container_name: scanner-entry-audit-archiver
  command: ["sh", "-c", "sleep 30 && python -u services/archivers/stream_archiver.py"]
  environment:
    # PostgreSQL (precedence)
    - TRADES_DB_DSN=${TRADES_DB_DSN}
    - DATABASE_URL=${DATABASE_URL}
    - PG_DSN=${PG_DSN}
    
    # Stream names
    - TRADE_ENTRY_AUDIT_STREAM=${TRADE_ENTRY_AUDIT_STREAM:-stream:trade:entry_audit}
    - TRADE_EVENTS_STREAM=${TRADE_EVENTS_STREAM:-events:trades}
    
    # Archiver config (см. выше ENV variables)
    # ...
  depends_on:
    redis-worker-1: {condition: service_healthy}
    postgres: {condition: service_healthy}
  restart: unless-stopped
  deploy:
    resources:
      limits: {memory: 512M, cpus: '0.5'}
```

##### `stream-exporter`
```yaml
stream-exporter:
  <<: *default-python-worker
  container_name: scanner-stream-exporter
  command: ["sh", "-c", "sleep 10 && python -u tools/stream_exporter.py"]
  environment:
    - REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0}
    - TRADE_ENTRY_AUDIT_STREAM=${TRADE_ENTRY_AUDIT_STREAM:-stream:trade:entry_audit}
    - TRADE_EVENTS_STREAM=${TRADE_EVENTS_STREAM:-events:trades}
    - STREAM_EXPORT_ENABLED=${STREAM_EXPORT_ENABLED:-1}
    - STREAM_EXPORT_DIR=${STREAM_EXPORT_DIR:-/var/log/trade/exports}
    - STREAM_EXPORT_KEEP_DAYS=${STREAM_EXPORT_KEEP_DAYS:-90}
    - STREAM_EXPORT_INTERVAL_SEC=${STREAM_EXPORT_INTERVAL_SEC:-300}
  volumes:
    - ${STREAM_EXPORT_HOST_DIR:-./exports}:/var/log/trade/exports
  restart: unless-stopped
  deploy:
    resources:
      limits: {memory: 256M, cpus: '0.2'}
```

---

### 6. Configuration File

#### `stream-archiver.env.example` (СОЗДАН)

Полная конфигурация со всеми ENV variables, примерами запросов для observability и rollout strategy.

---

### 7. Tests

#### `python-worker/tests/test_events_parsing.py` (СОЗДАН)

**Unit tests:**
- ✅ `event_row` парсит `position_id` (не `order_id`)
- ✅ `meta_json` парсится из JSON string в dict
- ✅ `event_type` извлекается из корня payload
- ✅ timestamp coalescing (ts_ms > ts > timestamp_ms > stream_id)
- ✅ decision normalization (decision/result/policy_decision)
- ✅ arm normalization (arm/ab_arm)
- ✅ ab_group normalization (group/ab_group)

**Запуск:**
```bash
cd python-worker
pytest tests/test_events_parsing.py -v
```

#### `python-worker/tests/services/test_stream_archiver_integration.py` (СОЗДАН)

**Integration tests:**
- ✅ Полный цикл: Redis Stream → Archiver → PostgreSQL
- ✅ Idempotency (ON CONFLICT DO NOTHING)
- ✅ DLQ на parse errors
- ✅ XACK после успешного commit

**Запуск:**
```bash
TEST_INTEGRATION=1 pytest tests/services/test_stream_archiver_integration.py -v
```

---

### 8. Documentation

#### `STREAM_ARCHIVAL_IMPLEMENTATION.md` (СОЗДАН)

Production-ready документация:
- Архитектура решения
- Компоненты и их взаимодействие
- Rollout strategy (Phase 1-4)
- Rollback plan
- Observability (metrics, alerts, dashboards)
- Production checklist
- Примеры использования (SQL queries для AB-анализа, timeline, trailing stats)

---

## Ключевые архитектурные решения

### 1. Двухуровневая архитектура

**PostgreSQL Archiver (structured queries):**
- Consumer Group для exactly-once processing
- XACK после commit (не теряем данные при crash)
- XAUTOCLAIM для recovery pending messages

**NDJSON.gz Exporter (fail-safe):**
- XRANGE (не consumer group) - не влияет на archiver
- Checkpoint в Redis (last exported stream_id)
- Offline replay capability

**Обоснование:** Разделение concerns - structured queries vs offline replay. Fail-safe: если PostgreSQL недоступен, NDJSON.gz продолжает работать.

### 2. Idempotency

**PostgreSQL:**
```sql
INSERT INTO entry_policy_audit (...) VALUES (...)
ON CONFLICT (stream_id) DO NOTHING
```

**Обоснование:** Можно перезапускать archiver без дублей. Crash recovery через XAUTOCLAIM безопасен.

### 3. Deterministic Timestamp

**Приоритет:**
1. `payload.ts_ms` (если есть)
2. `payload.ts` (events:trades использует это)
3. `payload.timestamp_ms`
4. Fallback: `stream_id` timestamp (Redis-generated)

**Обоснование:** events:trades использует `ts` (epoch ms). Fallback на stream_id для robustness.

### 4. MT5 Convention: position_id

**Code:**
```python
position_id = payload.get("position_id") or payload.get("order_id")
```

**Обоснование:** MT5 использует position_id вместо order_id. Поддержка обоих для backward compatibility.

### 5. Meta Parsing

**Code:**
```python
def parse_meta_json(meta: Any) -> Optional[Dict[str, Any]]:
    if isinstance(meta, str):
        return json.loads(meta)  # JSON string → dict
    if isinstance(meta, dict):
        return meta
    return None
```

**Обоснование:** events:trades может писать meta как JSON string. PostgreSQL JSONB требует dict.

### 6. DLQ (Dead Letter Queue)

**Code:**
```python
except Exception as e:
    # Parse error → DLQ + ack (не блокируем consumer)
    await self.dlq(self.entry_dlq, self.entry_stream, mid, f"parse_error:{e}", {"fields": fields})
    await self.r.xack(self.entry_stream, self.entry_cg, mid)
```

**Обоснование:** Malformed messages не должны блокировать consumer. DLQ для observability + manual retry.

---

## Observability

### Metrics (рекомендуемые alerts)

1. **DLQ growth:**
   - `XLEN stream:dlq:entry_audit > 100`
   - `XLEN stream:dlq:position_events > 100`

2. **Consumer lag:**
   - `XINFO GROUPS` pending count > 10000

3. **PostgreSQL errors:**
   - Рост `pg_batch_error` в DLQ

4. **Export lag:**
   - `export:last_id:*` не обновляется > 10 минут

### Commands

**Check consumer groups:**
```bash
redis-cli XINFO GROUPS stream:trade:entry_audit
redis-cli XINFO GROUPS events:trades
```

**Check pending messages:**
```bash
redis-cli XPENDING stream:trade:entry_audit entry_audit_archiver
redis-cli XPENDING events:trades position_events_archiver
```

**Check DLQ:**
```bash
redis-cli XLEN stream:dlq:entry_audit
redis-cli XLEN stream:dlq:position_events
```

**PostgreSQL check:**
```sql
SELECT count(*) FROM entry_policy_audit;
SELECT count(*) FROM position_events;

-- Recent events
SELECT symbol, decision, ts FROM entry_policy_audit ORDER BY ts DESC LIMIT 10;
SELECT position_id, event_type, ts FROM position_events ORDER BY ts DESC LIMIT 10;
```

---

## Rollout Plan

### Phase 1: Enable Exporter (safest)
```bash
STREAM_EXPORT_ENABLED=1
ENTRY_AUDIT_ARCHIVE_ENABLED=0
POSITION_EVENTS_ARCHIVE_ENABLED=0
```

**Check:** NDJSON.gz files appear in exports/

### Phase 2: Apply Migrations
```bash
psql -f migrations/026_entry_policy_audit.sql
psql -f migrations/027_position_events.sql
```

**Check:** Tables exist with correct schema

### Phase 3: Enable Archiver
```bash
ENTRY_AUDIT_ARCHIVE_ENABLED=1
POSITION_EVENTS_ARCHIVE_ENABLED=1
```

**Check:** Consumer groups created, data flows to PostgreSQL

### Phase 4: Monitor
- Lag, pending, DLQ, row counts
- Alerts configured

---

## Rollback

**Disable archiver:**
```bash
ENTRY_AUDIT_ARCHIVE_ENABLED=0
POSITION_EVENTS_ARCHIVE_ENABLED=0
```

**Keep exporter running** (safe, no side effects).

**Pending recovery:** Restart archiver → XAUTOCLAIM processes pending messages.

---

## Production Checklist

- [x] SQL migrations created (026, 027)
- [x] stream_archiver.py с position_id + meta support
- [x] stream_exporter.py для NDJSON.gz
- [x] Configurable maxlen в producers
- [x] Docker Compose integration
- [x] Unit tests (test_events_parsing.py)
- [x] Integration tests (test_stream_archiver_integration.py)
- [x] ENV config (.env.example)
- [x] Documentation (IMPLEMENTATION.md)
- [x] Rollback plan
- [x] Observability guidelines
- [x] No linter errors

---

## Summary

**Что сделано:**
- ✅ Надежное архивирование Redis Streams в PostgreSQL и NDJSON.gz
- ✅ Idempotency (ON CONFLICT DO NOTHING)
- ✅ Crash recovery (XAUTOCLAIM pending messages)
- ✅ position_id + meta parsing (MT5 convention)
- ✅ Configurable maxlen
- ✅ Tests (unit + integration)
- ✅ Production-ready documentation

**Статус:** ✅ Ready for Production

**Next Steps:**
1. Phase 1 rollout (enable exporter)
2. Phase 2 (apply migrations)
3. Phase 3 (enable archiver)
4. Phase 4 (monitor metrics)

