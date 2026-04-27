# Runbook: P2 Confidence Scores Archiver (Redis Streams -> Timescale)

## Goal
Archive high-frequency confidence scoring events from Redis stream `signals:confidence:scores` into TimescaleDB, while isolating ingestion from critical trade archives (noisy-neighbor avoidance).

## Components
- **Producer**: `SignalPipeline` (publishes score events to `signals:confidence:scores`)
- **Consumer**: `services/archivers/stream_archiver.py` (Consumer Group + batch inserts)
- **DB table**: `signal_confidence_scores` (hypertable on `ts`)

## Data contract (scores stream)
Each message in `signals:confidence:scores` is a JSON payload (stored under Redis field `payload`) with:
- `schema_version: int`
- `producer: str`
- `sid: str`
- `symbol: str`
- `ts_event_ms: int` (epoch ms)
- `confidence_raw: float`
- `confidence_final: float|null`
- `evidence_map: dict[str, float]` (numbers only)
- optional `context_json: dict` (kept small)

## Setup

### 1) Create table
Run SQL migration:
- `services/archivers/sql/20260220_signal_confidence_scores.sql`

Alternatively, enable auto-migration in the archiver (recommended only in controlled envs):
- `CONF_SCORES_AUTO_MIGRATE=1`

### 2) Enable producer (shadow)
Enable publishing to the scores stream:
- `CONF_SCORES_PUBLISH_ENABLED=1`

Optional:
- `CONF_SCORES_INCLUDE_CONTEXT=0|1` (default 0)
- `CONF_SCORES_STREAM_MAXLEN=...` (default 200000)

### 3) Run archiver as a separate container (recommended)
Use the same image as the python-worker, but isolate via ENV:

- Trades archiver:
  - `ENTRY_AUDIT_ARCHIVE_ENABLED=1`
  - `POSITION_EVENTS_ARCHIVE_ENABLED=1`
  - `CONFIDENCE_SCORES_ARCHIVE_ENABLED=0`

- Signals archiver:
  - `ENTRY_AUDIT_ARCHIVE_ENABLED=0`
  - `POSITION_EVENTS_ARCHIVE_ENABLED=0`
  - `CONFIDENCE_SCORES_ARCHIVE_ENABLED=1`
  - set larger batch: `CONF_SCORES_BATCH=5000` (or 10000)

Optionally use a dedicated DSN for the signals archiver:
- `ARCHIVER_PG_DSN=...`

## Monitoring
Recommended metrics/alerts:
- Redis group lag for scores stream (Consumer Group pending + stream length)
- PostgreSQL insert latency / errors

Operational signals:
- If DB becomes saturated: stop/scale down the signals archiver container.
- Messages will accumulate in Redis; you can later replay by restarting the archiver.

## Troubleshooting
- **No rows in DB**: verify `CONF_SCORES_PUBLISH_ENABLED=1` on producer and `CONFIDENCE_SCORES_ARCHIVE_ENABLED=1` on archiver.
- **DLQ grows**: check `stream:dlq:confidence_scores` for parse/schema errors.
- **Backlog**: increase `CONF_SCORES_BATCH` and/or run multiple consumers in the same Consumer Group.
