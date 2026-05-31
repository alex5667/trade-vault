# OFInputs DLQ DB rollups (P99) — Runbook

## Goal

Add a DB-backed DLQ/quarantine observability layer for OFInputs on top of P98 (`of_inputs_dlq_events`):

- deterministic **reason** extraction (`dq_code` else `err_prefix`)
- low-cardinality Prometheus gauges (via DB exporter)
- optional Grafana dashboard (Prometheus datasource)
- CLI drilldown for on-call

## Components

### 1) SQL views (rollups)

- `services/archivers/sql/20260225_of_inputs_dlq_events_rollups_p99.sql`
  - `v_of_inputs_dlq_events_parsed`
  - `v_of_inputs_dlq_events_1h`
  - `v_of_inputs_dlq_events_reason_24h`

Apply (once):

```sql
\i services/archivers/sql/20260225_of_inputs_dlq_events_rollups_p99.sql
```

### 2) Prometheus exporter

Module: `orderflow_services/of_inputs_dlq_db_exporter_p99.py`

Run:

```bash
export TRADES_DB_DSN='postgresql://...'
export OF_INPUTS_DLQ_DB_EXPORTER_LOOKBACK_H=1   # recommended for alerting
python -m orderflow_services.of_inputs_dlq_db_exporter_p99
```

Metrics:

- `of_inputs_dlq_db_events_lookback_total{kind,reason}`
- `of_inputs_dlq_db_last_event_ts_ms{kind}`
- `of_inputs_dlq_db_last_event_age_sec{kind}`

Cardinality control:

- `OF_INPUTS_DLQ_DB_REASON_ALLOWLIST=missing_lob_fields,book_state_degraded,bad_ts_ms,ValueError,KeyError,...`
- all other reasons aggregate into `reason="other"`

### 3) Drilldown CLI

Module: `orderflow_services/of_inputs_dlq_db_drilldown_p99.py`

Examples:

```bash
TRADES_DB_DSN=... python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --lookback-h 24 --top 15
TRADES_DB_DSN=... python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --kind dlq --reason missing_lob_fields --sample 10
```

Notify (best-effort):

```bash
REDIS_URL=redis://redis-worker-1:6379/0 \
TELEGRAM_NOTIFY_STREAM=notify:telegram:crit \
python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --notify
```

### 4) Grafana dashboard

- `orderflow_services/grafana/of_inputs_dlq_db_p99.json`

Uses Prometheus metrics produced by the exporter.

### 5) Alerts

- `orderflow_services/prometheus_alerts_of_inputs_dlq_db_p99.yml`

**Important:** set `OF_INPUTS_DLQ_DB_EXPORTER_LOOKBACK_H=1` (or similar) for meaningful thresholds.

## Triage playbook

1) Check backlog streams (P96/P97) + DB gauges:
   - DLQ: `stream:dlq:of_inputs`
   - Quarantine: `quarantine:signals:of:inputs`

2) Find top reasons:

```bash
python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --kind dlq --lookback-h 1 --top 10
```

3) For a top reason, view samples:

```bash
python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --kind dlq --reason missing_lob_fields --sample 5
```

4) Decide action:

- `missing_lob_fields` / `book_state_degraded` → check LOB ingestion quality + deterministic V3→V2 downgrade path.
- `bad_ts_ms` / `bad_time` → check upstream time sanitization + bad-time quarantine counters.
- `ValueError` / `KeyError` / `TypeError` → inspect recent deploy changes affecting payload encoding.

## Rollback

- Stop exporter.
- Views can remain (read-only) or can be dropped safely.
