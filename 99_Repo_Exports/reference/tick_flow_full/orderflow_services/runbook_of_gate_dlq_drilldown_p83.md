# Runbook P83: OF-Gate DLQ Drilldown + Optional DB Archive

## When to use
- Prometheus alert `OF_Gate_DLQ_NonZero` / dashboard shows DLQ length > 0.
- You need to quickly identify the dominant failure mode and decide: **fix+replay**, **fix+purge**, or **accept+ignore**.

## Streams
Default DLQ streams:
- `stream:dlq:of_gate_metrics`
- `stream:dlq:of_gate_quarantine`

DLQ message format (typical):
- `stream` (source stream)
- `stream_id` (source id)
- `err` (error string)
- `payload` (JSON string, often truncated)

## Step 0 — Confirm DLQ is non-empty
```bash
python -m orderflow_services.of_gate_dlq_drilldown_p83 stats
```

## Step 1 — Identify top failure modes
```bash
python -m orderflow_services.of_gate_dlq_drilldown_p83 top --limit 5000
```
Interpretation:
- `err_prefix` shows dominant error class (e.g., `parse_error`, `pg_batch_error`, `schema_version_not_accepted`).
- `dq_code/why` shows dominant contract/DQ failures (if embedded in payload).
- `reason_code` shows what business reason dominated (if present).

## Step 2 — Inspect samples
```bash
# sample specific dq_code
python -m orderflow_services.of_gate_dlq_drilldown_p83 sample --source stream:dlq:of_gate_metrics --dq-code ts_ms_bad_range --n 10

# sample specific err_prefix
python -m orderflow_services.of_gate_dlq_drilldown_p83 sample --source stream:dlq:of_gate_metrics --err-prefix parse_error --n 10
```

## Step 3 — Decide action
### A) Fix producer/consumer and **replay**
Default replay is dry-run; use `--commit` to actually push.

```bash
# dry-run (recommended first)
python -m orderflow_services.of_gate_dlq_drilldown_p83 replay \
  --source stream:dlq:of_gate_metrics --target metrics:of_gate --max 100

# commit (writes to target stream)
python -m orderflow_services.of_gate_dlq_drilldown_p83 replay \
  --source stream:dlq:of_gate_metrics --target metrics:of_gate --max 100 --commit
```
Notes:
- replay tries to write *flat fields* when payload looks like a metric row; otherwise writes a single `payload` field.
- replay adds `dlq_id/dlq_err` metadata unless `--no-meta`.

### B) **Purge** (dangerous)
Only when you are sure messages are irrecoverably bad or obsolete.

```bash
# delete exact ids
python -m orderflow_services.of_gate_dlq_drilldown_p83 purge \
  --source stream:dlq:of_gate_metrics --ids 1700000000000-0,1700000000001-0 --yes

# trim stream to maxlen (keeps newest)
python -m orderflow_services.of_gate_dlq_drilldown_p83 purge \
  --source stream:dlq:of_gate_metrics --maxlen 20000 --yes
```

## Step 4 — Optional: archive DLQ to DB (postmortems)
### Create table (Timescale optional)
Apply SQL migration:
- `services/archivers/sql/20260224_of_gate_dlq_events_p83.sql`

### Run archive job
```bash
export TRADES_DB_DSN='postgresql://...'
export REDIS_URL='redis://redis-worker-1:6379/0'

# one-shot forward scan (checkpoint stored in Redis)
python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --once

# daemon
python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --loop --interval-s 60

# backfill last N (does not update checkpoint)
python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --tail 200000 --no-checkpoint
```

### Queries
```sql
SELECT dq_code, count(*)
FROM of_gate_dlq_events
WHERE ts > now() - interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC;

SELECT reason_code, count(*)
FROM of_gate_dlq_events
WHERE ts > now() - interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC;
```

## Rollback
- Drilldown tool is additive: no runtime impact.
- DB archive job: stop the process / disable its timer (if you scheduled it).
