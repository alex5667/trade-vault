# DecisionSnapshotWriter Runbook

Scope: `events:decision_snapshot` (Redis Stream) → `decision_snapshot` (Timescale)

## Fast triage
1) Is the container up?
- `docker ps | grep decision-snapshot-writer`
- `docker logs scanner-decision-snapshot-writer --tail=200`

2) Is /metrics alive?
- `curl -sS http://scanner-decision-snapshot-writer:9825/metrics | head`

3) Redis connectivity
- `redis-cli -h redis-worker-1 ping`

4) DB connectivity
- `pg_isready -h postgres -U trading -d scanner_analytics`

## Symptoms and actions

### A) `db_fail_total` grows
Meaning: DB upsert failing; good messages remain pending (at-least-once).
Actions:
- Validate DSN: `TRADES_DB_DSN` (host must be `postgres` in compose)
- Check DB locks/disk:
  - `SELECT now(), * FROM pg_stat_activity WHERE wait_event IS NOT NULL;`
  - `df -h` on DB node
- If table missing: apply migration SQL for `decision_snapshot`.

### B) `redis_lag_ms_p95` high
Meaning: decision snapshots are being processed late.
Common causes:
- DB slow or writer CPU throttled
- PEL growing (see pending_count)
Actions:
- Check pending:
  - `redis-cli XPENDING events:decision_snapshot decision_snapshot_writer`
  - `redis-cli XINFO GROUPS events:decision_snapshot`
- Increase temporarily:
  - `DECISION_SNAPSHOT_BATCH_SIZE`
  - `DECISION_SNAPSHOT_DB_UPSERT_CHUNK`

### C) `pending_count` high or growing
Meaning: messages are stuck pending (writer crashed mid-batch, DB failure, or consumer churn).
Actions:
- Confirm reclaim loop working (look for `pel reclaimed=` in logs)
- If reclaim failing: check `claim_fail_total` and Redis version (XAUTOCLAIM requires Redis 6.2+)
- Validate `DECISION_SNAPSHOT_PEL_MIN_IDLE_MS` (too small may churn; too big delays recovery)
- Manual inspection:
  - `redis-cli XPENDING events:decision_snapshot decision_snapshot_writer - + 10`

### D) DLQ spiking / by reason
Meaning: malformed payloads or contract mismatch.
Actions:
- Inspect DLQ:
  - `redis-cli XREVRANGE stream:decision_snapshot:dlq + - COUNT 20`
- Look at fields:
  - `reason` (`bad_payload_json`, `bad_payload_row`)
  - `payload` (truncated)
- Fix producer:
  - ensure `sid` present
  - ensure `decision_ts_ms` (or `ts_emit_ms`) present and epoch-ms
  - ensure bid/ask are sane (non-crossed)

### E) `claim_fail_total` growing
Meaning: XAUTOCLAIM or XCLAIM fallback is failing.
Actions:
- Check Redis version: XAUTOCLAIM requires Redis 6.2+
- Inspect logs for `xautoclaim not usable` or `pel reclaim fallback failed`
- If Redis < 6.2: disable or patch by setting `DECISION_SNAPSHOT_PEL_ENABLE=0` temporarily

## Safety notes
- Writer is **fail-open** relative to trading: stopping it does not block signal emit.
- Data correctness is protected by idempotent upsert: `UNIQUE(sid, ts_decision_ms)`.

## Useful queries
- Latest rows:
  - `SELECT ts_decision_ms, sid, symbol, decision_mid FROM decision_snapshot ORDER BY ts_decision_ms DESC LIMIT 20;`
- Volume:
  - `SELECT date_trunc('minute', to_timestamp(ts_decision_ms/1000.0)) AS m, count(*) FROM decision_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 60;`

## Useful PromQL (Grafana)
- Top DLQ reasons (1h):
  ```
  topk(5, sum by (reason) (increase(decision_snapshot_writer_dlq_by_reason_total[1h])))
  ```
- Pending growth (10m delta):
  ```
  delta(decision_snapshot_writer_pending_count[10m])
  ```
- Reclaim rate (5m):
  ```
  decision_snapshot_writer_reclaim_rate_5m
  ```
