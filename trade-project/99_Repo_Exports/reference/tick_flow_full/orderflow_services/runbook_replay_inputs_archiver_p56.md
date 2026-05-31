# P56 Replay Inputs Archiver Runbook

## Purpose
Archive `ml_replay_inputs_v1` to durable NDJSON files so ML training and KPI audits are not bounded by Redis retention.

## Process
- `python -m ml_analysis.tools.replay_inputs_archiver`

## Exporter (Prometheus)
- `python -m tools.replay_inputs_archiver_exporter_v1`
- Port: `REPLAY_ARCHIVER_EXPORTER_PORT` (default 9139)

## Key Streams / Keys
- Input stream: `ml_replay_inputs_v1`
- Consumer group: `ml_replay_archiver_v1`
- Archive dir: `ARCHIVE_DIR` (default `./archives/ml_replay_inputs_v1`)
- Seen dedup keys: `archiver:seen:<stream_id>` (TTL)
- Metrics hash: `metrics:replay_inputs_archiver`

## Common issues
### 1) Stale / stopped
Check:
- `HGET metrics:replay_inputs_archiver last_run_ts_ms`
Fix:
- restart the process, verify REDIS_URL, ensure permissions on ARCHIVE_DIR

### 2) Bad payload / no sid
Means producers wrote malformed payload into ml_replay_inputs_v1.
Inspect last stream id:
- `HGET metrics:replay_inputs_archiver last_stream_id`
- `XRANGE ml_replay_inputs_v1 <id> <id>`

### 3) Disk space / permissions
- ensure `ARCHIVE_DIR` exists and writable
- monitor disk usage; compress via `ARCHIVE_GZIP=1`

## Rollback
Stop the archiver. It does not affect trading; it only affects offline analysis and training data durability.
