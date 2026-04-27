# P84 Runbook: OF-Gate DLQ fixed-then-replay

## Why
DLQ streams may accumulate due to transient failures or mixed schema/producers.
This tool helps you:
- quantify fixable share,
- see top causes and stable hint codes,
- safely replay *fixable* rows back into their original streams.

## Preconditions
- `REDIS_URL` reachable from the host/container.
- Canonical contract validator is present (`services.orderflow.of_gate_metrics_contract`).

## Commands

### 1) Triage (no writes)
```bash
REDIS_URL=redis://redis-worker-1:6379/0 \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 triage --limit 5000
```

To notify via Redis notify-stream:
```bash
REDIS_URL=... NOTIFY_TELEGRAM_STREAM=notify:telegram \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 triage --limit 5000 --notify
```

### 2) Replay (DRY RUN)
```bash
REDIS_URL=... \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 replay \
  --source stream:dlq:of_gate_metrics --max 200
```

### 3) Replay (COMMIT)
```bash
REDIS_URL=... \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 replay \
  --source stream:dlq:of_gate_metrics --max 200 --commit
```

### 4) Replay + delete from DLQ (danger)
Deletes the DLQ entry **after** successful replay write.
```bash
REDIS_URL=... \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 replay \
  --source stream:dlq:of_gate_metrics --max 200 --commit --delete-after-replay
```

### 5) Automation-style (triage + safe replay across streams)
Recommended defaults (restrict to *fixable classes*: `ts_ms`, `schema_version`, `missing_legs`).
```bash
REDIS_URL=... \
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 auto \
  --commit --delete-after-replay --notify --require-fix \
  --allow-fixes add_schema_name,add_schema_version,coerce_schema_version_int,normalize_ts_ms,ts_from_stream_id,default_missing_legs_empty,coerce_missing_legs_to_json,stringify_missing_legs
```

## Fix policy (conservative)
The tool only applies *additive / deterministic* fixes:
- add schema markers (schema_name/schema_version)
- coerce schema_version to int
- normalize ts_ms (sec/us/ns → ms), fallback to stream_id ms-part
- missing_legs: ensure present and valid JSON (defaults to "[]" only when missing)
- low-card normalization via `enrich_schema_fields()`

For `parse_error:*` DLQ entries, tool tries to recover original payload from stored `{"fields": ...}`.

## Safety notes
- Replay writes include markers: `replay=1`, `replay_src_dlq_id`, `replay_fix_tags`.
- For automation, use `--require-fix` to avoid replaying entries that need no fixes.
- Use `--allow-fixes` to prevent accidental broad sanitization.

## Rollback
Tool is additive.
If replay caused issues:
- stop running the tool or disable timers,
- consumers can ignore rows with `replay=1`,
- optionally purge replays by filtering on `replay_src_dlq_id` markers.
