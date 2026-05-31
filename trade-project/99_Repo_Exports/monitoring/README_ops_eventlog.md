# Ops Event Log (Redis Stream)

## Purpose
Track manual operational actions (freeze set/clear) with an audit trail.

Stream:
- `ops:eventlog` (override `OPS_EVENT_STREAM`)

## Tail
```bash
pip install -r scripts/requirements_ops.txt
REDIS_URL="redis://redis-worker-1:6379/0" ./scripts/ops_eventlog_tail.sh
```

## Examples
- `promote_freeze_set`
- `promote_freeze_clear`
