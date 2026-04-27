# P55 Close Backfill / Replay Runbook

## Purpose
Replays POSITION_CLOSED events from events:trades into the enrichment pipeline.
It is used to restore missing trades:closed entries (and therefore KPI coverage) for time periods where
joiner/drainer was not running or decision records arrived later than closes.

## How it works
- Scan events:trades over a time window (default 48h) or from an explicit since-id.
- For each POSITION_CLOSED:
  - if join:closed:{sid} exists -> skip
  - else if decision:{sid} exists and DIRECT_JOIN_ON_BACKFILL=1 -> write directly to trades:closed
  - else -> push to trades:close_wait and let P54 drainer complete later

## Commands
### Backfill last 72 hours
python -m tools.close_backfill_replay_v1 --hours 72 --count 200000

### Backfill from a stream id (inclusive)
python -m tools.close_backfill_replay_v1 --since-id 1739990000000-0 --count 50000

### Exporter
python -m tools.close_backfill_replay_exporter_v1

## ENV
- REDIS_URL
- TRADE_EVENTS_STREAM=events:trades
- TRADES_CLOSED_STREAM=trades:closed
- CLOSE_WAIT_STREAM=trades:close_wait
- DECISION_KEY_PREFIX=decision:
- DEDUP_KEY_PREFIX=join:closed:
- DIRECT_JOIN_ON_BACKFILL=1
- BACKFILL_SEEN_PREFIX=backfill:seen:
- BACKFILL_SEEN_TTL_SEC=1209600

## Metrics
Stored in Redis hash: metrics:close_backfill_replay and exported (optional).
- close_backfill_processed_total
- close_backfill_direct_joined_total
- close_backfill_pushed_to_close_wait_total
- close_backfill_bad_payload_total
- close_backfill_no_sid_total
- close_backfill_already_joined_total

## Safety / Rollback
Safe to stop anytime. Dedup keys prevent duplicates.
Rollback = stop the process; no impact on trade execution.
