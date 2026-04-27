# P41 full closure: events:trades POSITION_CLOSED meta fields contract

Goal: ensure every POSITION_CLOSED event in Redis stream `events:trades` carries:
- `meta_enforce_cov_bucket` (string)
- `meta_enforce_applied` (bool)

This enables outcome stats and preflight hard-requirement: META_COV_PREFLIGHT_REQUIRE_TRADE_META=1.

## Required fields (top-level JSON fields recommended)
- event_type: "POSITION_CLOSED" (or type="POSITION_CLOSED")
- ts_ms: int (epoch milliseconds, UTC)
- symbol: string (e.g., "ETHUSDT")
- sid: string (signal id used for joins; consistent with trades:closed sid)
- r_mult: float (R-multiple), or r (alias); consumers accept either

### Meta enforce fields (P41)
- meta_enforce_cov_bucket: string
  - minimal: "control" | "enforce"
  - recommended: allow richer buckets: "enforce:A", "enforce:B", "control"
- meta_enforce_applied: bool
  - true if enforcement/config actually applied for this trade at entry time
  - false otherwise

## Backward/forward compatibility
To ease rollout, you may also include aliases:
- meta_cov_bucket (alias for meta_enforce_cov_bucket)
- meta_applied (alias for meta_enforce_applied)

Consumers (orderflow_services) tolerate:
- fields at top-level
- fields nested in `payload` JSON (stringified object) — preflight merges payload.

## Deterministic time
- Always epoch ms (int) in UTC.
- If exchange timestamps exist, store them separately (exch_ts_ms) and keep ts_ms as your event creation/ingest time.

## Data quality rules
- If meta fields are unknown at close time, emit:
  - meta_enforce_cov_bucket = "unknown"
  - meta_enforce_applied = false
  and increment a metric/counter; do NOT omit the fields once rollout is complete.

## Metrics to add in writer
- trade_events_closed_total
- trade_events_closed_missing_meta_total
- trade_events_closed_missing_meta_rate = missing/total (alert if > 1-5%)

## Rollout sequence
1) Dual-write: add meta_* fields + aliases; keep existing schema.
2) Observe missing_meta_rate until stable.
3) Enable `META_COV_PREFLIGHT_REQUIRE_TRADE_META=1` in meta_cov_ops_validate.
4) Optionally remove aliases later.
