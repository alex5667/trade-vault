# Auto-apply Tick Gate Runbook

## What it means
The auto-apply pipeline is blocked due to degraded tick quality (unknown side, bad timestamps, high ingest latency, etc.).

## Signals
- `auto_apply_tick_gate_blocked` is 1
- `auto_apply_tick_gate_block_meta_age_seconds` keeps increasing
- Upstream tick-quality alerts may also fire (skew, ts_source now, unknown-side EMA, ingest p99)

## Immediate actions
1. Check blocker status / logs:
   - `systemctl status auto-apply-tick-gate-blocker-v2` (or v1)
2. Inspect block meta:
   - Redis keys:
     - `{prefix}:tick_gate`
     - `{prefix}:tick_gate:meta`
     - `{prefix}:tick_gate:ts_ms`

## Root-cause drilldown
1. Run smoke tool (Step 13):
   - `python -m tools.smoke_tick_side_quality --hours 1 --limit 20000`
2. Check ts_source distribution:
   - rising `ts_source=wall` means payload timestamps are missing/invalid
3. Check ingest latency:
   - `python -m tools.bench_tick_ingest_latency_ab --interval 30`

## Unblock guidance
Do not force-unblock unless:
- tick quality recovered AND
- you understand the failure mode.

If you must unblock:
- remove key `{prefix}:tick_gate` and meta keys (manual override), then monitor for re-block.
