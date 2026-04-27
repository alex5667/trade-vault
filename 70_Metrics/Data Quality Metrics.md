---
type: metrics
name: Data Quality Metrics
scope: preprocessing
owners:
  - alex
tags:
  - metrics
  - dq
updated_at: 2026-04-18
---

# Data Quality Metrics

## Key metrics
- `ticks_dropped_total{reason="stale"}`
- `ticks_dropped_total{reason="future"}`
- `tick_dedup_drop_total`
- `unknown_side_total`
- `quarantine_events_total`
- `symbol_freeze_total`
- `freshness_ms`
- `ingest_ts_minus_event_ts_ms`
- `gap_detected_total`

## Required dashboards
- per-symbol freshness
- stale/future/dup rate by symbol
- age/skew distribution
- quarantine volume over time
- top bad symbols in current window

## Alerts
- freshness exceeds budget for major symbols
- stale/future rate breaches threshold
- freeze triggered repeatedly on same symbol
- unknown side spike threatens CVD quality

## Notes
- keep epoch ms everywhere
- measure both count and duration of freezes
- chart bad-time trigger streaks where available

## Links
- [[Time Model]]
- [[Data Quality Model]]
- [[RCA-2026-04-18-time-skew-freeze]]
