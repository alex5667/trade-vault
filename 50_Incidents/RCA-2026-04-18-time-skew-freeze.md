---
type: incident
id: RCA-2026-04-18-time-skew-freeze
severity: SEV2
service: python-crypto-orderflow-service
status: template-filled
tags:
  - incident
  - rca
  - time
  - dq
updated_at: 2026-04-18
---

# RCA-2026-04-18-time-skew-freeze

## Summary
Ticks arrive too far in the past or future, breach time policy thresholds, and push one or more symbols into freeze / quarantine state. This protects downstream calculations but reduces signal throughput.

## Impact
- affected service: `python-crypto-orderflow-service`
- affected concepts:
  - stale tick drops
  - future tick drops
  - symbol freeze
  - quarantine stream growth
- user-visible effect:
  - symbol goes quiet
  - candidate generation falls sharply
  - freshness metrics deteriorate

## Facts
- Tick time policy should reject stale/future ticks
- freeze should only activate after configured bad-time streak
- quarantine should preserve evidence for replay and diagnosis

## Assumptions
- exchange timestamps drifted
- local host clock/NTP skewed
- ingestion reordering burst exceeded tolerance
- backfill inserted old events into hot path

## Detection
- `ticks_dropped_total{reason="stale|future"}`
- symbol-level freshness rising above SLO
- quarantine stream growth
- sudden drop in per-symbol publish rate

## Timeline
- identify exact symbol set
- compare exchange ts vs ingest ts vs wall clock
- verify NTP health on hosts
- inspect whether backfill/replay leaked into live stream

## Root cause
Temporal contract between ingestion and preprocessing was violated. The freeze mechanism worked as a safety brake, but throughput degraded until the timestamp issue was corrected.

## Contributing factors
- missing monotonicity dashboards
- too little separation between replay/backfill and hot streams
- no explicit alert on clock skew until downstream impact appears

## Corrective actions
1. Add host clock skew dashboard and alert
2. Tag replay/backfill payloads explicitly
3. Add per-symbol age/freshness metrics to dashboards
4. Log reason codes with thresholds used at decision time

## Prevention
- NTP health checks in node bootstrap
- deployment checklist for clock sync
- stronger replay/live separation

## Linked docs
- [[Time Model]]
- [[Data Quality Model]]
- [[Python Crypto Orderflow Service]]
- [[Data Quality Metrics]]
