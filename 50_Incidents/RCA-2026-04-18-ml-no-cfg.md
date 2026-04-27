---
type: incident
id: RCA-2026-04-18-ml-no-cfg
severity: SEV2
service: ml-confirm-gate
status: template-filled
tags:
  - incident
  - rca
  - ml
  - config
updated_at: 2026-04-18
---

# RCA-2026-04-18-ml-no-cfg

## Summary
ML confirm gate cannot load active champion/challenger configuration and starts returning missing/error status. Depending on fail policy, this degrades to fail-open shadowed decisions or fail-closed blocking.

## Impact
- affected service: `ml-confirm-gate`
- likely affected streams:
  - `signals:of:confirm`
  - `metrics:ml_confirm`
- blast radius:
  - all symbols served by current cfg bucket
- user-visible effect:
  - degraded enforcement quality
  - unexpected increase in missing / abstain / fail-open statuses

## Facts
- Config keys are expected under champion/challenger Redis config paths
- Missing config should still emit metrics and status instead of failing silently
- Shadow/Enforce rollout safety depends on accurate metrics around missing config

## Assumptions
- Redis persistence was incomplete or wrong DB/URL was used
- promotion worker failed before writing champion metadata
- model artifact path and schema version drifted

## Detection
- spikes in `missing_cfg_total`
- `status_count{status="MISSING_FAILOPEN"}` or equivalent
- `err_rate` in ML metrics stream trends toward 1.0
- gap between rollout intention and actual enforcement state

## Timeline
- `t0`: alert fires on missing cfg / error rate
- `t0 + 5m`: verify Redis keys and selected DB
- `t0 + 10m`: verify model path, schema version, and promotion worker logs
- `t0 + 20m`: restore champion cfg or force safe mode
- `t0 + 30m`: verify metrics recovery and decision quality

## Root cause
Primary failure is control-plane config unavailability for ML gating. The serving path depends on runtime config consistency; when that contract breaks, decisions degrade.

## Contributing factors
- weak backup / persistence discipline for cfg keys
- missing parity check between model artifact and feature schema
- insufficient alerting on config freshness / existence
- rollout state not pinned strongly enough

## What went well
- service can fail-open rather than crashing
- metrics stream can surface missing status for diagnosis
- rollback to SHADOW/OFF is operationally clear

## What went poorly
- missing cfg can persist longer than acceptable before human response
- if metrics are incomplete, the blast radius becomes harder to quantify
- schema mismatch and cfg absence can look similar unless reason codes are explicit

## Corrective actions
1. Persist champion config to durable storage on every promotion
2. Add startup and periodic cfg existence checks
3. Add explicit model/schema parity check before enforce
4. Alert on cfg freshness age and missing ratio
5. Document rollback to SHADOW with exact commands

## Prevention
- keep backup copy of champion/challenger cfg
- require approval gate before ENFORCE if cfg freshness is stale
- add nightly audit for model path, schema version, and Redis keys

## Linked docs
- [[ML No CFG]]
- [[ML Confirm Gate]]
- [[ML Confirm Metrics]]
- [[ML Shadow to Enforce]]
