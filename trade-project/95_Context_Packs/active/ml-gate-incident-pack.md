---
type: context_pack
tags: [context-pack, generated, llm]
topic: "ML gate incident review"
source_notes:
  - 50_Incidents/RCA-2026-04-18-ml-no-cfg.md
  - 60_Rollouts/ML Shadow to Enforce.md
  - 70_Metrics/ML Confirm Metrics.md
updated_at: auto
---

# Context Pack: ML gate incident review

## Task
Подготовить compact context pack для внешней модели по incident ml-no-cfg, rollout shadow->enforce и метрикам ML Confirm

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
- [[50_Incidents/RCA-2026-04-18-ml-no-cfg.md]]
- [[60_Rollouts/ML Shadow to Enforce.md]]
- [[70_Metrics/ML Confirm Metrics.md]]

## Key excerpts

### 50_Incidents/RCA-2026-04-18-ml-no-cfg.md
```text
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
```
### 60_Rollouts/ML Shadow to Enforce.md
```text
# ML Shadow to Enforce

## Goal
Promote ML confirm gate from observability-only mode to real blocking for selected buckets/symbols without losing decision quality or operational control.

## Scope
- service: `ml-confirm-gate`
- streams:
  - `signals:of:confirm`
  - `metrics:ml_confirm`
- rollout surface:
  - selected symbols / regimes / buckets first

## Preconditions
- shadow metrics available and trusted
- model/schema parity verified
- champion cfg exists and is persistent
- rollback to SHADOW tested
- missing/error/abstain metrics visible on dashboard

## Guardrails
- no sustained spike in missing/error rate
- p95 latency within target
- blocked/allowed ratio stable vs shadow expectation
- no unexplained drop in accepted trade quality

## Rollout steps
1. validate cfg freshness and model path
2. enable ENFORCE for tiny share or symbol subset
3. watch `allow/block/abstain/missing` by symbol and bucket
4. expand only if metrics remain healthy
5. record exact ts_ms and config diff for each stage

## Abort criteria
- missing/error ratio breaches threshold
- latency p95/p99 jumps materially
- accepted signal quality deteriorates
- reason codes or metrics become incomplete

## Rollback
- set mode back to `SHADOW`
- pin champion cfg
- document exact cause and affected symbols

## Post-rollout verification
- compare shadow expectation vs enforce reality
- confirm stream metrics, reason codes, and no silent gaps
- update incident/risk notes if new failure mode observed

## Links
- [[ML Confirm Metrics]]
- [[ML No CFG]]
- [[RCA-2026-04-18-ml-no-cfg]]
```
### 70_Metrics/ML Confirm Metrics.md
```text
# ML Confirm Metrics

## Key metrics
- `allow_total`
- `block_total`
- `abstain_total`
- `missing_total`
- `err_rate`
- `latency_ms`
- `latency_us`
- `p_edge`
- `p_margin`
- `conf`
- `status_count{status=*}`
- `share_used`
- `enforce_count`

## Required breakdowns
- by symbol
- by bucket / scenario
- by mode: OFF / SHADOW / ENFORCE
- by model version / challenger version

## Alerts
- missing/error ratio breaches threshold
- latency p95/p99 breaches budget
- mode/enforce/share configuration differs from rollout plan
- abstain ratio spikes unexpectedly
- model version disappears or changes without change record

## Operational notes
- never run ENFORCE without clear visibility into `missing`, `status`, and `latency`
- config freshness should be tracked separately if possible

## Links
- [[ML Confirm Gate]]
- [[ML Shadow to Enforce]]
- [[RCA-2026-04-18-ml-no-cfg]]
```

## Ask for external model
Use only this context pack. Preserve contracts and invariants. Return:
- goal
- facts
- assumptions
- risks
- plan
- tests
- metrics/alerts
- rollout/rollback
