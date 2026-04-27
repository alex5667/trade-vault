---
type: rollout
name: ML Shadow to Enforce
service: ml-confirm-gate
status: ready
risk_level: high
tags:
  - rollout
  - ml
  - shadow
  - enforce
updated_at: 2026-04-18
---

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
