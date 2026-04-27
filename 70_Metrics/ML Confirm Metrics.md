---
type: metrics
name: ML Confirm Metrics
scope: ml-confirm-gate
owners:
  - alex
tags:
  - metrics
  - ml
updated_at: 2026-04-18
---

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
