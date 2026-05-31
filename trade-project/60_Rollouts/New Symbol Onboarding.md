---
type: rollout
name: New Symbol Onboarding
service: pipeline
status: ready
risk_level: medium
tags:
  - rollout
  - symbols
  - ingestion
updated_at: 2026-04-18
---

# New Symbol Onboarding

## Goal
Add a new symbol to the trade pipeline safely, with correct liquidity expectations, data quality controls, and rollout blast-radius limits.

## Scope
- ingestion subscriptions
- preprocessing calibration
- detector thresholds
- gates / execution eligibility
- dashboards and alerts

## Preconditions
- symbol has sufficient liquidity and exchange support
- tick/book stream names defined
- execution venue mapping validated
- symbol-specific config exists where required
- dashboards include freshness / latency / DQ views

## Rollout steps
1. enable ingestion only and observe data quality
2. validate time monotonicity, gaps, duplicates, and unknown side rate
3. enable detector + diagnostics only
4. enable shadow publish path
5. enable tradeable path with low blast radius if metrics are healthy

## Abort criteria
- persistent stale/future tick issues
- book health unstable
- execution venue symbol mismatch
- spread/slippage too wide for viable edge

## Rollback
- remove symbol from live subscriptions
- stop tradeable publish
- preserve diagnostics for replay

## Post-rollout verification
- symbol appears on ingestion dashboards
- candidate/confirm volumes are plausible
- no abnormal lag, skew, or reconnect storm

## Links
- [[Go Worker Ingestion]]
- [[Python Crypto Orderflow Service]]
- [[Execution Metrics]]
- [[WS Reconnect Storm]]
