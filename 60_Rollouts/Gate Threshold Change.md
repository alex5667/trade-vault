---
type: rollout
name: Gate Threshold Change
service: pre-publish-gates
status: ready
risk_level: medium
tags:
  - rollout
  - gates
  - thresholds
updated_at: 2026-04-18
---

# Gate Threshold Change

## Goal
Change one or more gate thresholds without silently altering effective trade frequency or risk profile.

## Scope
- DQ thresholds
- regime/session rules
- drift thresholds
- edge cost constraints
- spread/slippage limits

## Preconditions
- current baseline metrics captured
- reason-code distribution known
- exact config diff reviewed
- rollback values documented

## Rollout steps
1. record pre-change reason distribution
2. apply change to shadow/diagnostic path if possible
3. compare veto/allow deltas by symbol and regime
4. promote to active path only if result matches intent
5. document new threshold and motivation

## Abort criteria
- large unexplained change in veto distribution
- signal count collapses or spikes unexpectedly
- risk costs rise without compensating edge improvement

## Rollback
- restore previous threshold values
- verify reason-code distribution returns toward baseline

## Post-rollout verification
- check veto totals by reason
- verify execution and post-trade outcomes remain within guardrails

## Links
- [[Pre-Publish Gates]]
- [[OF Gate Metrics]]
- [[Service SLOs]]
