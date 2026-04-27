---
name: trade-execution-risk
description: Use this skill for execution-risk analysis in the trade project: spread, slippage, fill assumptions, market impact, latency-to-fill sensitivity, execution cost metrics, and rollout gating based on execution quality.
---

# Trade Execution Risk

## Goal
Evaluate whether a strategy, detector, or rollout is viable after execution costs and fill quality are considered.

## Default lane
Use claude-opus-4-6 when the task changes entry policy, exit policy, fill assumptions, or rollout approval. Use claude-haiku-4-5 only for bounded metric collection or local config edits.

## Use this skill for
- slippage analysis
- spread cost estimation
- fill-probability validation
- execution-risk gating
- rollout approval for strategy changes sensitive to entry quality
- range/trend exit-policy review
- execution cost metrics and alert thresholds

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Metrics and thresholds
6. Pass/fail gates
7. Rollback triggers

## Scope rules
- Prefer canonical fills/trades/quotes sources over derived summaries.
- Always state time window, symbol scope, fee model, and slippage model.
- Separate market edge from execution edge.
- If the repository already contains execution-cost metrics, reuse them before inventing new ones.

## Escalate to claude-sonnet-4-6/opus-4-6 if
- the task changes TP/SL/trailing logic
- the task changes entry timing or fill assumptions
- execution cost may flip expectancy sign
- the task spans signal + execution + rollout at once

## Token discipline
- Read only touched strategy files, nearest metrics definitions, and current rollout policy first.
- Avoid repository-wide scans unless execution-risk ownership is unclear.
