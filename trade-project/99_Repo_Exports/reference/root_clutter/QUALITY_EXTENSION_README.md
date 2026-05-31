# Trade quality extension

This extension adds a dedicated quality layer on top of the existing trade workspace pack.

## What was added

### Agents
- `@quality-gatekeeper` — definition of done, blockers, release gates
- `@contract-governor` — producer/consumer compatibility and schema safety
- `@latency-benchmarker` — budgets, benchmarks, re-measure discipline
- `@resilience-drillmaster` — degraded modes, drills, kill switches, rollback

### Skills
- `trade-quality-gates`
- `trade-contract-regression`
- `trade-latency-benchmarking`
- `trade-resilience-failure-drills`

### Workflows
- `/trade-quality-gate`
- `/trade-contract-check`
- `/trade-latency-audit`
- `/trade-failure-drill`
- `/trade-regression-pack`

## Recommended usage

Before merge:
1. `/trade-quality-gate <change>`
2. `/trade-contract-check <scope>` if boundaries changed
3. `/trade-latency-audit <scope>` if hot path changed

Before canary:
4. `/trade-failure-drill <scenario>`
5. `/trade-regression-pack <change>`

## Why this improves quality
- makes pass/fail gates explicit
- catches contract drift before rollout
- forces baseline -> change -> re-measure for performance claims
- validates degraded modes and kill switches before incidents
