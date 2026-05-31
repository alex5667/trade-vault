---
name: trade-sequential-review
description: Sequential specialist review workflow for bounded but high-safety trade changes that require ordered validation
---

When the user types `/trade-sequential-review <change>` or asks for a safe ordered review of a non-trivial change, orchestrate a sequential multi-role review where each specialist builds on the previous result.

## Mission
Turn `<change>` into a reviewed, implementation-ready, production-safe plan with ordered validation and minimal rework.

## Use this workflow when
- the change is non-trivial but not fully ambiguous
- each review step depends on the previous one
- you want one clean chain: design -> contracts -> performance -> quality -> rollout

## Execution sequence
1. Act as **@trade-lead** and restate the change, scope, assumptions, and success criteria.
2. Load **trade-project-core**.
3. Route domain review in order, selecting only the needed steps:
   - design / domain logic:
     - **@microstructure-analyst**
     - **@python-signal-engineer** + `trade-python-signal-engine`
     - **@exchange-adapter-engineer** + `trade-exchange-adapter`
     - **@backtest-validity-reviewer** + `trade-backtest-validity`
   - transport / contract layer:
     - **@contract-governor** + `trade-contract-regression`
     - **@platform-api-ui-engineer** + `trade-api-ui-contracts`
   - storage / lifecycle layer:
     - **@timeseries-dba** + `trade-timescale-postgres`
     - **@storage-retention-governor** + `trade-storage-retention`
   - performance / risk layer:
     - **@latency-benchmarker** + `trade-latency-benchmarking`
     - **@execution-risk-analyst** + `trade-execution-risk`
   - release safety layer:
     - **@quality-gatekeeper** + `trade-quality-gates`
     - **@resilience-drillmaster** + `trade-resilience-failure-drills`
     - **@sre-rollout** + `trade-observability-rollout`
4. Each step must explicitly state whether it is:
   - approved
   - approved with conditions
   - blocked
5. If a step blocks the change, downstream steps must treat that block as a constraint rather than ignoring it.
6. Act as **@trade-lead** and return one merged verdict.

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Sequential review log
6. Blocking issues
7. Final verdict
8. Exact file changes
9. Tests
10. Metrics / alerts
11. Rollout / rollback
12. Prod checklist

## Review rules
- Do not parallelize this workflow conceptually; later steps must consume earlier conclusions.
- Prefer additive and backward-compatible changes unless the user explicitly allows breaking changes.
- If blocked, return the smallest viable path to unblock.

## Model lane
- Default to **claude-haiku-4-5** for bounded sequential review.
- Escalate to claude-opus-4-6 in **Planning** mode if:
  - architecture redesign is required,
  - the review crosses more than 2 subsystems,
  - the change is non-backward-compatible,
  - strategy / ML / retention policy must be redesigned.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
