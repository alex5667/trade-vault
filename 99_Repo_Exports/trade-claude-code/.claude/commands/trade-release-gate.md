---
name: trade-release-gate
description: Final production release gate for trade changes with explicit pass/fail criteria, stop conditions, and rollback readiness
---

When the user types `/trade-release-gate <change>` or asks whether a trade change is ready for merge, canary, or production, orchestrate a formal release-gate review.

## Mission
Decide whether `<change>` is ready for merge, canary, or production using explicit pass/fail evidence.

## Use this workflow when
- code or config is about to be merged
- canary or production rollout is planned
- a change needs a formal go / no-go decision

## Execution sequence
1. Act as **@trade-lead** and identify the target stage:
   - merge
   - canary
   - production
2. Load **trade-project-core**, **trade-quality-gates**, and **trade-observability-rollout**.
3. Pull in the minimum additional reviewers required by the change:
   - **@contract-governor** if contracts may have changed
   - **@latency-benchmarker** if hot paths may have changed
   - **@execution-risk-analyst** if entry quality, spread, slippage, or execution costs may change
   - **@resilience-drillmaster** if failure behavior or kill switches matter
   - **@timeseries-dba** / **@storage-retention-governor** if storage or retention changed
   - **@ml-replay-engineer** / **@backtest-validity-reviewer** if replay, ML, labels, or offline evaluation changed
4. Evaluate the release gate by evidence only:
   - implementation completeness
   - tests status
   - regression status
   - contract compatibility
   - latency budgets
   - observability readiness
   - rollback readiness
   - operational stop conditions
5. Produce one stage-specific verdict:
   - PASS
   - PASS WITH CONDITIONS
   - FAIL
6. If the verdict is not PASS, return the smallest set of actions needed to reach PASS.

## Required output
1. Goal
2. Target stage
3. Facts
4. Assumptions
5. Risks
6. Gate checklist by category
7. Verdict: PASS / PASS WITH CONDITIONS / FAIL
8. Blocking issues
9. Required file changes or missing artifacts
10. Metrics / alerts
11. Stop conditions
12. Rollout / rollback

## Gate categories
- Functional correctness
- Contract safety
- Latency / throughput
- Execution-risk safety
- Replay / regression validity
- Observability and alerting
- Resilience / kill switch / degraded mode
- Rollback readiness

## Release rules
- No generic “looks good”. Every gate must map to evidence.
- Do not recommend canary or prod without rollback steps and stop conditions.
- If contracts changed, backward compatibility must be stated explicitly.
- If latency changed, budgets and measurement method must be shown explicitly.

## Model lane
Default to a **premium reasoning model + Planning** mode for canary/prod decisions.
For merge-only bounded changes, **Gemini Flash + Fast** is acceptable if no premium trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
