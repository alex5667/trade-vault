---
name: trade-parallel-investigation
description: Parallel multi-specialist investigation for ambiguous cross-service issues, regressions, or design questions in the trade system
---

When the user types `/trade-parallel-investigation <issue>` or asks to investigate an ambiguous cross-service problem, orchestrate a parallel investigation and merge the findings into one decision-ready report.

## Mission
Investigate `<issue>` across the relevant subsystems in parallel, minimize blind spots, and return one consolidated answer with explicit contradictions, confidence, and next actions.

## Use this workflow when
- the root cause is unclear
- more than one subsystem may be involved
- multiple specialists can investigate independently
- you need a fast map of risks before implementation or escalation

## Execution sequence
1. Act as **@trade-lead** and restate the issue, current impact, confidence level, and success criteria.
2. Load **trade-project-core**.
3. Select the minimum relevant specialist lanes from this list and investigate them in parallel:
   - **@go-ingest-engineer** + `trade-go-redis-ingest`
   - **@python-signal-engineer** + `trade-python-signal-engine`
   - **@platform-api-ui-engineer** + `trade-api-ui-contracts`
   - **@timeseries-dba** + `trade-timescale-postgres`
   - **@contract-governor** + `trade-contract-regression`
   - **@latency-benchmarker** + `trade-latency-benchmarking`
   - **@exchange-adapter-engineer** + `trade-exchange-adapter`
   - **@storage-retention-governor** + `trade-storage-retention`
   - **@backtest-validity-reviewer** + `trade-backtest-validity`
   - **@execution-risk-analyst** + `trade-execution-risk`
   - **@resilience-drillmaster** + `trade-resilience-failure-drills`
   - **@ml-replay-engineer** + `trade-ml-replay-gating`
   - **@sre-rollout** + `trade-observability-rollout`
4. Each selected lane must return only:
   - facts found
   - assumptions made
   - risks in its own domain
   - exact files / contracts / metrics that matter
   - confidence level: high / medium / low
5. Act as **@trade-lead** and merge the lane outputs into:
   - confirmed facts
   - open questions
   - conflicting interpretations
   - most probable causes or decisions
   - exact next step
6. If implementation is requested, convert the merged result into a file-by-file patch plan.

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Findings by lane
6. Conflicts / uncertainty
7. Recommended next action
8. File changes or investigation plan
9. Tests
10. Metrics / alerts
11. Rollout / rollback if production-facing

## Investigation rules
- Keep each lane scoped and avoid repository-wide scans unless a trigger fires.
- Do not let one lane silently overwrite another lane's conclusions.
- Explicitly mark unresolved contradictions.
- Prefer evidence over speculation.

## Model lane
- Start with **claude-haiku-4-5** if the issue is still bounded.
- Escalate to claude-opus-4-6 in **Planning** mode if:
  - more than 2 subsystems are implicated,
  - conclusions conflict,
  - the root cause remains ambiguous,
  - rollout or capital-risk decisions depend on the answer.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
