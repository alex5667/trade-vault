---
description: Review or redesign execution policy, slippage assumptions, spread budgets, and rollout gates for entry-sensitive strategy changes
---

When the user types `/trade-execution-policy-review <change>` or asks to review trailing stop, TP logic, fill assumptions, spread/slippage budgets, or execution-risk-sensitive rollout, orchestrate a structured execution-risk review.

## Mission
Turn `<change>` into a production-safe execution-policy decision with explicit pass/fail criteria.

## Execution sequence
1. Act as **@trade-lead** and restate the change, affected symbols/regimes, and success criteria.
2. Load **trade-project-core**.
3. Always load **trade-execution-risk**, **trade-observability-rollout**, and **trade-quality-gates**.
4. Load **trade-python-signal-engine** if detector/gate/strategy logic is affected.
5. Load **trade-ml-replay-gating** if replay, outcomes, or model confirmation is affected.
6. Load **trade-timescale-postgres** if history, outcome storage, or labeling queries must change.
7. Act as **@microstructure-analyst** and define:
   - expected market edge
   - regime sensitivity
   - failure modes
   - false positive / false negative trade-offs
8. Act as **@execution-risk-analyst** and define:
   - spread/slippage/fill assumptions
   - execution cost metrics
   - where expectancy can break
   - rollout stop conditions tied to execution quality
9. Act as **@quality-gatekeeper** and convert the result into explicit pass/fail release gates.
10. Act as **@sre-rollout** and define rollout / rollback steps.
11. Return one merged answer with:
   - Goal
   - Facts / Assumptions / Risks
   - Execution-policy decision
   - File-by-file implementation plan
   - Tests
   - Metrics / alerts
   - Rollout / rollback
   - Prod checklist

## Model lane
Default to a premium reasoning model in Planning mode.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
