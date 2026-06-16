---
name: social-sequential-review
description: Run a staged sequential review of a social-ai change — correctness → contracts → data-quality/time → observability → publish-safety/policy → rollout — each stage gating the next.
---

When the user types `/social-sequential-review <change>` or asks for a thorough staged review, run a gated sequential review.

## Stages (each must pass before the next)
1. **Correctness** — logic, edge cases, tests. (@python-worker-engineer / @go-collector-engineer / @control-plane-engineer / @llm-content-engineer per area)
2. **Contracts** — backward compatibility (load **social-contract-check**; @contract-governor).
3. **Data quality & time** — load **social-data-quality-time**.
4. **Observability** — metrics/logs/alerts (load **social-observability-rollout**).
5. **Publish safety & policy** — load **social-publish-policy** for publish-affecting changes.
6. **Rollout/rollback** — load **social-rollout**.

## Execution
1. Act as **@social-lead**; load **social-project-core**; restate change + blast radius.
2. Walk stages in order; stop and report if a stage fails with a hard blocker.
3. Return per-stage findings + final verdict (ship/hold/no-ship) + ordered fix list.

## Rules
- A failed earlier stage blocks later stages.
- Findings are concrete (file/line/test/metric).

## Model lane
**claude-sonnet/opus**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
