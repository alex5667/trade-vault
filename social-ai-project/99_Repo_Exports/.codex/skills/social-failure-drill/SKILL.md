---
name: social-failure-drill
description: Design failure-injection / resilience drills for the social-ai project: platform API outage, quota exhaustion, Redis stream backlog/PEL, LLM timeout/invalid-JSON storm, publish adapter 5xx, poison rows, governor flap. Validates degraded modes and rollback.
---

When the user types `/social-failure-drill <subsystem>` or asks to test resilience / degraded modes, design a failure drill.

## Execution sequence
1. Act as **@social-lead**; identify the subsystem and its failure modes.
2. Load **social-project-core**, **social-observability-rollout**, plus subsystem skills.
3. Act as **@sre-rollout** and define for each fault: trigger, expected detection (metric/alert), expected degraded behavior, expected recovery, and pass/fail criteria.
4. Return: Drill scenarios · Injection method · Expected vs unsafe behavior · Observability to confirm · Remediation gaps · Hardening actions.

## Fault catalog
- Platform API outage / 429 quota exhaustion → backoff, draft-only fallback
- Redis stream backlog / PEL growth / consumer death → claim/reclaim, DLQ
- LLM timeout / invalid-JSON storm → retry→quarantine, no raw publish
- Publish adapter 5xx / partial upload → idempotent retry, no duplicate post
- Poison row in stream → isolate, continue, alert
- Governor flap / stale config → TTL fail-safe reverts to safe stage

## Rules
- Every drill has measurable pass/fail.
- Confirm no silent failure: bad path = detect→sanitize→quarantine→metrics.
- Confirm safe fallback = degrade to draft-only / pause autopublish.

## Model lane
**claude-sonnet/opus + Planning**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
