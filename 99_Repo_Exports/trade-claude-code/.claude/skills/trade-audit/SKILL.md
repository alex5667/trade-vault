---
name: trade-audit
description: Audit the full trade pipeline for production readiness, contracts, latency, data quality, and risk
---

When the user types `/trade-audit <scope>` or asks for a comprehensive trade-system review, orchestrate the audit using `.claude/agents/agents.md` and the relevant skills in `.claude/skills/`.

## Mission
Produce one consolidated production-readiness review for `<scope>`.

## Execution sequence
1. Act as **@trade-lead** and restate the user's goal, scope, and success criteria.
2. Load **trade-project-core** for repository truth and response contract.
3. Load specialist skills based on `<scope>`:
   - always include **trade-observability-rollout**
   - include **trade-data-quality-time** if timestamps, sequencing, monotonicity, or malformed data may matter
   - include **trade-go-redis-ingest** for Go / exchange / Redis edge scope
   - include **trade-python-signal-engine** for detectors, gates, or Python workers
   - include **trade-api-ui-contracts** for NestJS / Next.js / DTO / WebSocket scope
   - include **trade-timescale-postgres** for schema / storage / metrics history scope
   - include **trade-ml-replay-gating** for ML, replay, or regression scope
4. Act as the relevant specialists and inspect the codebase or user-provided material.
5. Produce a single merged report with these sections:
   - Goal
   - Facts
   - Assumptions
   - Risks
   - Findings by subsystem
   - Required file changes
   - Tests
   - Metrics / alerts
   - Rollout / rollback
   - Prod checklist
6. Prioritize issues as:
   - P0: unsafe / data-corrupting / capital-risk / broken production path
   - P1: high operational risk / poor observability / fragile contracts
   - P2: correctness or maintainability debt
   - P3: optimization or polish
7. If the user asked for implementation, convert the audit directly into a file-by-file patch plan.

## Audit rules
- Do not stop at generic advice.
- Name exact files, contracts, ENV keys, migrations, metrics, and alerts when possible.
- Explicitly evaluate time-unit consistency and out-of-order handling.
- Explicitly evaluate backward compatibility for Redis and WebSocket payloads.
- Explicitly evaluate whether the proposed change is replayable and observable.\n

## Model lane
Default to a **premium reasoning model + Planning** mode for this workflow.

## Escalation guidance
- comprehensive production readiness audits are cross-cutting and usually merit premium reasoning

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
