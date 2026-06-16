---
name: social-quality-gate
description: Build explicit quality gates, acceptance criteria, blockers, and release evidence for a social-ai project change (go/no-go).
---

When the user types `/social-quality-gate <change>` or asks how to raise implementation quality, orchestrate a formal quality-gate review.

## Mission
Convert `<change>` into a measurable go/no-go quality gate.

## Execution sequence
1. Act as **@social-lead** and restate the change and affected subsystems.
2. Load **social-project-core**.
3. Load specialist skills for affected areas: **social-contract-check** for boundaries, **social-observability-rollout** for prod readiness, **social-replay** for deterministic validation, **social-publish-policy** for publish-affecting changes, plus subsystem skills.
4. Act as **@quality-gatekeeper** and define: invariants · acceptance criteria · blockers vs follow-ups · required evidence.
5. Produce one merged output: Goal · Facts · Assumptions · Risks · Quality-gate checklist · Required tests · Required metrics/alerts · Release blockers · Rollout/rollback · Final ship / do-not-ship rule.

## Rules
- Every gate is measurable or directly testable. No vague wording.
- For LLM-touching changes: schema-valid > 99%, reason codes valid, no raw publish — these are gate items.
- Prefer repository-ready file/test/metric actions.

## Model lane
**claude-haiku-4-5** first; escalate if release governance itself must change.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
