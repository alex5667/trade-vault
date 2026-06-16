---
name: social-release-gate
description: Final release go/no-go gate for the social-ai project — aggregates quality gate, contract check, replay evidence, observability, and rollout/rollback readiness into a single ship decision.
---

When the user types `/social-release-gate <change>` or asks for a final ship decision, orchestrate a release-readiness review.

## Mission
Produce a single, evidence-backed ship / hold / no-ship verdict for `<change>`.

## Execution sequence
1. Act as **@social-lead**; restate change + blast radius.
2. Load **social-project-core**, **social-observability-rollout**.
3. Aggregate evidence (invoke or reference): **social-quality-gate**, **social-contract-check**, **social-replay**.
4. Act as **@quality-gatekeeper** and **@sre-rollout** and verify the release checklist:
   - tests green (unit/integration/replay/golden)
   - contracts proven backward-compatible (or migration approved)
   - metrics + alerts in place
   - rollout ladder + rollback triggers defined
   - publish-safety: autopublish gated / draft-first where applicable
   - policy/disclosure checks pass for publish-affecting changes
5. Return: Verdict (ship/hold/no-ship) · Evidence per item · Outstanding blockers · Required follow-ups · Rollback plan.

## Rules
- No green evidence → no ship. Blockers are explicit and actionable.
- Default to the safer staged path for external-facing/publish changes.

## Model lane
**claude-opus + Planning**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
