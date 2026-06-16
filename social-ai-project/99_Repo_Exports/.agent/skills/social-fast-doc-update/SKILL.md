---
name: social-fast-doc-update
description: Fast lane to update docs/runbooks/schemas-README for a bounded social-ai change — keep architecture/runbook/contract docs in sync with code.
---

When the user types `/social-fast-doc-update <change>`, update the relevant docs only.

## Steps
1. Identify which doc(s) the change affects: `docs/architecture`, `docs/runbooks`, `docs/rollout`, `docs/policy`, `docs/api`, or `schemas/*` README.
2. Apply the minimal accurate edit (diff).
3. Keep contracts, stream names, metric names, and state-machine states consistent with code.
4. Note anything now stale that needs a follow-up.

## Rules
- Docs must match the real contract (no aspirational drift).
- One-line changelog entry if a contract/metric/state changed.

## Model lane
**claude-haiku-4-5 (fast mode)**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
