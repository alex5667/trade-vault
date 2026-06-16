---
name: social-fast-fix
description: Fast lane for a small, bounded, low-risk social-ai fix (1-2 files / one narrow subsystem). Direct local fix with a minimal test, no orchestration.
---

When the user types `/social-fast-fix <issue>` or asks for a small bounded fix, stay in the fast lane.

## When this applies
Local, bounded, clear, low-risk, additive, limited to 1-2 files or one narrow subsystem. If it grows beyond that → escalate to **social-parallel-investigation** or **social-rollout**.

## Steps
1. Load **social-project-core** (lightly).
2. Restate the issue in one line; state the smallest correct fix.
3. Apply the diff (exact files).
4. Add/adjust the minimal test that proves it.
5. Note any metric/log touched and backward-compat impact (1 line).

## Rules
- Preserve stream/REST/WS/adapter contracts. No breaking change in fast lane.
- Time units explicit if touched. No silent failure.
- If the fix reveals a contract/architecture issue → stop and escalate.

## Model lane
**claude-haiku-4-5 (fast mode)**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
