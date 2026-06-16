---
name: social-fast-contract-check
description: Fast bounded backward-compatibility check for a single social-ai boundary (one stream envelope, one JSON/LLM schema, one DTO, one adapter port).
---

When the user types `/social-fast-contract-check <boundary>`, do a quick single-boundary compatibility pass.

## Steps
1. Identify the one producer + its consumers.
2. Diff current vs proposed at field level.
3. Classify: additive / backward-compatible / breaking (with proof).
4. If breaking → list affected consumers + minimal migration + golden test; recommend escalation to **social-contract-check**.

## Rules
- Timestamp fields/units explicit.
- Rename/type change is breaking unless proven otherwise.
- Recommend a golden payload test for the changed boundary.

## Model lane
**claude-haiku-4-5 (fast mode)**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
