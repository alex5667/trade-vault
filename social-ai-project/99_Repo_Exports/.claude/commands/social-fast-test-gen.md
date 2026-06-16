---
name: social-fast-test-gen
description: Fast lane to generate focused tests for a bounded social-ai change — unit/contract/golden tests for one function, schema, stream consumer, or LLM agent output.
---

When the user types `/social-fast-test-gen <target>`, generate focused tests, no orchestration.

## Steps
1. Identify the unit under test and its contract.
2. Generate the smallest meaningful set: happy path, edge cases, bad-data/quarantine, idempotency where relevant.
3. For schemas/streams: golden payload test. For LLM agents: schema-valid + required-fields + reason-codes-valid + score-in-range (not exact text).
4. Output runnable test files (pytest / go test / jest per stack).

## Rules
- Cover the detect→sanitize→quarantine path for ingestion/data code.
- Deterministic; no live API / wall-clock dependency (use fixtures/archives).

## Model lane
**claude-haiku-4-5 (fast mode)**.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
