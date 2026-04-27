---
name: trade-contract-regression
description: Use this skill when the task involves Redis payloads, WebSocket events, REST DTOs, DB schemas, producer-consumer compatibility, schema versioning, contract regression, or payload drift in the trade project. Relevant for prompts about контракты, DTO, backward compatibility, schema drift, payloads, Redis, WebSocket, REST, migrations.
---

# Trade Contract Regression

## Goal
Prevent silent breakage across service boundaries.

## Use this skill for
- Redis pub/sub or stream payload changes
- WebSocket message evolution
- REST DTO or response changes
- database schema changes that affect readers/writers
- producer/consumer compatibility review
- deprecation paths and versioning

## Required analysis steps
1. Identify producers and consumers.
2. Write the current contract and the proposed contract.
3. Mark fields as:
   - added optional
   - added required
   - removed
   - renamed
   - type-changed
   - semantic-change
4. Determine compatibility level:
   - backward compatible
   - forward compatible
   - breaking
5. Define migration or deprecation plan.
6. Define golden fixtures and contract tests.
7. Define monitoring for malformed/old-version payloads.

## Preferred artifacts
- canonical JSON examples
- DTO/schema diff
- compatibility matrix by consumer
- golden fixtures stored in tests
- deprecation timeline if breaking change is unavoidable

## Rules
- Prefer additive, versioned evolution.
- Never rename or repurpose fields silently.
- Keep timestamps and units explicit in every contract.
- When changing schemas, define fallback behavior for old consumers.
- When breaking changes are necessary, make blast radius explicit.\n

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- validate only the changed producer/consumer boundaries
- prefer field-level diff and golden fixture recommendations

## Escalate to claude-sonnet-4-6/opus-4-6 if
- compatibility cannot be proven locally
- the change spans Redis + REST + WS + DB together
- migration/deprecation strategy requires architectural reasoning

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
