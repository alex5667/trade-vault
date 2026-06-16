---
name: social-contract-check
description: Audit producer-consumer contracts and backward compatibility across Redis Streams, REST/WebSocket, JSON schemas, LLM output schemas, platform-adapter ports, and DB boundaries in the social-ai project.
---

When the user types `/social-contract-check <scope>` or asks for a compatibility review, run a contract-regression audit.

## Mission
Detect schema drift and compatibility risk for `<scope>`.

## Execution sequence
1. Act as **@social-lead** and identify producers, consumers, and storage/port boundaries (streams, REST/WS, JSON schemas, LLM output schemas, adapter ports, DB).
2. Load **social-project-core**; load subsystem skills as needed (Go, Python, NestJS/Next.js, Timescale, LLM, adapters).
3. Act as **@contract-governor** and produce: current contract · proposed contract · field-level diff · compatibility classification · affected consumers · migration/deprecation path · golden fixtures + tests.
4. End with: breaking-change verdict · required mitigations · rollout sequence · rollback plan.

## Scope includes
- Stream envelopes: `social_event.v1, trend_candidate.v1, content_brief.v1, render_job.v1, publish_job.v1, outcome_event.v1, policy_decision.v1`
- Feature registry schemas: `trend_features_v1 … commerce_features_v1`
- LLM agent output schemas (pinned)
- Platform-adapter port signatures

## Rules
- Be explicit about timestamp fields and units.
- Never treat field rename/type change as non-breaking without proof.
- Always recommend golden payload tests for changed boundaries.
- LLM schema changes: keep additive; pin prompt+model version alongside.

## Model lane
**claude-haiku-4-5** for bounded checks; escalate to sonnet/opus if compatibility can't be proven locally or migration is non-trivial.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
