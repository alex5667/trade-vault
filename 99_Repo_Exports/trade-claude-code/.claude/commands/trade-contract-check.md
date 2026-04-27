---
name: trade-contract-check
description: Audit producer-consumer contracts and backward compatibility across Redis, WebSocket, REST, and DB boundaries in the trade project
---

When the user types `/trade-contract-check <scope>` or asks for a compatibility review, run a contract-regression audit.

## Mission
Detect schema drift and compatibility risk for `<scope>`.

## Execution sequence
1. Act as **@trade-lead** and identify producers, consumers, and storage boundaries.
2. Load **trade-project-core** and **trade-contract-regression**.
3. Add subsystem skills as needed for Go, Python, NestJS, Next.js, or Timescale.
4. Act as **@contract-governor** and produce:
   - current contract
   - proposed contract
   - field-level diff
   - compatibility classification
   - affected consumers
   - migration/deprecation path
   - golden fixtures and tests
5. End with:
   - breaking-change verdict
   - required mitigations
   - rollout sequence
   - rollback plan

## Rules
- Be explicit about timestamp fields and units.
- Never treat field rename/type change as non-breaking without proof.
- Always recommend golden payload tests for changed boundaries.\n

## Model lane
Default to **Gemini Flash + Fast** for the first pass. Escalate to premium when the triggers below fire.

## Escalation guidance
- use Flash first for bounded compatibility checks
- escalate if compatibility cannot be proven locally or if migration strategy is non-trivial

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
