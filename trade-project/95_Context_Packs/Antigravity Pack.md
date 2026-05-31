---
type: template_like
tags: [context-pack, antigravity, template]
template_for: antigravity
updated_at: 2026-04-18
---

# Antigravity Pack

## Best use
Используйте этот pack для:
- structured investigation,
- repo review,
- workflow-driven analysis,
- contract and architecture verification.

## Recommended structure
1. Goal
2. Current state
3. Relevant notes
4. Excerpts
5. Invariants
6. Ask

## Suggested ask
```text
Work only from this context pack.
Preserve contracts and invariants.
Return:
- findings
- risks
- proposed changes
- validation plan
- rollback plan
```

## Invariants to include for trade project
- epoch ms / sec must be explicit
- no hidden dependencies
- idempotency for Redis/streams
- replayability for decisions
- metrics/logs/alerts for every production change

## Notes
Если pack связан с production path, добавляйте:
- streams involved
- DTO/contracts
- rollout scope
- owner / service boundary
