---
type: template_like
tags: [context-pack, codex, template]
template_for: codex
updated_at: 2026-04-18
---

# Codex Pack

## Best use
Используйте этот pack для:
- code generation,
- refactor tasks,
- DTO/contract updates,
- script generation,
- tests and fixtures.

## What Codex should receive
- Task
- Relevant files / note paths
- Summary of current state
- Constraints
- Expected outputs

## Recommended ask
```text
Using only this context pack, produce:
- implementation plan
- code changes
- tests
- migration / env notes
- metrics and rollback notes
```

## Constraints block
Всегда указывайте:
- language/runtime
- repo path
- contracts to preserve
- forbidden changes
- expected entrypoints

## Notes
Codex лучше работает, когда контекст:
- меньше,
- точнее,
- содержит file paths и contracts,
- не содержит шумных README целиком.
