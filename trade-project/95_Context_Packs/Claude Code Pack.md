---
type: template_like
tags: [context-pack, claude-code, template]
template_for: claude_code
updated_at: 2026-04-18
---

# Claude Code Pack

## Best use
Используйте этот pack для:
- code review,
- patch planning,
- architecture review,
- rollout/rollback plan,
- RCA на основе нескольких notes.

## What to send
1. Task
2. Summary
3. Key facts
4. 2–5 excerpts
5. Explicit ask

## Recommended ask
```text
Review this context pack and produce:
1) goal
2) facts
3) assumptions
4) risks
5) implementation plan
6) tests
7) metrics/alerts
8) rollout/rollback
```

## Guardrails
- Не отправляйте весь vault.
- Не отправляйте сырые большие логи без summary.
- Для production always ask for rollback and observability.
- Для data/time logic ask for epoch ms / stale / skew / duplicates / gaps checks.

## Good pack size
- Summary: 300–800 слов
- Excerpts: 3–8 блоков
- Total: компактно, но достаточно для reasoning
