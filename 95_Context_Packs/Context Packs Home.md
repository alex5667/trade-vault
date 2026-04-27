---
type: dashboard
tags: [context-pack, llm, vault, dashboard]
section: context-packs
updated_at: 2026-04-18
---

# Context Packs Home

## Purpose
Context Pack — это короткий, контролируемый пакет контекста для внешней модели.  
Он нужен, чтобы не отправлять весь vault, а передавать только:
- задачу,
- summary,
- ключевые facts,
- список note-path,
- 2–5 excerpt’ов.

## Recommended flow
1. Найдите релевантные notes локально в Obsidian.
2. Сожмите их локальной LLM или вручную.
3. Сохраните результат как отдельный Context Pack.
4. Передайте наружу только Context Pack, а не весь vault.

## Templates
- [[tpl-context-pack]]
- [[Claude Code Pack]]
- [[Codex Pack]]
- [[Antigravity Pack]]

## Automation
Скрипт сборки из выбранных notes:
- `99_Automation/scripts/build_context_pack.py`

## Operational rules
- Не включайте секреты, токены, приватные ключи.
- Один pack = одна задача.
- Пакет должен быть коротким и проверяемым.
- Для production-изменений всегда добавляйте явные разделы: факты, риски, rollback.

## Suggested storage
- `95_Context_Packs/active/`
- `95_Context_Packs/archive/`

## Minimal quality bar
- Чёткая цель
- 3–10 source notes
- Summary < 800 слов
- Excerpts только нужные
- Явный final ask к внешней модели
