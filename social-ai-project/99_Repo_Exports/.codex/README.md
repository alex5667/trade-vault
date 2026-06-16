# Codex — Social AI Infra Workspace Pack

This directory adapts the repository's Claude/Antigravity operating pack for Codex use on the **social-ai-infra** project (TikTok / Instagram / YouTube content operating system).

## Layout
- `skills/`: local social skills (knowledge + orchestration), mirrored from `.claude/skills`.
- `workflows/`: workflow / slash-command playbooks, mirrored from `.claude/commands`.
- `agents/agents.md`: specialist role catalog, mirrored from `.claude/agents/agents.md`.
- `rules/`: coding/operating rules (Go, Python, social operating prompt).
- `hooks/`: Claude hook references; documentation/reference, not auto-executed by Codex.
- `settings.json`: Codex-facing routing and safety metadata for this workspace.

## Operating Contract
- Primary user-facing language: **Russian**.
- `s:` is the canonical social entrypoint.
- Route `s:` requests through `@social-lead` semantics first.
- Start with the cheapest sufficient local path.
- Object of control: `social event → trend → content hypothesis → asset → publish → outcome → governor`.
- Escalate only for explicit high-risk triggers:
  - more than two subsystems affected;
  - ambiguous production root cause;
  - architecture, schema, retention, ML, replay, or governor redesign;
  - possible breaking Redis-stream / REST / WS / adapter-port / storage contract change;
  - LLM prompt/schema version redesign;
  - platform-adapter autopublish enablement;
  - rollout / canary / production go-no-go decision.

## Codex Usage Notes
- Treat `skills/*/SKILL.md` as local skill bodies.
- Treat `workflows/*.md` as workflow recipes, not executable shell commands.
- Treat `agents/agents.md` as role definitions for reasoning and review structure.
- Preserve backward compatibility for Redis streams, REST/WS, JSON & LLM output schemas, adapter ports, and storage unless the user explicitly allows a breaking change.
- LLM output is always JSON validated against a pinned schema — never act on raw/hallucinated text.
- Publishing defaults to draft/private/unlisted/shadow until SLOs are stable; no silent autopublish.
- For production-affecting changes include tests, observability, rollout, and rollback.
