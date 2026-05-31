# Codex Trade Workspace Pack

This directory mirrors and adapts the repository's Claude/Cursor operating pack for Codex use.

## Layout
- `skills/`: local trade skills copied from `.claude/skills`.
- `workflows/`: workflow and slash-command playbooks copied from `.claude/commands`.
- `agents/agents.md`: specialist role catalog copied from `.claude/agents/agents.md`.
- `rules/`: Cursor rule references copied from `.cursor/rules`.
- `hooks/`: Claude hook references copied from `.claude/hooks`; kept as documentation/reference, not auto-executed by Codex.
- `claude-settings.reference.json`: source Claude permissions/env reference.
- `cursor-settings.reference.json`: source Cursor editor/tooling reference.
- `settings.json`: Codex-facing routing and safety metadata for this workspace.

## Operating Contract
- Primary user-facing language: Russian.
- `tr:` is the canonical trade entrypoint.
- Route `tr:` requests through `@trade-lead` semantics first.
- Start with the cheapest sufficient local path.
- Escalate only for explicit high-risk triggers:
  - more than two subsystems affected;
  - ambiguous production root cause;
  - architecture, schema, retention, ML, replay, regime, or execution-risk redesign;
  - possible breaking Redis, WebSocket, REST, or storage contract change;
  - rollout / canary / production go-no-go decision.

## Codex Usage Notes
- Treat `skills/*/SKILL.md` as local skill bodies.
- Treat `workflows/*.md` as workflow recipes, not executable shell commands.
- Treat `agents/agents.md` as role definitions for reasoning and review structure.
- Preserve backward compatibility for Redis, WebSocket, API, and storage contracts unless the user explicitly allows a breaking change.
- For production-affecting changes include tests, observability, rollout, and rollback.
