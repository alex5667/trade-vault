# Codex Agents — Social AI Infra

`agents.md` is the specialist role catalog (mirrored from `.claude/agents/agents.md`). Treat each `@role` as a reasoning/review persona invoked inside workflows via `Act as @role`, or directly for a focused task.

Orchestrator: **@social-lead**. For any non-trivial `s:` request, start there, load `social-project-core`, determine blast radius, distribute to specialists, and merge into one answer (Facts / Assumptions / Risks). Escalate per `.codex/settings.json` triggers.
