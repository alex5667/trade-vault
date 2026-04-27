# Trade model routing for Antigravity

This repository uses a cost-aware routing policy for Antigravity.

## Default
- Model lane: **Gemini Flash**
- Conversation mode: **Fast**

Use this by default for:
- small local fixes
- repo/file lookup
- additive contract checks
- DTO updates
- test scaffolding
- docs updates
- bounded benchmark setup
- log summarization

## Escalate to premium reasoning model
Switch the current conversation/agent to a premium model in **Planning** mode when:
- more than 2 subsystems are affected
- architecture or rollout policy is being redesigned
- the incident root cause is ambiguous
- backward compatibility is at risk
- ML/replay/regime/execution logic is being redesigned
- DB lifecycle / retention / compression strategy is changing

## Process
1. Use Flash first to narrow the scope and produce a draft.
2. Escalate only when explicit triggers fire.
3. Keep premium sessions focused on high-risk decisions.
4. Prefer `/trade-fast-*` workflows for routine work.
5. Prefer `/trade-pro-*` workflows for complex and high-risk work.
