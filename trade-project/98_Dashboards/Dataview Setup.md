---
type: dashboard
title: Dataview Setup
tags: [dashboard, dataview, setup]
updated_at: 2026-04-18
---

# Dataview Setup

## Minimal setup
1. Install and enable the **Dataview** plugin in Obsidian
2. Keep YAML frontmatter on notes
3. Rebuild vault index if new files do not appear immediately

## Expected note types
- `incident`
- `rollout`
- `runbook`
- `adr`
- `service`
- `dashboard`

## Recommended fields
```yaml
type:
service:
status:
severity:
risk_level:
updated_at:
tags:
```

## Query assumptions in this package
- incidents live under `50_Incidents`
- rollouts live under `60_Rollouts`
- ADRs live under `80_Research`
- runbooks live under `40_Runbooks`
- services live under `20_Services`

## Debug checklist
- note exists inside current vault
- file has valid YAML frontmatter
- `type` is set correctly
- Dataview plugin is enabled
- vault indexing finished

## Fast links
- [[Dashboard Home]]
- [[Note Lint Checklist]]
- [[Naming Rules]]
