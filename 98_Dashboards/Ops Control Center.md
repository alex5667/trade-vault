---
type: dashboard
title: Ops Control Center
section: ops
tags: [dashboard, ops, noc]
updated_at: 2026-04-18
---

# Ops Control Center

## Open incidents
```dataview
TABLE severity, service, status, updated_at
FROM "50_Incidents"
WHERE type = "incident" AND status != "closed"
SORT updated_at DESC
```

## Ready / high-risk rollouts
```dataview
TABLE risk_level, service, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout" AND (status = "ready" OR risk_level = "high")
SORT risk_level DESC, updated_at DESC
```

## High-severity runbooks
```dataview
TABLE severity, service, trigger, updated_at
FROM "40_Runbooks"
WHERE type = "runbook"
SORT severity DESC, updated_at DESC
```

## Recent architectural decisions
```dataview
TABLE adr_id, status, date
FROM "80_Research"
WHERE type = "adr"
SORT date DESC
LIMIT 10
```

## Critical navigation
- [[Incidents Dashboard]]
- [[Rollouts Dashboard]]
- [[Runbooks Dashboard]]
- [[ADR Dashboard]]
- [[Rollback Policy]]
- [[Service SLOs]]
