---
type: dashboard
title: Services Coverage Dashboard
section: services
tags: [dashboard, services, coverage]
updated_at: 2026-04-18
---

# Services Coverage Dashboard

## Service catalog
```dataview
TABLE criticality, inputs, outputs, updated_at
FROM "20_Services"
WHERE type = "service"
SORT file.name ASC
```

## Services by criticality
```dataview
TABLE rows.file.link AS services
FROM "20_Services"
WHERE type = "service"
GROUP BY criticality
SORT criticality ASC
```

## Service-to-ops checklist
```dataview
TABLE file.link, criticality, updated_at
FROM "20_Services"
WHERE type = "service"
SORT updated_at DESC
```

## Manual review prompts
- does each service have at least one runbook?
- does each critical service map to metrics / SLO notes?
- does each trade-impacting service have rollback guidance?
- does each service note list inputs / outputs / dependencies?

## Links
- [[Services Index]]
- [[Runbooks Dashboard]]
- [[Metrics Coverage Dashboard]]
- [[Rollouts Dashboard]]
