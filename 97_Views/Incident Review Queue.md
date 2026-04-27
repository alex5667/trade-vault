---
type: dashboard
title: Incident Review Queue
section: views
tags: [dashboard, dataview, incidents, queue]
updated_at: 2026-04-18
---

# Incident Review Queue

## Open / in-progress incidents
```dataview
TABLE severity, service, status, updated_at
FROM "50_Incidents"
WHERE type = "incident" AND status != "closed"
SORT updated_at desc
```

## Incidents missing follow-up quality
```dataview
TABLE severity, service, updated_at
FROM "50_Incidents"
WHERE type = "incident"
AND (
  !contains(file.outlinks, [[Rollback Policy]])
  OR !contains(file.outlinks, [[Runbooks Index]])
)
SORT updated_at desc
```

## Last updated incidents
```dataview
TABLE id, severity, service, status, updated_at
FROM "50_Incidents"
WHERE type = "incident"
SORT updated_at desc
LIMIT 10
```

## Review checklist
- root cause explicit, not symptoms only
- blast radius and affected streams documented
- rollback / containment path linked
- at least one prevention action assigned
- metrics / alert gaps captured
