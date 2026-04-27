---
type: dashboard
title: Incidents Dashboard
section: incidents
tags: [dashboard, incidents, rca]
updated_at: 2026-04-18
---

# Incidents Dashboard

## Open incidents
```dataview
TABLE severity, service, status, updated_at
FROM "50_Incidents"
WHERE type = "incident" AND status != "closed"
SORT severity ASC, updated_at DESC
```

## Incident timeline / recent updates
```dataview
TABLE id, service, severity, updated_at
FROM "50_Incidents"
WHERE type = "incident"
SORT updated_at DESC
LIMIT 20
```

## By service
```dataview
TABLE rows.file.link AS incidents
FROM "50_Incidents"
WHERE type = "incident"
GROUP BY service
SORT length(rows) DESC
```

## By severity
```dataview
TABLE rows.file.link AS incidents
FROM "50_Incidents"
WHERE type = "incident"
GROUP BY severity
SORT severity ASC
```

## Missing closure hygiene
```dataview
TABLE service, severity, updated_at
FROM "50_Incidents"
WHERE type = "incident" AND !contains(lower(status), "closed")
SORT updated_at ASC
```

## Links
- [[Incidents Index]]
- [[Runbooks Dashboard]]
- [[Rollback Policy]]
- [[Ops Control Center]]
