---
type: dashboard
title: Runbooks Dashboard
section: runbooks
tags: [dashboard, runbooks, sre]
updated_at: 2026-04-18
---

# Runbooks Dashboard

## All runbooks
```dataview
TABLE severity, service, trigger, updated_at
FROM "40_Runbooks"
WHERE type = "runbook"
SORT severity DESC, updated_at DESC
```

## By service
```dataview
TABLE rows.file.link AS runbooks
FROM "40_Runbooks"
WHERE type = "runbook"
GROUP BY service
SORT length(rows) DESC
```

## Stale runbooks
```dataview
TABLE service, severity, updated_at
FROM "40_Runbooks"
WHERE type = "runbook" AND updated_at <= date(2026-01-01)
SORT updated_at ASC
```

## Response-first runbooks
```dataview
TABLE severity, trigger, file.link
FROM "40_Runbooks"
WHERE type = "runbook" AND (severity = "high" OR severity = "critical")
SORT severity DESC, updated_at DESC
```

## Links
- [[Runbooks Index]]
- [[Redis Lag]]
- [[ML No CFG]]
- [[Stale Book]]
- [[WS Reconnect Storm]]
