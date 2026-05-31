---
type: dashboard
title: Metrics Coverage Dashboard
section: metrics
tags: [dashboard, metrics, coverage]
updated_at: 2026-04-18
---

# Metrics Coverage Dashboard

## Metric notes
```dataview
TABLE file.link, updated_at
FROM "70_Metrics"
WHERE contains(file.name, "Metrics") OR contains(file.name, "Health") OR contains(file.name, "SLO")
SORT file.name ASC
```

## Services without obvious metric note link
```dataview
TABLE file.link AS service, updated_at
FROM "20_Services"
WHERE type = "service"
SORT file.name ASC
```

## Metric docs quick access
- [[Metrics Index]]
- [[Service SLOs]]
- [[Data Quality Metrics]]
- [[OF Gate Metrics]]
- [[ML Confirm Metrics]]
- [[Execution Metrics]]
- [[Redis Stream Health]]

## Suggested usage
Use this page as a manual coverage checklist: every critical service should map to at least one metrics or SLO note and one runbook.
