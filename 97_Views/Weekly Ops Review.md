---
type: dashboard
title: Weekly Ops Review
section: views
tags: [dashboard, dataview, weekly, ops]
updated_at: 2026-04-18
---

# Weekly Ops Review

## This week's operational focus
- open [[Incident Review Queue]]
- review [[Runbooks Dashboard]]
- review [[Rollout Follow-up Queue]]
- verify [[Service SLOs]] and [[Redis Stream Health]]

## Recently updated incidents
```dataview
TABLE severity, service, status, updated_at
FROM "50_Incidents"
WHERE type = "incident"
SORT updated_at desc
LIMIT 7
```

## Recently updated runbooks
```dataview
TABLE service, severity, updated_at
FROM "40_Runbooks"
WHERE type = "runbook"
SORT updated_at desc
LIMIT 7
```

## Recently updated rollouts
```dataview
TABLE service, risk_level, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout"
SORT updated_at desc
LIMIT 7
```

## Weekly prompts
- Which alert pages were actionable vs noisy?
- Which runbook is stale relative to the last real incident?
- Did any rollout lack a measurable success criterion?
- Which stream or service needs tighter observability this week?
