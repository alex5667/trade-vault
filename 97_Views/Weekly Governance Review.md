---
type: dashboard
title: Weekly Governance Review
section: views
tags: [dashboard, dataview, weekly, governance]
updated_at: 2026-04-18
---

# Weekly Governance Review

## Governance surfaces
- [[ADR Dashboard]]
- [[Runbooks Dashboard]]
- [[Metrics Coverage Dashboard]]
- [[Services Coverage Dashboard]]
- [[Automation Index]]

## ADRs updated recently
```dataview
TABLE adr_id, status, date, updated_at
FROM "80_Research"
WHERE type = "adr"
SORT updated_at desc
LIMIT 10
```

## Runbooks updated recently
```dataview
TABLE service, severity, updated_at
FROM "40_Runbooks"
WHERE type = "runbook"
SORT updated_at desc
LIMIT 10
```

## Metrics notes updated recently
```dataview
TABLE updated_at
FROM "70_Metrics"
WHERE type = "metrics"
SORT updated_at desc
LIMIT 10
```

## Weekly prompts
- Is each high-risk service linked to a runbook and SLO?
- Did a recent incident expose a missing ADR or outdated policy?
- Are note types and naming still passing lint?
- Do dashboards still reflect how the system is actually operated?
