---
type: dashboard
title: Rollout Follow-up Queue
section: views
tags: [dashboard, dataview, rollout, queue]
updated_at: 2026-04-18
---

# Rollout Follow-up Queue

## Active rollouts
```dataview
TABLE service, risk_level, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout" AND status != "closed"
SORT updated_at desc
```

## High-risk rollouts
```dataview
TABLE service, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout" AND risk_level = "high"
SORT updated_at desc
```

## Rollouts missing rollback reference
```dataview
TABLE service, risk_level, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout"
AND !contains(file.outlinks, [[Rollback Policy]])
SORT updated_at desc
```

## Follow-up checklist
- post-rollout verification completed
- control metrics reviewed after change window
- rollback criteria still valid
- config diff or deployment marker recorded
- any new failure mode linked to incident or runbook
