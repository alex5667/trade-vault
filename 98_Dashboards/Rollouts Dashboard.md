---
type: dashboard
title: Rollouts Dashboard
section: rollouts
tags: [dashboard, rollouts, change-management]
updated_at: 2026-04-18
---

# Rollouts Dashboard

## All rollout playbooks
```dataview
TABLE risk_level, service, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout"
SORT risk_level DESC, updated_at DESC
```

## High-risk changes first
```dataview
TABLE service, status, updated_at
FROM "60_Rollouts"
WHERE type = "rollout" AND risk_level = "high"
SORT updated_at DESC
```

## Ready-to-run rollouts
```dataview
TABLE risk_level, service, updated_at
FROM "60_Rollouts"
WHERE type = "rollout" AND status = "ready"
SORT risk_level DESC, updated_at DESC
```

## Coverage by service
```dataview
TABLE rows.file.link AS playbooks
FROM "60_Rollouts"
WHERE type = "rollout"
GROUP BY service
SORT length(rows) DESC
```

## Links
- [[Rollouts Index]]
- [[Rollback Policy]]
- [[ML Shadow to Enforce]]
- [[New Symbol Onboarding]]
- [[Gate Threshold Change]]
- [[Execution Bridge Cutover]]
