---
type: dashboard
title: Weekly Research Review
section: views
tags: [dashboard, dataview, weekly, research]
updated_at: 2026-04-18
---

# Weekly Research Review

## Review path
- [[Hypotheses Backlog]]
- [[Experiments Register]]
- [[AB Tests Register]]
- [[Decision Log]]
- [[ADR Index]]

## Research items updated recently
```dataview
TABLE type, updated_at
FROM "80_Research"
WHERE type = "hypothesis" OR type = "experiment" OR type = "ab_test" OR type = "adr"
SORT updated_at desc
LIMIT 15
```

## Experiments pending production path
```dataview
TABLE status, owner, updated_at
FROM "80_Research"
WHERE type = "experiment"
AND status != "closed"
SORT updated_at desc
```

## Weekly prompts
- Which hypothesis should graduate to experiment?
- Which experiment has enough evidence for rollout or ADR?
- Which A/B test should be stopped early for risk reasons?
- Which research note is missing deterministic replay details?
