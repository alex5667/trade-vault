---
type: dashboard
title: Experiment Review Queue
section: views
tags: [dashboard, dataview, research, experiments, queue]
updated_at: 2026-04-18
---

# Experiment Review Queue

## Active experiments
```dataview
TABLE status, owner, horizon, updated_at
FROM "80_Research"
WHERE type = "experiment"
AND status != "closed"
SORT updated_at desc
```

## Hypotheses backlog
```dataview
TABLE priority, owner, updated_at
FROM "80_Research"
WHERE type = "hypothesis"
SORT priority asc, updated_at desc
```

## A/B tests pending decision
```dataview
TABLE status, owner, updated_at
FROM "80_Research"
WHERE type = "ab_test"
AND status != "closed"
SORT updated_at desc
```

## Review checklist
- success metric and stop metric both present
- sample size / time window defined
- rollout implication captured
- candidate ADR noted if decision changes architecture
- replayability / reproducibility documented
