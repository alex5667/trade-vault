---
type: dashboard
title: ADR Dashboard
section: architecture
tags: [dashboard, adr, architecture]
updated_at: 2026-04-18
---

# ADR Dashboard

## All ADRs
```dataview
TABLE adr_id, status, date, updated_at
FROM "80_Research"
WHERE type = "adr"
SORT adr_id ASC
```

## Accepted ADRs
```dataview
TABLE adr_id, date, file.link
FROM "80_Research"
WHERE type = "adr" AND status = "accepted"
SORT date DESC
```

## Non-accepted ADRs
```dataview
TABLE adr_id, status, date, file.link
FROM "80_Research"
WHERE type = "adr" AND status != "accepted"
SORT date DESC
```

## By status
```dataview
TABLE rows.file.link AS adrs
FROM "80_Research"
WHERE type = "adr"
GROUP BY status
SORT status ASC
```

## Links
- [[ADR Index]]
- [[Decision Log]]
- [[Research Index]]
- [[Ops Control Center]]
