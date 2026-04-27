---
type: dashboard
tags: [dashboard, context-pack, dataview]
section: 98_Dashboards
---

# Context Packs Dashboard

## Active packs
```dataview
TABLE file.link AS Pack, topic AS Topic, created_at AS Created, source_notes AS Notes
FROM "95_Context_Packs/active"
WHERE type = "context_pack"
SORT file.name DESC
```

## Archived packs
```dataview
TABLE file.link AS Pack, topic AS Topic, created_at AS Created, source_notes AS Notes
FROM "95_Context_Packs/archive"
WHERE type = "context_pack"
SORT file.name DESC
```

## All context packs
```dataview
TABLE file.link AS Pack, topic AS Topic, created_at AS Created, tags
FROM "95_Context_Packs"
WHERE type = "context_pack"
SORT created_at DESC
```

## Packs missing source notes
```dataview
TABLE file.link AS Pack, topic AS Topic
FROM "95_Context_Packs"
WHERE type = "context_pack" AND (!source_notes OR length(source_notes) = 0)
SORT file.name ASC
```
