---
name: trade_virtual_trades_last_hour_fast
description: Fast workflow for counting virtual trades in the last 60 minutes using a pinned canonical source only. Use Gemini Flash / Fast mode. Do not scan the repository broadly. Fail fast if the canonical source is unavailable.
---

# /trade-virtual-trades-last-hour-fast

## Goal
Return the number of virtual trades created in the last 60 minutes with minimal token and reasoning cost.

## Default lane
- Use Gemini Flash.
- Use Fast mode.
- Keep the scope minimal.

## Hard rule
Do **not** auto-discover the source across the repository.
Use only the pinned canonical source from this workflow.
If it is missing or clearly wrong, stop and report that the source must be updated.

## Pinned canonical source
Prefer exactly one source, in this order:

1. **Postgres / Timescale**
   - table: `trades_closed`
   - time column: `created_at`
   - optional virtual flag column: `is_virtual = true`

2. **Redis fallback** only if the project already exposes an exact known key for virtual trades counts.
   - do not search for alternative keys
   - do not inspect many streams

## Time contract
- Window: `now() - interval '60 minutes'` to `now()`
- Timezone: UTC unless the project explicitly uses another canonical timezone
- Be explicit about the chosen timezone in the final answer

## Execution plan
1. Read only the minimal files or config needed to confirm the pinned source exists.
2. If the `trades_closed` table exists, run the count query.
3. If `is_virtual` exists, include it in the filter.
4. If the table or columns do not exist, fail fast and report exactly what must be changed in this workflow.
5. Do not redesign schema.
6. Do not perform repository-wide search.

## Preferred SQL
Use the smallest correct query.

### Variant A — with explicit virtual flag
```sql
SELECT COUNT(*) AS virtual_trades_last_hour
FROM trades_closed
WHERE is_virtual = TRUE
  AND created_at >= NOW() - INTERVAL '60 minutes'
  AND created_at < NOW();
```

### Variant B — if the table already stores only virtual trades
```sql
SELECT COUNT(*) AS virtual_trades_last_hour
FROM trades_closed
WHERE created_at >= NOW() - INTERVAL '60 minutes'
  AND created_at < NOW();
```

## Output format
Return only:
1. Facts
2. Assumptions
3. Risks
4. Result
5. Evidence

## Result requirements
- Report the numeric count.
- Report the exact window used.
- Report the exact source used.
- Report whether the result came from SQL or Redis.

## Fail-fast conditions
Stop and report instead of searching broadly if:
- `trades_closed` table is absent
- `created_at` is absent
- the schema clearly uses another canonical source
- more than one plausible source exists and no canonical one is documented

## Escalate only if
- counting requires cross-service reasoning
- multiple competing sources exist
- the request changes from counting to redesigning storage or metrics

## Token discipline
- No broad repo scans
- No long explanations
- No architecture discussion
- No speculative fixes beyond the pinned source
