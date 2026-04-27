---
name: trade_virtual_trades_last_hour
description: Get the count of virtual trades for the last hour from Timescale/Postgres or Redis, return the total, source used, query used, and key assumptions.
---

# /trade-virtual-trades-last-hour

1. Clarify the data source only if it is impossible to infer automatically.
   - Prefer **Postgres/Timescale** if a virtual/paper trades table exists.
   - Otherwise use **Redis** if virtual trades are stored in a stream or list.

2. First inspect the project for the most likely virtual trade source.
   - Search for identifiers like:
     - `virtual trades`
     - `paper trades`
     - `simulated trades`
     - `is_virtual`
     - `is_paper`
     - `mode = virtual`
     - `account_type = paper`
     - `events:trades`
     - `trades:closed`
   - Also inspect `.env*`, config files, SQL migrations, Prisma schema, TypeORM entities, and Redis key helpers.

3. Determine the canonical source using this priority:
   - **A. Postgres/Timescale** if there is a durable table for virtual trades.
   - **B. Redis stream/list** if virtual trades are only kept in Redis.
   - **C. Service endpoint or repository method** if the app already exposes a metric/query for this.

4. If **Postgres/Timescale** is the source, run a query equivalent to the relevant schema.
   Prefer the exact table/column names found in the repo.

   Example patterns to adapt:

   ```sql
   SELECT COUNT(*) AS virtual_trades_last_hour
   FROM trades
   WHERE created_at >= NOW() - INTERVAL '1 hour'
     AND is_virtual = TRUE;
   ```

   or

   ```sql
   SELECT COUNT(*) AS virtual_trades_last_hour
   FROM trade_events
   WHERE event_time >= NOW() - INTERVAL '1 hour'
     AND trade_mode = 'virtual';
   ```

   If the table is hypertable-based, use the event timestamp column actually used by the service.

5. If **Redis** is the source, inspect the actual key format first.
   - Use the exact redis key discovered in code.
   - Count only entries from the last 60 minutes.
   - Filter to virtual/paper trades using the real payload field.

   Example approach:
   - read recent entries from `events:trades` or the real key
   - filter by timestamp in epoch ms
   - filter by `is_virtual=true` or `mode=virtual`
   - count matching entries

6. Always normalize time explicitly.
   - Treat timestamps as one of: `epoch_ms`, `epoch_s`, or ISO-8601.
   - State which format was used.
   - If mixed/ambiguous timestamps are found, report the ambiguity and quarantine invalid rows conceptually in the result.

7. Return the result in this exact structure:

   ```text
   Goal
   Count virtual trades for the last hour.

   Facts
   - Source used: <postgres|redis|service>
   - Entity used: <table/key/method>
   - Time column or field: <name>
   - Timestamp format: <epoch_ms|epoch_s|iso8601>

   Assumptions
   - <only if needed>

   Risks
   - <missing retention / mixed timestamps / possible duplicate events>

   Result
   - virtual_trades_last_hour = <number>
   - window_start = <timestamp>
   - window_end = <timestamp>

   Evidence
   - query/command used

   Next checks
   - optional: by symbol
   - optional: open vs closed
   - optional: per-strategy breakdown
   ```

8. If the source is ambiguous, do not guess silently.
   - Present the 2-3 strongest candidates.
   - State what prevented a definitive count.
   - Provide the exact query/command for each candidate.

9. If there is already an internal repository method or API for this metric, prefer using that path over inventing a new one.

10. Keep output compact.
    Do not redesign architecture.
    Do not scan unrelated parts of the repo once the source is confirmed.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
