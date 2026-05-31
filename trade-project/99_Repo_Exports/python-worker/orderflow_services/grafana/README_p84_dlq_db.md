# P84 Grafana: OF-Gate DLQ events (DB)

## Prereqs
- Table `of_gate_dlq_events` exists (P83 SQL: `20260224_of_gate_dlq_events_p83.sql`).
- Optional indexes (P84 SQL): `20260224_of_gate_dlq_events_indexes_p84.sql`.
- Grafana has a PostgreSQL datasource.

## Import
Import dashboard JSON:
- `orderflow_services/grafana/of_gate_dlq_events_db_p84.json`

Select datasource variable `DS_POSTGRES`.

## What you get
- DLQ event rate over time per stream
- Top `dq_code`, `reason_code`, `err_prefix`
- Schema version mix

## Useful SQL snippets
Top 20 errors last 24h:
```sql
SELECT split_part(coalesce(err,''),' ',1) AS err_prefix, count(*)
FROM of_gate_dlq_events
WHERE ts > now() - interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
```

Fixability workflow:
1) Use this dashboard to find dominant `dq_code` / `err_prefix`.
2) Run triage tool:
```bash
python -m orderflow_services.of_gate_dlq_fixed_replay_p84 triage --limit 5000
```
3) If fixable share is high, do replay in dry-run, then commit.

