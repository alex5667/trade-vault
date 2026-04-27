-- P3.3 autonomy: replay/rehydrate SLO materialized summary
-- Provides per-window counters for replay/rehydrate operations and quarantine events.
-- Refreshed periodically by scripts/refresh_execution_replay_slo_summary.py.

DROP MATERIALIZED VIEW IF EXISTS execution_replay_slo_summary_mv;

CREATE MATERIALIZED VIEW execution_replay_slo_summary_mv AS
WITH windows AS (
  SELECT '1h'::text AS window_name, ((extract(epoch from now() - interval '1 hour') * 1000)::bigint) AS from_ms
  UNION ALL
  SELECT '24h'::text, ((extract(epoch from now() - interval '24 hour') * 1000)::bigint)
  UNION ALL
  SELECT '7d'::text, ((extract(epoch from now() - interval '7 day') * 1000)::bigint)
),
rehydrate AS (
  SELECT
    w.window_name,
    count(*) FILTER (WHERE e.event_type = 'state_rehydrated') AS rehydrate_total,
    count(*) FILTER (WHERE e.event_type = 'state_rehydrated' AND coalesce(e.payload_jsonb->>'rehydrate_source','') = 'stream') AS rehydrate_stream_total,
    count(*) FILTER (WHERE e.event_type = 'state_rehydrated' AND coalesce(e.payload_jsonb->>'rehydrate_source','') = 'sql') AS rehydrate_sql_total,
    count(*) FILTER (WHERE e.event_type = 'state_rehydrated' AND coalesce((e.payload_jsonb->>'replay_truncated')::int, 0) = 1) AS replay_truncated_total,
    count(*) FILTER (WHERE e.event_type = 'state_rehydrated' AND coalesce((e.payload_jsonb->>'retention_guard_triggered')::int, 0) = 1) AS retention_guard_total,
    percentile_cont(0.95) within group (order by coalesce((e.payload_jsonb->>'replay_latency_ms')::double precision, 0)) FILTER (WHERE e.event_type = 'state_rehydrated') AS replay_latency_p95_ms
  FROM windows w
  LEFT JOIN execution_order_events e
    ON e.event_ts_ms >= w.from_ms
  GROUP BY w.window_name
),
quarantine AS (
  SELECT
    w.window_name,
    count(*) FILTER (WHERE q.action = 'REPLAY_MISMATCH_QUARANTINED') AS replay_mismatch_quarantine_total,
    count(*) FILTER (WHERE q.action = 'RETENTION_GUARD_QUARANTINED') AS retention_guard_quarantine_total
  FROM windows w
  LEFT JOIN execution_quarantine_ledger q
    ON q.created_at_ms >= w.from_ms
  GROUP BY w.window_name
)
SELECT
  r.window_name,
  r.rehydrate_total,
  r.rehydrate_stream_total,
  r.rehydrate_sql_total,
  r.replay_truncated_total,
  r.retention_guard_total,
  q.replay_mismatch_quarantine_total,
  q.retention_guard_quarantine_total,
  coalesce(r.replay_latency_p95_ms, 0)::double precision AS replay_latency_p95_ms,
  (extract(epoch from now()) * 1000)::bigint AS refreshed_at_ms
FROM rehydrate r
JOIN quarantine q USING (window_name);

ALTER MATERIALIZED VIEW execution_replay_slo_summary_mv OWNER TO trading;
