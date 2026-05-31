-- P4.8: SQL ledger for repeated risk mismatch quarantine and materialized mismatch summary.

CREATE TABLE IF NOT EXISTS risk_mismatch_quarantine_ledger (
  id BIGSERIAL PRIMARY KEY,
  decision_id TEXT NULL,
  sid TEXT NULL,
  signal_id TEXT NULL,
  symbol TEXT NOT NULL DEFAULT '',
  tier TEXT NOT NULL DEFAULT '',
  repeated_count INTEGER NOT NULL DEFAULT 0,
  mismatch_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
  reasons_jsonb JSONB NOT NULL DEFAULT '[]'::jsonb,
  source TEXT NOT NULL DEFAULT 'risk_consistency_checker',
  quarantine_action TEXT NOT NULL DEFAULT 'REPEATED_MISMATCH_QUARANTINED',
  created_ts_ms BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk_mismatch_summary_mv AS
WITH base AS (
  SELECT '1h'::text AS window_name, *
  FROM risk_mismatch_quarantine_ledger
  WHERE created_ts_ms >= (extract(epoch from now() - interval '1 hour')*1000)::bigint
  UNION ALL
  SELECT '24h'::text AS window_name, *
  FROM risk_mismatch_quarantine_ledger
  WHERE created_ts_ms >= (extract(epoch from now() - interval '24 hours')*1000)::bigint
  UNION ALL
  SELECT '7d'::text AS window_name, *
  FROM risk_mismatch_quarantine_ledger
  WHERE created_ts_ms >= (extract(epoch from now() - interval '7 days')*1000)::bigint
)
SELECT
  window_name,
  COALESCE(NULLIF(tier, ''), 'UNKNOWN') AS tier,
  COUNT(*)::bigint AS quarantine_count,
  COUNT(DISTINCT sid)::bigint AS distinct_sid_count,
  AVG(repeated_count)::double precision AS avg_repeated_count,
  MAX(repeated_count)::integer AS max_repeated_count,
  AVG(mismatch_rate)::double precision AS avg_mismatch_rate,
  MAX(created_ts_ms)::bigint AS latest_created_ts_ms,
  (extract(epoch from now())*1000)::bigint AS refreshed_ts_ms
FROM base
GROUP BY window_name, COALESCE(NULLIF(tier, ''), 'UNKNOWN');
