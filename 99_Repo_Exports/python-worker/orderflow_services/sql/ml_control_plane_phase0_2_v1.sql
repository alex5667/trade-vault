-- Phase 0.2: compact per-model snapshots (scanner_infra only)

CREATE INDEX IF NOT EXISTS idx_ml_model_registry_family_kind
  ON ml_model_registry (family, kind);

ALTER TABLE ml_model_registry ADD COLUMN IF NOT EXISTS artifact_exists boolean DEFAULT false;
ALTER TABLE ml_model_registry ADD COLUMN IF NOT EXISTS artifact_age_sec double precision;
ALTER TABLE ml_model_registry ADD COLUMN IF NOT EXISTS mode text;
ALTER TABLE ml_model_registry ADD COLUMN IF NOT EXISTS fail_policy text;
ALTER TABLE ml_model_registry ADD COLUMN IF NOT EXISTS cfg_source text;

CREATE OR REPLACE VIEW ml_model_runtime_latest_v1 AS
SELECT DISTINCT ON (model_id, symbol)
  ts_ms,
  model_id,
  symbol,
  mode,
  latency_p50_ms,
  latency_p95_ms,
  latency_p99_ms,
  allow_rate,
  block_rate,
  abstain_rate,
  shadow_rate,
  error_rate,
  ece,
  brier,
  psi_top_json,
  ks_top_json,
  missing_critical_rate,
  artifact_age_sec
FROM ml_model_runtime_1m
ORDER BY model_id, symbol, ts_ms DESC;

CREATE OR REPLACE VIEW ml_model_snapshot_seed_v1 AS
SELECT
  r.model_id,
  r.family,
  r.kind,
  r.artifact_uri,
  r.schema_ver,
  r.schema_hash,
  r.promotion_state,
  r.champion_flag,
  r.owner_service,
  r.created_at_ms,
  r.promoted_at_ms,
  r.artifact_exists,
  r.artifact_age_sec,
  r.mode,
  r.fail_policy,
  r.cfg_source,
  x.ts_ms        AS latest_runtime_ts_ms,
  x.symbol       AS latest_symbol,
  x.mode         AS latest_mode,
  x.latency_p95_ms,
  x.error_rate,
  x.ece,
  x.brier,
  x.missing_critical_rate
FROM ml_model_registry r
LEFT JOIN LATERAL (
  SELECT *
  FROM ml_model_runtime_latest_v1 m
  WHERE m.model_id = r.model_id
  ORDER BY m.ts_ms DESC, m.symbol ASC
  LIMIT 1
) x ON TRUE;
