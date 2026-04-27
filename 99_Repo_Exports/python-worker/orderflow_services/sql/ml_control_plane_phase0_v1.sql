-- Phase-0 ML Control Plane schema.
-- Safe to apply multiple times.

CREATE TABLE IF NOT EXISTS ml_model_registry (
  model_id                text PRIMARY KEY,
  family                  text NOT NULL,
  kind                    text NOT NULL,
  artifact_uri            text NOT NULL,
  schema_ver              text,
  schema_hash             text,
  promotion_state         text NOT NULL,
  champion_flag           boolean NOT NULL DEFAULT false,
  owner_service           text NOT NULL,
  created_at_ms           bigint NOT NULL,
  promoted_at_ms          bigint
);

CREATE TABLE IF NOT EXISTS ml_training_runs (
  training_run_id         text PRIMARY KEY,
  ts_ms                   bigint NOT NULL,
  family                  text NOT NULL,
  kind                    text NOT NULL,
  model_id                text,
  status                  text NOT NULL,
  metrics_json            jsonb,
  artifact_uri            text,
  notes_json              jsonb
);

CREATE TABLE IF NOT EXISTS ml_model_runtime_1m (
  ts_ms                   bigint NOT NULL,
  model_id                text NOT NULL,
  symbol                  text NOT NULL DEFAULT '*',
  mode                    text NOT NULL,
  latency_p50_ms          double precision,
  latency_p95_ms          double precision,
  latency_p99_ms          double precision,
  allow_rate              double precision,
  block_rate              double precision,
  abstain_rate            double precision,
  shadow_rate             double precision,
  error_rate              double precision,
  ece                     double precision,
  brier                   double precision,
  psi_top_json            jsonb,
  ks_top_json             jsonb,
  missing_critical_rate   double precision,
  artifact_age_sec        double precision,
  PRIMARY KEY (ts_ms, model_id, symbol)
);

CREATE TABLE IF NOT EXISTS llm_analysis_runs (
  analysis_run_id         text PRIMARY KEY,
  ts_ms                   bigint NOT NULL,
  provider                text NOT NULL,
  model_name              text NOT NULL,
  task_type               text NOT NULL,
  scope_json              jsonb NOT NULL,
  input_refs_json         jsonb NOT NULL,
  output_json             jsonb,
  status                  text NOT NULL,
  latency_ms              bigint,
  cost_usd                double precision
);

CREATE TABLE IF NOT EXISTS llm_recommendations (
  recommendation_id       text PRIMARY KEY,
  analysis_run_id         text NOT NULL,
  ts_ms                   bigint NOT NULL,
  action_type             text NOT NULL,
  target_kind             text NOT NULL,
  target_ref              text NOT NULL,
  risk_level              text NOT NULL,
  recommendation_json     jsonb NOT NULL,
  apply_status            text NOT NULL DEFAULT 'PENDING'
);

DO $$
BEGIN
  PERFORM create_hypertable('ml_model_runtime_1m', 'ts_ms', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
  -- TimescaleDB extension missing; keep plain Postgres table.
  NULL;
END$$;
