CREATE TABLE IF NOT EXISTS llm_incident_rca_runs (
  analysis_run_id         text PRIMARY KEY,
  recommendation_id       text NOT NULL,
  ts_ms                   bigint NOT NULL,
  provider                text NOT NULL,
  model_name              text NOT NULL,
  prompt_version          text,
  policy_version          text,
  status                  text NOT NULL,
  latency_ms              bigint,
  estimated_cost_usd      double precision,
  output_json             jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_incident_rca_runs_rec_ts
  ON llm_incident_rca_runs (recommendation_id, ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_incident_rca_results (
  recommendation_id       text PRIMARY KEY,
  latest_analysis_run_id  text NOT NULL,
  latest_ts_ms            bigint NOT NULL,
  provider                text NOT NULL,
  model_name              text NOT NULL,
  severity                text,
  summary                 text,
  result_json             jsonb NOT NULL
);
