ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS batch_id text;
ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS input_chars bigint;
ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS output_chars bigint;
ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS estimated_cost_usd double precision;
ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS actual_cost_usd double precision;
ALTER TABLE llm_analysis_runs ADD COLUMN IF NOT EXISTS context_cache_ref text;

CREATE TABLE IF NOT EXISTS llm_recommendation_feedback (
  recommendation_id   text NOT NULL,
  analysis_run_id     text,
  ts_ms               bigint NOT NULL,
  verdict             text NOT NULL,
  action              text NOT NULL,
  target              text NOT NULL,
  reviewer            text NOT NULL,
  reason_code         text,
  prompt_version      text,
  policy_version      text,
  notes               text,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_batch_ts
  ON llm_analysis_runs (batch_id, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_provider_ts
  ON llm_analysis_runs (provider, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendation_feedback_action_ts
  ON llm_recommendation_feedback (action, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendation_feedback_verdict_ts
  ON llm_recommendation_feedback (verdict, ts_ms DESC);
