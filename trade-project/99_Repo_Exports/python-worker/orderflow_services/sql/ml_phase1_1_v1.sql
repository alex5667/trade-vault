ALTER TABLE llm_analysis_runs
  ADD COLUMN IF NOT EXISTS prompt_version text,
  ADD COLUMN IF NOT EXISTS policy_version text,
  ADD COLUMN IF NOT EXISTS compact_hash text;

ALTER TABLE llm_recommendations
  ADD COLUMN IF NOT EXISTS prompt_version text,
  ADD COLUMN IF NOT EXISTS policy_version text,
  ADD COLUMN IF NOT EXISTS compact_hash text;

CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_ts_provider
  ON llm_analysis_runs (ts_ms DESC, provider);

CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_compact_hash
  ON llm_analysis_runs (compact_hash);

