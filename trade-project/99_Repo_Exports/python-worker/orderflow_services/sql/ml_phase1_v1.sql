CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_ts
  ON llm_analysis_runs (ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_analysis_runs_status
  ON llm_analysis_runs (status, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendations_ts
  ON llm_recommendations (ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendations_apply_status
  ON llm_recommendations (apply_status, ts_ms DESC);
