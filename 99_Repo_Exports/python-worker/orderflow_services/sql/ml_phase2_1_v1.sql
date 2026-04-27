ALTER TABLE llm_incident_rca_results
  ADD COLUMN IF NOT EXISTS prompt_version text,
  ADD COLUMN IF NOT EXISTS policy_version text,
  ADD COLUMN IF NOT EXISTS output_hash text,
  ADD COLUMN IF NOT EXISTS quality_score double precision,
  ADD COLUMN IF NOT EXISTS usefulness_score double precision;

CREATE TABLE IF NOT EXISTS llm_incident_rca_quality (
  recommendation_id       text NOT NULL,
  ts_ms                   bigint NOT NULL,
  output_hash             text NOT NULL,
  quality_score           double precision NOT NULL,
  quality_reasons_json    jsonb NOT NULL,
  parts_json              jsonb NOT NULL,
  prompt_version          text,
  policy_version          text,
  PRIMARY KEY (recommendation_id, output_hash)
);

CREATE INDEX IF NOT EXISTS idx_llm_incident_rca_quality_rec_ts
  ON llm_incident_rca_quality (recommendation_id, ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_incident_rca_feedback (
  recommendation_id       text NOT NULL,
  ts_ms                   bigint NOT NULL,
  reviewer                text NOT NULL,
  decision                text NOT NULL,
  action_type             text,
  usefulness_score        double precision NOT NULL,
  note                    text,
  PRIMARY KEY (recommendation_id, ts_ms, reviewer)
);

CREATE INDEX IF NOT EXISTS idx_llm_incident_rca_feedback_action_ts
  ON llm_incident_rca_feedback (action_type, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_incident_rca_results_latest_ts
  ON llm_incident_rca_results (latest_ts_ms DESC);
