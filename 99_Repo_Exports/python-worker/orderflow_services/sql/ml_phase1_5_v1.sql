ALTER TABLE llm_recommendations
  ADD COLUMN IF NOT EXISTS commit_policy_status text,
  ADD COLUMN IF NOT EXISTS commit_policy_reason text,
  ADD COLUMN IF NOT EXISTS commit_policy_mode text,
  ADD COLUMN IF NOT EXISTS committed_at_ms bigint,
  ADD COLUMN IF NOT EXISTS commit_cooldown_remaining_sec integer;

CREATE TABLE IF NOT EXISTS llm_commit_policy_audit (
  audit_id                 bigserial PRIMARY KEY,
  ts_ms                    bigint NOT NULL,
  recommendation_id        text NOT NULL,
  action_type              text NOT NULL,
  policy_status            text NOT NULL,
  policy_reason            text,
  policy_mode              text,
  dry_run_only             boolean NOT NULL DEFAULT true,
  created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_commit_policy_audit_rec_ts
  ON llm_commit_policy_audit (recommendation_id, ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_commit_executor_results (
  result_id                bigserial PRIMARY KEY,
  ts_ms                    bigint NOT NULL,
  recommendation_id        text NOT NULL,
  action_type              text NOT NULL,
  target_ref               text,
  executor_mode            text NOT NULL,
  status                   text NOT NULL,
  reason                   text,
  change_summary           text,
  created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_commit_executor_results_rec_ts
  ON llm_commit_executor_results (recommendation_id, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_recommendations_commit_policy
  ON llm_recommendations (commit_policy_status, apply_status, action_type);

CREATE INDEX IF NOT EXISTS idx_llm_recommendations_commit_time
  ON llm_recommendations (committed_at_ms DESC);

