ALTER TABLE IF EXISTS llm_recommendations
  ADD COLUMN IF NOT EXISTS executor_mode text,
  ADD COLUMN IF NOT EXISTS executor_status text,
  ADD COLUMN IF NOT EXISTS executor_reason_code text,
  ADD COLUMN IF NOT EXISTS applied_at_ms bigint,
  ADD COLUMN IF NOT EXISTS rollback_available boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS rollback_last_request_ms bigint;

CREATE TABLE IF NOT EXISTS llm_recommendation_apply_results (
  request_id           text PRIMARY KEY,
  recommendation_id    text,
  ts_ms                bigint NOT NULL,
  action_type          text NOT NULL,
  target_kind          text NOT NULL,
  target_ref           text NOT NULL,
  status               text NOT NULL,
  reason_code          text,
  dry_run              boolean NOT NULL DEFAULT true,
  before_json          jsonb,
  after_json           jsonb,
  patch_json           jsonb,
  rollback_json        jsonb
);

CREATE TABLE IF NOT EXISTS llm_recommendation_rollback_journal (
  request_id           text PRIMARY KEY,
  recommendation_id    text,
  ts_ms                bigint NOT NULL,
  action_type          text NOT NULL,
  target_kind          text NOT NULL,
  target_ref           text NOT NULL,
  rollback_json        jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_recommendation_apply_results_ts ON llm_recommendation_apply_results (ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_llm_recommendation_rollback_journal_ts ON llm_recommendation_rollback_journal (ts_ms DESC);
