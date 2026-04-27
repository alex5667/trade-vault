ALTER TABLE llm_recommendations
    ADD COLUMN IF NOT EXISTS verification_status text,
    ADD COLUMN IF NOT EXISTS verification_ts_ms bigint,
    ADD COLUMN IF NOT EXISTS rollback_triggered boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS llm_post_commit_verifications (
    recommendation_id text PRIMARY KEY,
    ts_ms bigint NOT NULL,
    action_type text NOT NULL,
    target_kind text NOT NULL,
    target_ref text NOT NULL,
    verification_status text NOT NULL,
    reasons_json jsonb NOT NULL,
    executor_mode text NOT NULL,
    replay_status text NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_auto_rollback_events (
    event_id bigserial PRIMARY KEY,
    recommendation_id text NOT NULL,
    ts_ms bigint NOT NULL,
    requested_by text NOT NULL,
    rollback_reason_codes_json jsonb NOT NULL,
    action_type text NOT NULL,
    target_kind text NOT NULL,
    target_ref text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_post_commit_verifications_status_ts
    ON llm_post_commit_verifications (verification_status, ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_auto_rollback_events_ts
    ON llm_auto_rollback_events (ts_ms DESC);
