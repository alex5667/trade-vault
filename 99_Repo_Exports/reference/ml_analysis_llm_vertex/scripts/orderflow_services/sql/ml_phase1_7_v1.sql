ALTER TABLE llm_recommendations
    ADD COLUMN IF NOT EXISTS rollback_state text,
    ADD COLUMN IF NOT EXISTS rollback_verification_status text,
    ADD COLUMN IF NOT EXISTS rollback_verified_at_ms bigint,
    ADD COLUMN IF NOT EXISTS rollback_failure_reason text;

CREATE TABLE IF NOT EXISTS llm_rollback_verifications (
    id bigserial PRIMARY KEY,
    recommendation_id text NOT NULL,
    verification_ts_ms bigint NOT NULL,
    verification_status text NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    details_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_llm_rollback_verifications_rec_ts
    ON llm_rollback_verifications (recommendation_id, verification_ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_rollback_state_history (
    id bigserial PRIMARY KEY,
    recommendation_id text NOT NULL,
    ts_ms bigint NOT NULL,
    prev_state text,
    event text NOT NULL,
    next_state text NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_llm_rollback_state_history_rec_ts
    ON llm_rollback_state_history (recommendation_id, ts_ms DESC);
