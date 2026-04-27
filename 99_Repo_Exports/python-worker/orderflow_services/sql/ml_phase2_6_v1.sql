ALTER TABLE IF EXISTS llm_recommendations
    ADD COLUMN IF NOT EXISTS operator_rca_route_verify_status text,
    ADD COLUMN IF NOT EXISTS operator_rca_route_verify_reason_codes jsonb,
    ADD COLUMN IF NOT EXISTS operator_rca_route_verify_ts_ms bigint,
    ADD COLUMN IF NOT EXISTS operator_rca_route_rollback_status text,
    ADD COLUMN IF NOT EXISTS operator_rca_route_rollback_ts_ms bigint;

CREATE TABLE IF NOT EXISTS llm_operator_rca_route_apply_verifications (
    id bigserial PRIMARY KEY,
    recommendation_id text NOT NULL,
    ts_ms bigint NOT NULL,
    verify_status text NOT NULL,
    reason_codes_json jsonb,
    baseline_route_json jsonb,
    applied_route_json jsonb,
    live_snapshot_json jsonb,
    rollback_required boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_route_apply_verifications_rec_ts
    ON llm_operator_rca_route_apply_verifications (recommendation_id, ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_operator_rca_route_rollback_journal (
    id bigserial PRIMARY KEY,
    recommendation_id text NOT NULL,
    ts_ms bigint NOT NULL,
    mode text NOT NULL,
    status text NOT NULL,
    before_route_json jsonb,
    baseline_route_json jsonb,
    applied_route_json jsonb,
    reason_codes_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_route_rollback_journal_rec_ts
    ON llm_operator_rca_route_rollback_journal (recommendation_id, ts_ms DESC);
