ALTER TABLE IF EXISTS llm_recommendations
    ADD COLUMN IF NOT EXISTS rollback_retry_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rollback_retry_state text,
    ADD COLUMN IF NOT EXISTS rollback_escalation_state text,
    ADD COLUMN IF NOT EXISTS rollback_last_escalated_ts_ms bigint;

CREATE TABLE IF NOT EXISTS llm_rollback_slo_rollups (
    ts_ms bigint NOT NULL,
    total integer NOT NULL,
    success integer NOT NULL,
    failed integer NOT NULL,
    success_rate double precision NOT NULL,
    mttr_sec_p50 double precision NOT NULL,
    mttr_sec_p95 double precision NOT NULL,
    breach_n integer NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (ts_ms)
);

CREATE TABLE IF NOT EXISTS llm_rollback_retry_events (
    recommendation_id text NOT NULL,
    ts_ms bigint NOT NULL,
    attempt integer NOT NULL,
    decision_json jsonb NOT NULL,
    PRIMARY KEY (recommendation_id, ts_ms)
);

CREATE TABLE IF NOT EXISTS llm_rollback_escalations (
    escalation_id text PRIMARY KEY,
    ts_ms bigint NOT NULL,
    severity text NOT NULL,
    summary text NOT NULL,
    payload_json jsonb NOT NULL
);
