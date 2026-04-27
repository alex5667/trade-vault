BEGIN;

ALTER TABLE llm_recommendations
    ADD COLUMN IF NOT EXISTS route_retry_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS route_retry_state text NOT NULL DEFAULT 'IDLE',
    ADD COLUMN IF NOT EXISTS route_escalation_state text NOT NULL DEFAULT 'NONE',
    ADD COLUMN IF NOT EXISTS route_last_escalated_ts_ms bigint;

CREATE TABLE IF NOT EXISTS llm_operator_rca_route_slo_rollups (
    ts_ms bigint PRIMARY KEY,
    events_n integer NOT NULL,
    success_n integer NOT NULL,
    failed_n integer NOT NULL,
    success_rate double precision NOT NULL,
    mttr_p50_sec double precision NOT NULL,
    mttr_p95_sec double precision NOT NULL,
    breaches integer NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS llm_operator_rca_route_retry_events (
    event_id text PRIMARY KEY,
    recommendation_id text NOT NULL,
    route_change_id text,
    ts_ms bigint NOT NULL,
    retry_attempt integer NOT NULL,
    reason_code text NOT NULL,
    retry_after_sec double precision NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_operator_rca_route_escalations (
    escalation_id text PRIMARY KEY,
    ts_ms bigint NOT NULL,
    severity text NOT NULL,
    open_items_n integer NOT NULL,
    critical_items_n integer NOT NULL,
    top_reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    summary text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_route_slo_rollups_ts
    ON llm_operator_rca_route_slo_rollups (ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_route_retry_events_rec
    ON llm_operator_rca_route_retry_events (recommendation_id, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_route_escalations_ts
    ON llm_operator_rca_route_escalations (ts_ms DESC);

COMMIT;
