BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_verification_results (
    id            bigserial PRIMARY KEY,
    ts_ms         bigint NOT NULL,
    current_mode  text NOT NULL,
    target_mode   text NOT NULL,
    decision      text NOT NULL,
    reason_code   text NOT NULL,
    advisory_only integer NOT NULL,
    executor_mode text NOT NULL,
    snapshot_json jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_verification_results_ts
    ON llm_route_incident_rca_mirror_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rollback_journal (
    id            bigserial PRIMARY KEY,
    ts_ms         bigint NOT NULL,
    reason_code   text NOT NULL,
    mode_before   text NOT NULL,
    mode_after    text NOT NULL,
    snapshot_json jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rollback_journal_ts
    ON llm_route_incident_rca_mirror_rollback_journal(ts_ms DESC);

COMMIT;
