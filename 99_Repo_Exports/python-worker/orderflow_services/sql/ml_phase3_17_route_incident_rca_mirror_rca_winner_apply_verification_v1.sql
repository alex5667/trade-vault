BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_verification_results (
    id              bigserial PRIMARY KEY,
    decision        text NOT NULL,
    reason          text NOT NULL,
    metrics_json    jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_verification_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal (
    id              bigserial PRIMARY KEY,
    failed_winner   text NOT NULL,
    reason          text NOT NULL,
    executor_mode   text NOT NULL,
    new_config_json jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal(ts_ms DESC);

COMMIT;
