BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_slo_rollups (
    id                    bigserial PRIMARY KEY,
    apply_rate            double precision NOT NULL,
    verify_keep_rate      double precision NOT NULL,
    rollback_mttr_p50_sec double precision NOT NULL,
    rollback_mttr_p95_sec double precision NOT NULL,
    ts_ms                 bigint NOT NULL,
    created_at            timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_slo_rollups_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_retry_results (
    id              bigserial PRIMARY KEY,
    action          text NOT NULL,
    target_json     jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_retry_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_escalations (
    id              bigserial PRIMARY KEY,
    severity        text NOT NULL,
    metrics_json    jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_escalations_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_escalations(ts_ms DESC);

COMMIT;
