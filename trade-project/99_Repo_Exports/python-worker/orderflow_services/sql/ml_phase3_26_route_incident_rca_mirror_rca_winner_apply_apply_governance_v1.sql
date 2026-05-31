BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups (
    id                    bigserial PRIMARY KEY,
    apply_rate            float8 NOT NULL,
    verify_keep_rate      float8 NOT NULL,
    rollback_mttr_p50_sec float8 NOT NULL,
    rollback_mttr_p95_sec float8 NOT NULL,
    ts_ms                 bigint NOT NULL,
    created_at            timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_aslo_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results (
    id         bigserial PRIMARY KEY,
    apply_id   varchar(255) NOT NULL,
    attempt    int NOT NULL,
    status     varchar(50) NOT NULL,
    ts_ms      bigint NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_art_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations (
    id           bigserial PRIMARY KEY,
    severity     varchar(50)              NOT NULL,
    message      text,
    summary_json jsonb,
    ts_ms        bigint                   NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_wa_aes_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations(ts_ms DESC);

COMMIT;
