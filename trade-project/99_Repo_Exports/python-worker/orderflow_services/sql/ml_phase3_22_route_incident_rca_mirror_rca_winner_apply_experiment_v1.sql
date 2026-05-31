BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_experiment_decisions (
    id           bigserial PRIMARY KEY,
    bundle_id    varchar(255) NOT NULL,
    mode         varchar(50) NOT NULL,
    exposures    jsonb NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_exp_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_experiment_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_experiment_exposures (
    id              bigserial PRIMARY KEY,
    bundle_id       varchar(255) NOT NULL,
    arm             varchar(100) NOT NULL,
    exposure_type   varchar(50) NOT NULL,
    severity        varchar(50) NOT NULL,
    experiment_mode varchar(50) NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_exp_exposures_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_experiment_exposures(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_exp_exposures_arm
    ON llm_route_incident_rca_mirror_rca_winner_apply_experiment_exposures(arm);

COMMIT;
