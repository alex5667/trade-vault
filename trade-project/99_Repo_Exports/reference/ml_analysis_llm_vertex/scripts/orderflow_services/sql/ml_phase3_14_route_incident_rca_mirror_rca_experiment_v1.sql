BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_experiment_exposures (
    id              bigserial PRIMARY KEY,
    bundle_id       uuid NOT NULL,
    arm             text NOT NULL,
    mode            text NOT NULL,
    severity        text NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_experiment_exposures_ts
    ON llm_route_incident_rca_mirror_rca_experiment_exposures(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_experiment_exposures_bundle
    ON llm_route_incident_rca_mirror_rca_experiment_exposures(bundle_id);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_experiment_decisions (
    id              bigserial PRIMARY KEY,
    bundle_id       uuid NOT NULL,
    arms            jsonb NOT NULL,
    ts_ms           bigint NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_experiment_decisions_ts
    ON llm_route_incident_rca_mirror_rca_experiment_decisions(ts_ms DESC);

COMMIT;
