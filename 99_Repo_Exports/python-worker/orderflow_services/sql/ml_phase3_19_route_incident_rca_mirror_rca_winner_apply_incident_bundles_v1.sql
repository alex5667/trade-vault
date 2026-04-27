BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles (
    bundle_id    varchar(255) PRIMARY KEY,
    trigger_type varchar(100) NOT NULL,
    severity     varchar(50) NOT NULL,
    bundle_json  jsonb NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles_sev
    ON llm_route_incident_rca_mirror_rca_winner_apply_incident_bundles(severity);

COMMIT;
