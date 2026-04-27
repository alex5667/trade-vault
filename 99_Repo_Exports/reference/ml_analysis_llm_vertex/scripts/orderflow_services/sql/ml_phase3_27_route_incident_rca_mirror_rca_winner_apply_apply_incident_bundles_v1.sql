BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles (
    id           bigserial PRIMARY KEY,
    bundle_id    varchar(255) NOT NULL,
    trigger_type varchar(100) NOT NULL,
    severity     varchar(50) NOT NULL,
    payload_json jsonb NOT NULL,
    ts_ms        bigint NOT NULL,
    created_at   timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_w_a_a_ib_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_w_a_a_ib_bundle
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles(bundle_id);

COMMIT;
