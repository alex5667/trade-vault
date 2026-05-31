BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_incident_bundles (
    id             bigserial PRIMARY KEY,
    bundle_id      uuid NOT NULL,
    ts_ms          bigint NOT NULL,
    trigger_source text NOT NULL,
    trigger_type   text NOT NULL,
    severity       text NOT NULL,
    bundle_json    jsonb NOT NULL,
    created_at     timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_incident_bundles_ts
    ON llm_route_incident_rca_mirror_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_incident_bundles_id
    ON llm_route_incident_rca_mirror_incident_bundles(bundle_id);

COMMIT;
