BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_rca_routing_incident_bundles (
    bundle_id                    text PRIMARY KEY,
    route_change_id              text NOT NULL,
    built_ts_ms                  bigint NOT NULL,
    severity                     text NOT NULL,
    bundle_hash                  text NOT NULL,
    primary_reason_codes_json    jsonb NOT NULL,
    summary_json                 jsonb NOT NULL,
    bundle_json                  jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_incident_bundles_route_change_id
    ON llm_operator_rca_routing_incident_bundles(route_change_id);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_incident_bundles_built_ts
    ON llm_operator_rca_routing_incident_bundles(built_ts_ms DESC);

COMMIT;
