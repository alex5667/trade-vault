BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    trigger_type        text NOT NULL,
    trigger_reason_code text NOT NULL,
    trigger_severity    text NOT NULL,
    source_stream       text NOT NULL,
    bundle_json         jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles(ts_ms DESC);

COMMIT;
