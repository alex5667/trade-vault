BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_decisions (
    id                 bigserial PRIMARY KEY,
    bundle_id          text NOT NULL,
    ts_ms              bigint NOT NULL,
    trigger_type       text NOT NULL,
    severity           text NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    route              text NOT NULL,
    destination_stream text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_decisions(ts_ms DESC);

COMMIT;
