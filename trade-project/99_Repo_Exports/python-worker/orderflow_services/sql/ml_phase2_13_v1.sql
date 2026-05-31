BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_exposures (
    id              bigserial PRIMARY KEY,
    route_change_id text NOT NULL,
    experiment_id   text NOT NULL,
    bucket          text NOT NULL, -- control, challenger
    base_provider   text NOT NULL,
    base_model      text NOT NULL,
    ts_ms           bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_exposures_route ON llm_operator_routing_incident_rca_exposures(route_change_id);
CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_exposures_exp ON llm_operator_routing_incident_rca_exposures(experiment_id, ts_ms DESC);

COMMIT;
