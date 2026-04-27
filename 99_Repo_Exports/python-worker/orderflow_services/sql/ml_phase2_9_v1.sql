BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_rca_routing_incident_rca_results (
    id               bigserial PRIMARY KEY,
    route_change_id  text NOT NULL,
    ts_ms            bigint NOT NULL,
    provider         text NOT NULL,
    model_name       text NOT NULL,
    prompt_version   text NOT NULL,
    policy_version   text NOT NULL,
    result_json      jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_incident_rca_results_route_change_id
    ON llm_operator_rca_routing_incident_rca_results(route_change_id);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_incident_rca_results_ts_ms
    ON llm_operator_rca_routing_incident_rca_results(ts_ms DESC);

COMMIT;
