BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_routing_decisions (
    id               bigserial PRIMARY KEY,
    route_change_id  text NOT NULL,
    task_type        text NOT NULL,
    provider         text NOT NULL,
    model_name       text NOT NULL,
    prompt_version   text NOT NULL,
    policy_version   text NOT NULL,
    routing_reason   text NOT NULL,
    mode             text NOT NULL, -- DRY_RUN, SHADOW, ENFORCE
    ts_ms            bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_rout_dec_ts_ms ON llm_operator_routing_incident_rca_routing_decisions(ts_ms DESC);

COMMIT;
