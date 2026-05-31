BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_results (
    output_hash      text PRIMARY KEY,
    route_change_id  text NOT NULL,
    task_type        text NOT NULL,
    compact_hash     text NOT NULL,
    prompt_version   text NOT NULL,
    policy_version   text NOT NULL,
    provider         text NOT NULL,
    model_name       text NOT NULL,
    result_json      jsonb NOT NULL,
    quality_score    numeric(4,3) DEFAULT NULL,
    usefulness_score numeric(4,3) DEFAULT NULL,
    ts_ms            bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_results_route_change_id
    ON llm_operator_routing_incident_rca_results(route_change_id);

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_quality (
    id                   bigserial PRIMARY KEY,
    output_hash          text references llm_operator_routing_incident_rca_results(output_hash),
    quality_score        numeric(4,3) NOT NULL,
    quality_reasons_json jsonb NOT NULL,
    ts_ms                bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_quality_output_hash
    ON llm_operator_routing_incident_rca_quality(output_hash);

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_feedback (
    id          bigserial PRIMARY KEY,
    output_hash text references llm_operator_routing_incident_rca_results(output_hash),
    operator_id text NOT NULL,
    usefulness  text NOT NULL,
    score       numeric(4,3) NOT NULL,
    comments    text,
    ts_ms       bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_feedback_output_hash
    ON llm_operator_routing_incident_rca_feedback(output_hash);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_feedback_ts_ms
    ON llm_operator_routing_incident_rca_feedback(ts_ms DESC);

COMMIT;
