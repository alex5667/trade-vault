ALTER TABLE llm_incident_rca_results
    ADD COLUMN IF NOT EXISTS routed_provider text,
    ADD COLUMN IF NOT EXISTS routed_model_name text,
    ADD COLUMN IF NOT EXISTS routed_prompt_version text,
    ADD COLUMN IF NOT EXISTS routed_policy_version text;

CREATE TABLE IF NOT EXISTS llm_operator_rca_routing_decisions (
    decision_id              text PRIMARY KEY,
    ts_ms                    bigint NOT NULL,
    provider                 text NOT NULL,
    model_name               text NOT NULL,
    prompt_version           text NOT NULL,
    policy_version           text NOT NULL,
    mode                     text NOT NULL,
    changed                  boolean NOT NULL DEFAULT false,
    audit_json               jsonb NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS llm_operator_rca_routing_audit (
    audit_id                 text PRIMARY KEY,
    ts_ms                    bigint NOT NULL,
    event                    text NOT NULL,
    provider                 text,
    model_name               text,
    prompt_version           text,
    policy_version           text,
    mode                     text,
    payload_json             jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_decisions_ts
    ON llm_operator_rca_routing_decisions (ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_audit_ts
    ON llm_operator_rca_routing_audit (ts_ms DESC);
