ALTER TABLE IF EXISTS llm_incident_rca_runs
    ADD COLUMN IF NOT EXISTS routed_provider text,
    ADD COLUMN IF NOT EXISTS routed_model_name text,
    ADD COLUMN IF NOT EXISTS routed_prompt_version text,
    ADD COLUMN IF NOT EXISTS routing_apply_decision text,
    ADD COLUMN IF NOT EXISTS routing_apply_mode text,
    ADD COLUMN IF NOT EXISTS routing_apply_ts_ms bigint;

CREATE TABLE IF NOT EXISTS llm_operator_rca_routing_apply_decisions (
    id bigserial PRIMARY KEY,
    ts_ms bigint NOT NULL,
    experiment_id text NOT NULL,
    decision text NOT NULL,
    mode text NOT NULL,
    provider text,
    model_name text,
    prompt_version text,
    policy_version text,
    sample_n integer,
    uplift double precision,
    confidence double precision,
    reason_codes_json jsonb,
    proposed_update_json jsonb
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_apply_decisions_ts
    ON llm_operator_rca_routing_apply_decisions (ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_routing_apply_decisions_decision
    ON llm_operator_rca_routing_apply_decisions (decision, mode);
