BEGIN;

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_governor_decisions (
    id            bigserial PRIMARY KEY,
    scope_type    text NOT NULL,  -- 'action' or 'provider'
    scope_key     text NOT NULL,
    action        text NOT NULL,  -- 'SUPPRESS', 'PROMOTE', 'HOLD'
    score         numeric(4,3) NOT NULL,
    sample_n      int NOT NULL,
    advisory_only boolean NOT NULL,
    ts_ms         bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_gov_dec_ts_ms
    ON llm_operator_routing_incident_rca_governor_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_operator_routing_incident_rca_governor_policy_versions (
    policy_version text PRIMARY KEY,
    description    text NOT NULL,
    is_active      boolean NOT NULL DEFAULT true,
    created_ms     bigint NOT NULL,
    updated_ms     bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_routing_incident_rca_gov_pol_is_active
    ON llm_operator_routing_incident_rca_governor_policy_versions(is_active);

COMMIT;
