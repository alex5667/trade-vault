ALTER TABLE IF EXISTS llm_incident_rca_results
    ADD COLUMN IF NOT EXISTS governor_last_decision text,
    ADD COLUMN IF NOT EXISTS governor_last_decision_ts_ms bigint,
    ADD COLUMN IF NOT EXISTS governor_pattern_key text;

CREATE TABLE IF NOT EXISTS llm_operator_rca_governor_decisions (
    id bigserial PRIMARY KEY,
    ts_ms bigint NOT NULL,
    scope text NOT NULL,
    action_type text,
    provider text,
    model_name text,
    prompt_version text,
    policy_version text,
    sample_n integer NOT NULL,
    combined_score double precision NOT NULL,
    quality_avg double precision,
    useful_rate double precision NOT NULL,
    decision text NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    advisory_only boolean NOT NULL DEFAULT true,
    policy_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_governor_decisions_ts
    ON llm_operator_rca_governor_decisions (ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_governor_decisions_scope
    ON llm_operator_rca_governor_decisions (scope, decision, ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_operator_rca_governor_policy_versions (
    id bigserial PRIMARY KEY,
    ts_ms bigint NOT NULL,
    scope text NOT NULL,
    policy_key text NOT NULL,
    prompt_version text,
    policy_version text,
    effective_mode text NOT NULL,
    decision text NOT NULL,
    payload_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(policy_key, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_governor_policy_versions_ts
    ON llm_operator_rca_governor_policy_versions (ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_operator_rca_governor_feedback_rollups (
    id bigserial PRIMARY KEY,
    bucket_ts_ms bigint NOT NULL,
    scope text NOT NULL,
    action_type text,
    provider text,
    model_name text,
    prompt_version text,
    policy_version text,
    sample_n integer NOT NULL,
    avg_quality_score double precision NOT NULL,
    avg_usefulness_score double precision NOT NULL,
    useful_rate double precision NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_operator_rca_governor_feedback_rollups_bucket
    ON llm_operator_rca_governor_feedback_rollups (bucket_ts_ms DESC);
