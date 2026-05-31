BEGIN;

CREATE TABLE IF NOT EXISTS llm_rca_governance_apply_flow_exp_exposures (
    id                 bigserial PRIMARY KEY,
    bundle_id          text NOT NULL,
    ts_ms              bigint NOT NULL,
    arm                text NOT NULL,
    severity           text NOT NULL,
    destination_stream text NOT NULL,
    exposure_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_governance_apply_flow_exp_exposures_ts
    ON llm_rca_governance_apply_flow_exp_exposures(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_rca_governance_apply_flow_exp_decisions (
    id                 bigserial PRIMARY KEY,
    bundle_id          text NOT NULL,
    ts_ms              bigint NOT NULL,
    severity           text NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    arm                text NOT NULL,
    destination_stream text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_rca_governance_apply_flow_exp_decisions_ts
    ON llm_rca_governance_apply_flow_exp_decisions(ts_ms DESC);

COMMIT;
