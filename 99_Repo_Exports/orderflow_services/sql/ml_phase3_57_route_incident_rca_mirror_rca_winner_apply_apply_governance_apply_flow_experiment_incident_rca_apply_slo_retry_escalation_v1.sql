CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    window_min INTEGER NOT NULL,
    verification_n INTEGER NOT NULL,
    verified_n INTEGER NOT NULL,
    rollback_planned_n INTEGER NOT NULL,
    rollback_applied_n INTEGER NOT NULL,
    retry_n INTEGER NOT NULL,
    escalation_n INTEGER NOT NULL,
    verify_keep_rate DOUBLE PRECISION NOT NULL,
    rollback_plan_rate DOUBLE PRECISION NOT NULL,
    rollback_applied_rate DOUBLE PRECISION NOT NULL,
    rollback_mttr_p95_sec DOUBLE PRECISION NOT NULL,
    retry_rate DOUBLE PRECISION NOT NULL,
    escalation_rate DOUBLE PRECISION NOT NULL,
    mttr_slo_sec DOUBLE PRECISION NOT NULL,
    rollup_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups',
    'ts',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_slo_rollups_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups (ts DESC);


CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    source_rollback_ts_ms BIGINT NOT NULL,
    source_verification_ts_ms BIGINT NOT NULL,
    rollback_mode TEXT NOT NULL,
    failed_target_mode TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    applied INTEGER NOT NULL,
    result_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms, rollback_mode, failed_target_mode, reason_code)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results',
    'ts',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_retry_results_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results (ts DESC);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_retry_results_reason
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results (reason_code, rollback_mode, failed_target_mode);


CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    source_rollback_ts_ms BIGINT NOT NULL,
    source_verification_ts_ms BIGINT NOT NULL,
    rollback_mode TEXT NOT NULL,
    failed_target_mode TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    escalation_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms, rollback_mode, failed_target_mode, reason_code)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations',
    'ts',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_escalations_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations (ts DESC);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_escalations_reason
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations (reason_code, severity);
