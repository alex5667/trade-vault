CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_runs (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    run_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    mode TEXT NOT NULL,
    start_id TEXT NOT NULL,
    end_id TEXT NOT NULL,
    last_stream_id TEXT NOT NULL,
    scanned_n INTEGER NOT NULL,
    written_n INTEGER NOT NULL,
    dlq_n INTEGER NOT NULL,
    duration_ms BIGINT NOT NULL,
    run_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms, run_id, alias)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_runs',
    'ts',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_reports (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    alias TEXT NOT NULL,
    window_min INTEGER NOT NULL,
    window_start_ts_ms BIGINT NOT NULL,
    window_end_ts_ms BIGINT NOT NULL,
    redis_n INTEGER NOT NULL,
    pg_n INTEGER NOT NULL,
    gap_n INTEGER NOT NULL,
    sample_n INTEGER NOT NULL,
    missing_sample_n INTEGER NOT NULL,
    status TEXT NOT NULL,
    report_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms, alias)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_reports',
    'ts',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_2_backfill_runs_alias_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_runs (alias, ts DESC);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_2_reconcile_reports_alias_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_reconcile_reports (alias, ts DESC);
