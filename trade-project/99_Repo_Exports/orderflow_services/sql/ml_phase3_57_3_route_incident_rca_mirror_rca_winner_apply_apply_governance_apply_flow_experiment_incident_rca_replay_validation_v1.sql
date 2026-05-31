CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    alias TEXT NOT NULL,
    window_start_ts_ms BIGINT NOT NULL,
    window_end_ts_ms BIGINT NOT NULL,
    stream_row_count INTEGER NOT NULL,
    pg_row_count INTEGER NOT NULL,
    key_coverage_ratio DOUBLE PRECISION NOT NULL,
    missing_in_pg_n INTEGER NOT NULL,
    extra_in_pg_n INTEGER NOT NULL,
    stream_subset_hash TEXT NOT NULL,
    pg_subset_hash TEXT NOT NULL,
    hash_match INTEGER NOT NULL,
    status TEXT NOT NULL,
    report_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms, alias)
);

SELECT create_hypertable(
    'llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports',
    'ts',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_3_replay_validation_alias_ts_desc
ON llm_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports (alias, ts DESC);
