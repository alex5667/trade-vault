CREATE TABLE IF NOT EXISTS llm_route_incident_rca_apply_replay_dashboard_snapshots (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    window_start_ts_ms BIGINT NOT NULL,
    window_end_ts_ms BIGINT NOT NULL,
    gate_decision TEXT NOT NULL,
    aliases_ok INTEGER NOT NULL,
    aliases_required INTEGER NOT NULL,
    snapshot_status TEXT NOT NULL,
    snapshot_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms)
);

SELECT create_hypertable(
    'llm_route_incident_rca_apply_replay_dashboard_snapshots',
    'ts_ms',
    chunk_time_interval => 86400000,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_4_1_replay_gate_decisions_ts_desc
ON llm_route_incident_rca_apply_replay_gate_decisions (ts DESC);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_4_1_replay_dashboard_snapshots_ts_desc
ON llm_route_incident_rca_apply_replay_dashboard_snapshots (ts DESC);
