CREATE TABLE IF NOT EXISTS llm_route_incident_rca_apply_replay_gate_decisions (
    ts_ms BIGINT NOT NULL,
    ts TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    window_start_ts_ms BIGINT NOT NULL,
    window_end_ts_ms BIGINT NOT NULL,
    aliases_ok INTEGER NOT NULL,
    aliases_required INTEGER NOT NULL,
    decision TEXT NOT NULL,
    gate_reasons JSONB NOT NULL,
    decision_json JSONB NOT NULL,
    PRIMARY KEY (ts_ms)
);

SELECT create_hypertable(
    'llm_route_incident_rca_apply_replay_gate_decisions',
    'ts_ms',
    chunk_time_interval => 86400000,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_llm_phase3_57_4_replay_gate_ts_desc
ON llm_route_incident_rca_apply_replay_gate_decisions (ts DESC);
