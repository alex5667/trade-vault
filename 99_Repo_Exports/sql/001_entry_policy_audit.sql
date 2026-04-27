-- Migration 001: Entry Policy Audit Table
-- Purpose: Long-term storage for stream:trade:entry_audit events
-- Retention: 90 days via TimescaleDB retention policy

CREATE TABLE IF NOT EXISTS entry_policy_audit (
    stream_id        TEXT PRIMARY KEY,
    ts_ms            BIGINT NOT NULL,
    ts               TIMESTAMPTZ NOT NULL,

    sid              TEXT,
    symbol           TEXT,
    tf               TEXT,
    strategy         TEXT,
    source           TEXT,

    decision         TEXT NOT NULL,          -- ALLOW / ALLOW_SHADOW / DENY / FROZEN_HARD / etc
    arm              TEXT,
    ab_group         TEXT,
    scenario         TEXT,
    regime           TEXT,

    of_confirm_score DOUBLE PRECISION,
    coh              DOUBLE PRECISION,
    leader_conf      DOUBLE PRECISION,

    spread_z         DOUBLE PRECISION,
    pressure_sps     DOUBLE PRECISION,
    obi_age_ms       BIGINT,

    payload_json     JSONB NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS entry_policy_audit_ts_idx ON entry_policy_audit (ts DESC);
CREATE INDEX IF NOT EXISTS entry_policy_audit_symbol_ts_idx ON entry_policy_audit (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS entry_policy_audit_decision_ts_idx ON entry_policy_audit (decision, ts DESC);
CREATE INDEX IF NOT EXISTS entry_policy_audit_arm_ts_idx ON entry_policy_audit (arm, ts DESC) WHERE arm IS NOT NULL;
CREATE INDEX IF NOT EXISTS entry_policy_audit_regime_scenario_idx ON entry_policy_audit (regime, scenario, ts DESC);

-- JSONB index for flexible queries on payload
CREATE INDEX IF NOT EXISTS entry_policy_audit_payload_gin_idx ON entry_policy_audit USING gin (payload_json);

-- Comments for documentation
COMMENT ON TABLE entry_policy_audit IS 'Long-term archive of entry policy audit events from Redis stream:trade:entry_audit';
COMMENT ON COLUMN entry_policy_audit.stream_id IS 'Redis stream message ID (format: <ts_ms>-<seq>), ensures idempotency';
COMMENT ON COLUMN entry_policy_audit.decision IS 'Policy decision: ALLOW, ALLOW_SHADOW, DENY, FROZEN_HARD, etc';
COMMENT ON COLUMN entry_policy_audit.arm IS 'A/B/C test arm';
COMMENT ON COLUMN entry_policy_audit.ab_group IS 'A/B test group (default/thin)';
COMMENT ON COLUMN entry_policy_audit.payload_json IS 'Full event payload for audit and replay';

-- TimescaleDB hypertable conversion (if TimescaleDB extension is available)
-- Run this separately if you have TimescaleDB:
SELECT create_hypertable('entry_policy_audit', 'ts', 
    chunk_time_interval => interval '1 day',
    if_not_exists => TRUE
);

-- TimescaleDB retention policy (90 days)
-- Run this after creating hypertable:
SELECT add_retention_policy('entry_policy_audit', INTERVAL '90 days', if_not_exists => TRUE);

