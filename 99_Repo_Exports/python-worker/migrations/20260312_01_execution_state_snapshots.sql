-- execution_state_snapshots: durable SQL fallback for replay rehydration.
-- Used by execution_state_replay._load_sql_state_snapshot() when the Redis
-- stream does not contain enough history for a given SID.
-- Migration: 20260312_01_execution_state_snapshots.sql

CREATE TABLE IF NOT EXISTS execution_state_snapshots (
    id            BIGSERIAL       PRIMARY KEY,
    sid           TEXT            NOT NULL,
    snapshot      TEXT            NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_state_snapshots_sid_created
    ON execution_state_snapshots (sid, created_at DESC);

-- Grant read access to the trading user (used by replay service for SELECT)
GRANT SELECT, INSERT ON execution_state_snapshots TO trading;
GRANT USAGE, SELECT ON SEQUENCE execution_state_snapshots_id_seq TO trading;
