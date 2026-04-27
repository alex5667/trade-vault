-- Migration 002: Position Events Table
-- Purpose: Long-term storage for intermediate position events (TP_HIT, TRAILING_MOVE, SL_ADJUST)
-- Retention: 90 days via TimescaleDB retention policy

CREATE TABLE IF NOT EXISTS position_events (
    stream_id    TEXT,
    ts_ms        BIGINT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,

    order_id     TEXT,
    sid          TEXT,
    symbol       TEXT,

    event_type   TEXT NOT NULL,   -- TP_HIT / TRAILING_MOVE / SL_ADJUST / etc
    payload_json JSONB NOT NULL,

    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, ts)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS position_events_ts_idx ON position_events (ts DESC);
CREATE INDEX IF NOT EXISTS position_events_order_ts_idx ON position_events (order_id, ts DESC) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS position_events_type_ts_idx ON position_events (event_type, ts DESC);
CREATE INDEX IF NOT EXISTS position_events_symbol_ts_idx ON position_events (symbol, ts DESC) WHERE symbol IS NOT NULL;
CREATE INDEX IF NOT EXISTS position_events_sid_ts_idx ON position_events (sid, ts DESC) WHERE sid IS NOT NULL;

-- JSONB index for flexible queries on payload
CREATE INDEX IF NOT EXISTS position_events_payload_gin_idx ON position_events USING gin (payload_json);

-- Comments for documentation
COMMENT ON TABLE position_events IS 'Long-term archive of intermediate position events from Redis events:trades stream';
COMMENT ON COLUMN position_events.stream_id IS 'Redis stream message ID (format: <ts_ms>-<seq>), ensures idempotency';
COMMENT ON COLUMN position_events.event_type IS 'Event type: TP_HIT, TRAILING_MOVE, SL_ADJUST, etc';
COMMENT ON COLUMN position_events.order_id IS 'Position order ID for grouping events';
COMMENT ON COLUMN position_events.payload_json IS 'Full event payload for audit and replay';

-- TimescaleDB hypertable conversion (if TimescaleDB extension is available)
-- Run this separately if you have TimescaleDB:
SELECT create_hypertable('position_events', 'ts',
    chunk_time_interval => interval '1 day',
    if_not_exists => TRUE
);

-- TimescaleDB retention policy (90 days)
-- Run this after creating hypertable:
SELECT add_retention_policy('position_events', INTERVAL '90 days', if_not_exists => TRUE);

