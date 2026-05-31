-- 20260516_01_liquidation_events_raw.sql
--
-- Raw liquidation event archive for replay + train/serve parity audit.
-- Source: stream:liq_evt (Go liquidation controller → Binance/Bybit/...).
-- Writer:  python-worker/orderflow_services/liq_evt_archiver_v1.py
-- Consumer group: liq_archive_group (separate from liqmap_group to avoid PEL collision).
--
-- Schema notes
-- ─────────────
-- * ts_event_ms is the venue's reported event timestamp (epoch ms).
-- * ts_ingest_ms is the worker's recv/ingest timestamp.
-- * (venue, symbol, ts_event_ms, event_id) provides deterministic dedup —
--   event_id is constructed by Go as "{venue}:{symbol}:{ts_event_ms}".
-- * `redis_msg_id` carries the source stream entry id for replay/forensics.
-- * `payload_json` keeps the full normalized payload (forward-compat for fields
--   added later without schema migration churn).

CREATE TABLE IF NOT EXISTS liquidation_events_raw (
    ts_event_ms          BIGINT NOT NULL,
    ts_event             TIMESTAMPTZ NOT NULL,
    ts_ingest_ms         BIGINT NOT NULL,

    venue                TEXT NOT NULL,
    symbol               TEXT NOT NULL,

    liq_side             TEXT NOT NULL,     -- "long" / "short"
    order_side           TEXT,              -- raw exchange-side ("BUY"/"SELL"/"Buy"/"Sell")

    price                DOUBLE PRECISION NOT NULL,
    qty                  DOUBLE PRECISION NOT NULL,
    notional_usd         DOUBLE PRECISION NOT NULL,

    event_id             TEXT NOT NULL,     -- {venue}:{symbol}:{ts_event_ms}
    trace_id             TEXT,
    quality_flags        TEXT,
    schema_version       SMALLINT DEFAULT 1,

    redis_msg_id         TEXT,              -- source stream entry id for replay
    payload_json         JSONB,             -- full normalized payload

    created_at           TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (ts_event, venue, symbol, event_id)
);

-- Convert to hypertable: 1-day chunks (typical Timescale pattern for medium-rate streams).
SELECT create_hypertable(
    'liquidation_events_raw',
    'ts_event',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for typical replay queries.
CREATE INDEX IF NOT EXISTS idx_liq_raw_symbol_ts
    ON liquidation_events_raw (symbol, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_liq_raw_venue_ts
    ON liquidation_events_raw (venue, ts_event DESC);
CREATE INDEX IF NOT EXISTS idx_liq_raw_liqside_ts
    ON liquidation_events_raw (liq_side, ts_event DESC);

-- Retention: 90 days raw events (configurable via ALTER). Most replay/train use
-- cases stay within 30–60d; 90d gives buffer for ad-hoc post-mortems.
SELECT add_retention_policy(
    'liquidation_events_raw',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- Compression: chunks older than 7 days → segmentby symbol (high cardinality
-- per chunk, decent compression on price/qty/notional within symbol).
ALTER TABLE liquidation_events_raw SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, venue',
    timescaledb.compress_orderby = 'ts_event DESC'
);

SELECT add_compression_policy(
    'liquidation_events_raw',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

COMMENT ON TABLE liquidation_events_raw IS
    'Raw liquidation events from stream:liq_evt (Binance/Bybit/...). 90d retention, 7d compression. '
    'PK enforces dedup via (venue,symbol,ts_event_ms,event_id). Hypertable chunks by 1 day on ts_event.';
