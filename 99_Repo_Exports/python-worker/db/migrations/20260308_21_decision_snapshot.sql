-- Migration: 20260308_21_decision_snapshot
-- Creates the decision_snapshot table (TimescaleDB hypertable if available).
-- Matches the schema in python-worker/services/posttrade/sql/decision_snapshot_timescale.sql
--
-- Design notes:
-- - Time column is epoch-ms BIGINT to match the system-wide time contract.
-- - UNIQUE(sid, ts_decision_ms) ensures idempotent at-least-once upserts.
-- - Converts to a TimescaleDB hypertable if the extension is loaded.

CREATE TABLE IF NOT EXISTS decision_snapshot (
  ts_decision_ms                BIGINT NOT NULL,
  sid                           TEXT   NOT NULL,
  symbol                        TEXT   NOT NULL,
  venue                         TEXT   NOT NULL DEFAULT 'binance',
  session                       TEXT   NOT NULL DEFAULT '',
  tf                            TEXT   NOT NULL DEFAULT '',
  kind                          TEXT   NOT NULL DEFAULT '',
  side                          TEXT   NOT NULL DEFAULT '',
  direction                     TEXT   NOT NULL DEFAULT '',

  decision_bid                  DOUBLE PRECISION NULL,
  decision_ask                  DOUBLE PRECISION NULL,
  decision_mid                  DOUBLE PRECISION NULL,
  decision_spread_bps           DOUBLE PRECISION NULL,

  decision_depth_bid_5          DOUBLE PRECISION NULL,
  decision_depth_ask_5          DOUBLE PRECISION NULL,
  decision_depth_bid_20         DOUBLE PRECISION NULL,
  decision_depth_ask_20         DOUBLE PRECISION NULL,

  decision_book_slope_bid       DOUBLE PRECISION NULL,
  decision_book_slope_ask       DOUBLE PRECISION NULL,
  decision_dws_bps              DOUBLE PRECISION NULL,

  decision_ofi_norm             DOUBLE PRECISION NULL,
  decision_expected_slippage_bps DOUBLE PRECISION NULL,
  decision_exec_risk_norm       DOUBLE PRECISION NULL,

  decision_price                DOUBLE PRECISION NULL,

  tca_ready                     BOOLEAN NOT NULL DEFAULT FALSE,
  book_sanity_flags             TEXT[]  NOT NULL DEFAULT ARRAY[]::TEXT[],

  schema_version                INTEGER NOT NULL DEFAULT 1,
  producer                      TEXT    NOT NULL DEFAULT '',
  ts_insert_ms                  BIGINT  NOT NULL DEFAULT 0,
  is_virtual                    BOOLEAN NOT NULL DEFAULT FALSE,

  extra                         JSONB   NULL,

  UNIQUE (sid, ts_decision_ms)
);

-- Promote to a TimescaleDB hypertable if the extension is present.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable(
      'decision_snapshot', 'ts_decision_ms',
      if_not_exists => TRUE
    );
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_sid
  ON decision_snapshot (sid);

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_symbol_ts
  ON decision_snapshot (symbol, ts_decision_ms DESC);
