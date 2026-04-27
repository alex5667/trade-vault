--
-- Design notes:
-- - Time column is epoch-ms BIGINT to stay consistent with the system-wide time contract.
-- - Idempotency: UNIQUE(sid, ts_decision_ms) so repeated writes are safe (at-least-once stream delivery).
-- - Hypertable: partition by ts_decision_ms; tune chunk interval via DECISION_SNAPSHOT_CHUNK_MS if desired.
--
-- Requirements:
--   CREATE EXTENSION IF NOT EXISTS timescaledb;
--
-- Apply:
--   psql "$TIMESCALE_DSN" -f decision_snapshot_timescale.sql
--

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

  extra                         JSONB   NULL,

  UNIQUE (sid, ts_decision_ms)
);

-- Create hypertable if TimescaleDB is available.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable('decision_snapshot', 'ts_decision_ms', if_not_exists => TRUE);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_sid ON decision_snapshot (sid);
CREATE INDEX IF NOT EXISTS idx_decision_snapshot_symbol_ts ON decision_snapshot (symbol, ts_decision_ms DESC);
