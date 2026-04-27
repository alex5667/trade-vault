-- Phase B (P1) — TCA storage schema for Timescale/Postgres
--
-- Tables:
--   - bbo_ts           : BBO time-series snapshots (for mid_{t+Δ})
--   - fills            : normalized fills (entry/exit) joinable by sid
--   - tca_fill_metrics : per-fill TCA metrics (effective/realized spread, impact, IS)
--
-- Conventions:
--   - event-time columns are epoch-ms BIGINT for deterministic joins
--   - a `ts TIMESTAMPTZ` companion is stored for Timescale hypertables
--
-- NOTE: decision_snapshot table is created by decision_snapshot_timescale.sql (A3).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- bbo_ts: best bid/ask + mid at time ts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bbo_ts (
  ts          TIMESTAMPTZ NOT NULL,
  ts_ms       BIGINT NOT NULL,
  sym         TEXT NOT NULL,
  venue       TEXT NOT NULL,
  bid         DOUBLE PRECISION NOT NULL,
  ask         DOUBLE PRECISION NOT NULL,
  mid         DOUBLE PRECISION NOT NULL,
  producer    TEXT NOT NULL DEFAULT '',
  schema_version INT NOT NULL DEFAULT 1,
  stream_id   TEXT NULL,
  PRIMARY KEY (sym, venue, ts_ms, ts)
);

SELECT create_hypertable('bbo_ts', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS bbo_ts_sym_venue_ts_idx
  ON bbo_ts (sym, venue, ts DESC);

-- Compression: keep recent uncompressed for fast writes
ALTER TABLE bbo_ts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sym,venue'
);

-- Optional policy (safe if Timescale installed). Adjust via migrations.
DO $$
BEGIN
  PERFORM add_compression_policy('bbo_ts', INTERVAL '3 days');
EXCEPTION WHEN OTHERS THEN
  -- Fail-open: policy may already exist or Timescale not enabled.
END $$;

-- ---------------------------------------------------------------------
-- fills: normalized execution fills (entry/exit)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fills (
  ts          TIMESTAMPTZ NOT NULL,
  ts_fill_ms  BIGINT NOT NULL,
  sid         TEXT NOT NULL,
  order_id    TEXT NOT NULL,
  sym         TEXT NOT NULL,
  venue       TEXT NOT NULL,
  side        TEXT NOT NULL,  -- LONG|SHORT
  fill_role   TEXT NOT NULL,  -- entry|exit
  px          DOUBLE PRECISION NOT NULL,
  qty         DOUBLE PRECISION NOT NULL,
  fee_bps     DOUBLE PRECISION NOT NULL,

  bid_at_fill DOUBLE PRECISION NULL,
  ask_at_fill DOUBLE PRECISION NULL,
  mid_at_fill DOUBLE PRECISION NULL,

  event_type  TEXT NOT NULL DEFAULT '',
  event_id    TEXT NULL,
  stream_id   TEXT NULL,
  ts_insert_ms BIGINT NOT NULL DEFAULT 0,

  PRIMARY KEY (sid, ts_fill_ms, fill_role, ts)
);

SELECT create_hypertable('fills', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS fills_sym_venue_ts_idx
  ON fills (sym, venue, ts DESC);
CREATE INDEX IF NOT EXISTS fills_sid_ts_idx
  ON fills (sid, ts DESC);

-- ---------------------------------------------------------------------
-- tca_fill_metrics: per-fill TCA metrics
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tca_fill_metrics (
  ts            TIMESTAMPTZ NOT NULL,
  ts_fill_ms    BIGINT NOT NULL,
  sid           TEXT NOT NULL,
  sym           TEXT NOT NULL,
  venue         TEXT NOT NULL,
  side          TEXT NOT NULL,
  fill_role     TEXT NOT NULL,

  -- decision context (from decision_snapshot)
  decision_ts_ms BIGINT NOT NULL,
  session       TEXT NOT NULL,
  tf            TEXT NOT NULL,
  kind          TEXT NOT NULL,
  decision_mid  DOUBLE PRECISION NULL,

  -- mid/bbo at fill (from bbo_ts join)
  mid_t         DOUBLE PRECISION NULL,
  bid_t         DOUBLE PRECISION NULL,
  ask_t         DOUBLE PRECISION NULL,

  -- mid at t+Δ
  mid_t_1s      DOUBLE PRECISION NULL,
  mid_t_5s      DOUBLE PRECISION NULL,

  -- core metrics (bps)
  eff_spread_bps DOUBLE PRECISION NULL,
  realized_spread_1s_bps DOUBLE PRECISION NULL,
  realized_spread_5s_bps DOUBLE PRECISION NULL,
  perm_impact_1s_bps DOUBLE PRECISION NULL,
  perm_impact_5s_bps DOUBLE PRECISION NULL,
  is_bps        DOUBLE PRECISION NULL,

  -- raw execution
  px            DOUBLE PRECISION NOT NULL,
  qty           DOUBLE PRECISION NOT NULL,
  fee_bps       DOUBLE PRECISION NOT NULL,

  ts_insert_ms  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (sid, ts_fill_ms, fill_role, ts)
);

SELECT create_hypertable('tca_fill_metrics', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS tca_fill_metrics_sym_venue_ts_idx
  ON tca_fill_metrics (sym, venue, ts DESC);
CREATE INDEX IF NOT EXISTS tca_fill_metrics_sid_ts_idx
  ON tca_fill_metrics (sid, ts DESC);

-- Optional CAGG example (1m rollups) — keep commented until you confirm volume.
-- CREATE MATERIALIZED VIEW IF NOT EXISTS tca_rollups_1m
-- WITH (timescaledb.continuous) AS
-- SELECT
--   time_bucket('1 minute', ts) AS bucket,
--   sym, venue, session, tf, kind, side,
--   percentile_cont(0.50) WITHIN GROUP (ORDER BY is_bps) AS is_p50_bps,
--   percentile_cont(0.95) WITHIN GROUP (ORDER BY is_bps) AS is_p95_bps,
--   percentile_cont(0.99) WITHIN GROUP (ORDER BY is_bps) AS is_p99_bps
-- FROM tca_fill_metrics
-- WHERE is_bps IS NOT NULL
-- GROUP BY bucket, sym, venue, session, tf, kind, side;
