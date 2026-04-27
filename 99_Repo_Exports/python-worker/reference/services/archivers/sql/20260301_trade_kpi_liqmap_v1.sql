-- H2: trade_kpi_liqmap_v1
-- Purpose: persist liqmap-related post-trade KPIs (from trades:post_sl) for reporting.
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS trade_kpi_liqmap_v1 (
  stream_id TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  trade_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  regime TEXT NOT NULL,

  -- Convenience columns (nullable; populated only when corresponding KPI exists).
  sl_hit_near_liqmap_peak SMALLINT,
  tp1_anchored SMALLINT,
  tp1_anchored_and_hit SMALLINT,
  sl_liqmap_peak_dist_bps DOUBLE PRECISION,
  sl_liqmap_peak_usd DOUBLE PRECISION,

  -- Compact KPI subset and full raw payload.
  liqmap_kpi JSONB NOT NULL,
  payload_json JSONB NOT NULL,

  PRIMARY KEY (stream_id, ts)
);

-- If TimescaleDB is present, convert to hypertable (no-op otherwise in auto-migrate).
-- SELECT create_hypertable('trade_kpi_liqmap_v1','ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_symbol_ts_idx
  ON trade_kpi_liqmap_v1 (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_trade_id_ts_idx
  ON trade_kpi_liqmap_v1 (trade_id, ts DESC);

-- Optional: JSON queries acceleration.
CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_liqmap_kpi_gin
  ON trade_kpi_liqmap_v1 USING GIN (liqmap_kpi jsonb_path_ops);
-- H2: trade_kpi_liqmap_v1
-- Purpose: persist liqmap-related post-trade KPIs (from trades:post_sl) for reporting.
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS trade_kpi_liqmap_v1 (
  stream_id TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  trade_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  regime TEXT NOT NULL,

  -- Convenience columns (nullable; populated only when corresponding KPI exists).
  sl_hit_near_liqmap_peak SMALLINT,
  tp1_anchored SMALLINT,
  tp1_anchored_and_hit SMALLINT,
  sl_liqmap_peak_dist_bps DOUBLE PRECISION,
  sl_liqmap_peak_usd DOUBLE PRECISION,

  -- Compact KPI subset and full raw payload.
  liqmap_kpi JSONB NOT NULL,
  payload_json JSONB NOT NULL,

  PRIMARY KEY (stream_id, ts)
);

-- If TimescaleDB is present, convert to hypertable (no-op otherwise in auto-migrate).
-- SELECT create_hypertable('trade_kpi_liqmap_v1','ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_symbol_ts_idx
  ON trade_kpi_liqmap_v1 (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_trade_id_ts_idx
  ON trade_kpi_liqmap_v1 (trade_id, ts DESC);

-- Optional: JSON queries acceleration.
CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_liqmap_kpi_gin
  ON trade_kpi_liqmap_v1 USING GIN (liqmap_kpi jsonb_path_ops);
