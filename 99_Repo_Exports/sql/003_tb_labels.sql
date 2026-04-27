-- Migration 003: Triple-Barrier Labels Table (v10.1)
-- Purpose: Long-term storage for triple-barrier labels computed from tick streams
-- Retention: Long-term (no automatic retention policy, manual cleanup if needed)

CREATE TABLE IF NOT EXISTS tb_labels (
  sid            TEXT PRIMARY KEY,
  symbol         TEXT NOT NULL,
  ts_ms          BIGINT NOT NULL,
  direction      TEXT NOT NULL,

  primary_h_ms   INTEGER NOT NULL,
  primary_label  TEXT NOT NULL,         -- TP|SL|TIMEOUT|NO_TICKS
  primary_hit_ms BIGINT NOT NULL,
  primary_ret_bps DOUBLE PRECISION NOT NULL,
  primary_r_mult  DOUBLE PRECISION NOT NULL,
  primary_y_edge  INTEGER NOT NULL,

  horizons_json  JSONB NOT NULL,        -- {"60000": {...}, "180000": {...}, ...}
  ticks_sample   JSONB,                 -- optional: [[ts,price], ...] sampled
  meta           JSONB,                 -- costs/spread/slip/exec_risk_bps, etc.

  created_ms     BIGINT NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS tb_labels_symbol_ts_idx ON tb_labels(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS tb_labels_ts_ms_idx ON tb_labels(ts_ms DESC);
CREATE INDEX IF NOT EXISTS tb_labels_direction_idx ON tb_labels(direction, ts_ms DESC);
CREATE INDEX IF NOT EXISTS tb_labels_primary_label_idx ON tb_labels(primary_label, ts_ms DESC);

-- JSONB index for flexible queries on horizons_json and meta
CREATE INDEX IF NOT EXISTS tb_labels_horizons_gin_idx ON tb_labels USING gin (horizons_json);
CREATE INDEX IF NOT EXISTS tb_labels_meta_gin_idx ON tb_labels USING gin (meta);

-- Comments for documentation
COMMENT ON TABLE tb_labels IS 'Triple-barrier labels computed from tick streams (v10.1)';
COMMENT ON COLUMN tb_labels.sid IS 'Signal ID from signals:of:inputs';
COMMENT ON COLUMN tb_labels.primary_h_ms IS 'Primary horizon in milliseconds (default: 180000 = 180s)';
COMMENT ON COLUMN tb_labels.primary_label IS 'Primary label: TP (take profit), SL (stop loss), TIMEOUT, or NO_TICKS';
COMMENT ON COLUMN tb_labels.primary_ret_bps IS 'Primary return in basis points (signed: positive for TP, negative for SL)';
COMMENT ON COLUMN tb_labels.primary_r_mult IS 'Primary return in R-multiples (ret_bps / scale_bps)';
COMMENT ON COLUMN tb_labels.primary_y_edge IS 'Binary edge label: 1 if TP_HIT, 0 otherwise';
COMMENT ON COLUMN tb_labels.horizons_json IS 'Multi-horizon labels: {"60000": {...}, "180000": {...}, "300000": {...}}';
COMMENT ON COLUMN tb_labels.ticks_sample IS 'Sampled tick path: [[ts,price], ...] for debugging/visualization';
COMMENT ON COLUMN tb_labels.meta IS 'Metadata: exec_cost_r, util_r, tp_bps, sl_bps, scale_bps';

