-- 4.1 SQL: table + indices + Timescale (D1)
CREATE TABLE IF NOT EXISTS edge_gate_events (
  id                BIGSERIAL, 

  -- Identity
  signal_id          TEXT NOT NULL,
  symbol             TEXT NOT NULL,
  gate_name          TEXT NOT NULL DEFAULT 'edge_cost',
  gate_version       INT  NOT NULL DEFAULT 2,
  stage              TEXT NOT NULL DEFAULT 'pre_emit',

  -- Time
  ts_ms              BIGINT NOT NULL,
  ts                 TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,

  -- Decision
  passed             BOOLEAN NOT NULL,
  veto_code          TEXT NULL,
  edge_source        TEXT NOT NULL DEFAULT 'none',

  -- BPS metrics
  exp_bps            DOUBLE PRECISION NOT NULL,
  req_bps            DOUBLE PRECISION NOT NULL,
  margin_bps         DOUBLE PRECISION NOT NULL,
  edge_ratio         DOUBLE PRECISION NOT NULL,

  k                  DOUBLE PRECISION NOT NULL,
  fees_bps           DOUBLE PRECISION NOT NULL,
  slip_bps           DOUBLE PRECISION NOT NULL,
  buf_bps            DOUBLE PRECISION NOT NULL,
  total_costs_bps    DOUBLE PRECISION NOT NULL,

  -- Optional debug/context (keep small!)
  ctx                JSONB NULL,

  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency: 1 event per (signal_id, gate_name, stage, gate_version)
CREATE UNIQUE INDEX IF NOT EXISTS ux_edge_gate_events_dedupe
ON edge_gate_events (signal_id, gate_name, stage, gate_version, ts_ms);

-- Query patterns
CREATE INDEX IF NOT EXISTS ix_edge_gate_events_symbol_ts
ON edge_gate_events (symbol, ts_ms DESC);

CREATE INDEX IF NOT EXISTS ix_edge_gate_events_passed_ts
ON edge_gate_events (passed, ts_ms DESC);

-- For large volumes: cheap time index
CREATE INDEX IF NOT EXISTS brin_edge_gate_events_ts
ON edge_gate_events USING BRIN (ts_ms);

-- Timescale (Enforced)
SELECT create_hypertable('edge_gate_events', 'ts_ms', chunk_time_interval => 86400000, if_not_exists => TRUE);
