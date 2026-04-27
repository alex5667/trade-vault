-- Migration: signal_confidence_scores (TimescaleDB)
-- Purpose: archive high-frequency scores from Redis stream `signals:confidence:scores`

CREATE TABLE IF NOT EXISTS signal_confidence_scores (
  stream_id TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  sid TEXT NOT NULL,
  symbol TEXT NOT NULL,
  schema_version INT NOT NULL,
  producer TEXT NOT NULL,
  confidence_raw DOUBLE PRECISION NOT NULL,
  confidence_final DOUBLE PRECISION,
  evidence_json JSONB NOT NULL,
  context_json JSONB,
  PRIMARY KEY (stream_id, ts)
);

-- Timescale (safe if extension installed). If extension is not installed, run this manually after enabling it.
SELECT create_hypertable('signal_confidence_scores', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS signal_confidence_scores_symbol_ts_idx
  ON signal_confidence_scores (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS signal_confidence_scores_sid_ts_idx
  ON signal_confidence_scores (sid, ts DESC);
