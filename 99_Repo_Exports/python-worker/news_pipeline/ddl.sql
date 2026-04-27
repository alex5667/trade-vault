-- DDL for news pipeline PostgreSQL tables
-- news_analysis: raw analysis results per uid/symbol
CREATE TABLE IF NOT EXISTS news_analysis (
  uid           TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  ts_ms         BIGINT NOT NULL,
  source        TEXT NOT NULL,
  risk          DOUBLE PRECISION NOT NULL,
  surprise      DOUBLE PRECISION NOT NULL,
  confidence    DOUBLE PRECISION NOT NULL,
  tags_mask     BIGINT NOT NULL,
  primary_tag   INTEGER NOT NULL,
  payload_json  JSONB NOT NULL,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(uid, symbol)
);

CREATE INDEX IF NOT EXISTS news_analysis_ts_idx ON news_analysis (ts_ms DESC);
CREATE INDEX IF NOT EXISTS news_analysis_symbol_ts_idx ON news_analysis (symbol, ts_ms DESC);

-- news_features_symbol: online aggregates per symbol
CREATE TABLE IF NOT EXISTS news_features_symbol (
  symbol      TEXT NOT NULL,
  ts_ms       BIGINT NOT NULL,
  risk        DOUBLE PRECISION NOT NULL,
  surprise    DOUBLE PRECISION NOT NULL,
  tags_mask   BIGINT NOT NULL,
  primary_tag INTEGER NOT NULL,
  ref         TEXT NOT NULL,
  confidence  DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  grade_id    INTEGER NOT NULL DEFAULT 0,
  horizon_sec INTEGER NOT NULL DEFAULT 0,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol, ts_ms)
);

CREATE INDEX IF NOT EXISTS news_features_symbol_ts_idx ON news_features_symbol (ts_ms DESC);
CREATE INDEX IF NOT EXISTS news_features_symbol_symbol_ts_idx ON news_features_symbol (symbol, ts_ms DESC);
