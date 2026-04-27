-- 1) Полная история анализов (heavy JSON)
CREATE TABLE IF NOT EXISTS news_analysis (
  uid           TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  ts_ms         BIGINT NOT NULL,
  source        TEXT NOT NULL,
  risk          DOUBLE PRECISION NOT NULL,
  surprise      DOUBLE PRECISION NOT NULL,
  tags_mask     BIGINT NOT NULL,
  primary_tag   INTEGER NOT NULL,
  payload_json  JSONB NOT NULL,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(uid, symbol)
);

CREATE INDEX IF NOT EXISTS news_analysis_ts_idx ON news_analysis (ts_ms DESC);
CREATE INDEX IF NOT EXISTS news_analysis_symbol_ts_idx ON news_analysis (symbol, ts_ms DESC);

-- 2) Снимки online агрегатов (для backtest)
CREATE TABLE IF NOT EXISTS news_features_symbol (
  symbol      TEXT NOT NULL,
  ts_ms       BIGINT NOT NULL,
  risk        DOUBLE PRECISION NOT NULL,
  surprise    DOUBLE PRECISION NOT NULL,
  tags_mask   BIGINT NOT NULL,
  primary_tag INTEGER NOT NULL,
  ref         TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol, ts_ms)
);

CREATE INDEX IF NOT EXISTS news_features_symbol_ts_idx ON news_features_symbol (ts_ms DESC);

-- Если у вас TimescaleDB:
-- SELECT create_hypertable('news_analysis','ts_ms', if_not_exists => TRUE);
-- SELECT create_hypertable('news_features_symbol','ts_ms', if_not_exists => TRUE);
