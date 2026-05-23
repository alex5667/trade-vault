-- Migration: create trade_performance table
-- Used by ml-scorer-pipeline ETL (build_ml_dataset_from_scratch.py, train_ml_scorer.py)
-- Added: 2026-05-22

CREATE TABLE IF NOT EXISTS trade_performance (
    ts_open                 TIMESTAMPTZ      NOT NULL,
    ts_close                TIMESTAMPTZ      NOT NULL,
    signal_id               TEXT             NOT NULL,
    symbol                  TEXT             NOT NULL,
    direction               SMALLINT         NOT NULL,
    r                       DOUBLE PRECISION NOT NULL,
    hit                     BOOLEAN          NOT NULL,
    holding_ms              BIGINT           NULL,
    slippage_bps            DOUBLE PRECISION DEFAULT 0.0,
    adverse_bps             DOUBLE PRECISION DEFAULT 0.0,
    close_reason_raw        TEXT,
    close_reason_bucket     TEXT,
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (signal_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_performance_signal_id
    ON trade_performance (signal_id);

CREATE INDEX IF NOT EXISTS idx_trade_performance_symbol_ts
    ON trade_performance (symbol, ts_open DESC);

CREATE INDEX IF NOT EXISTS idx_trade_performance_hit_ts
    ON trade_performance (hit, ts_open DESC);

CREATE INDEX IF NOT EXISTS idx_trade_performance_r_ts
    ON trade_performance (r, ts_open DESC);
