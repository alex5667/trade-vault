-- Migration: 054_create_tca_and_ml_tables.sql
-- Description: Create trade_decisions_tca and ml_predictions tables for durable TCA and ML state persistence

CREATE TABLE IF NOT EXISTS trade_decisions_tca (
    decision_ts_ms BIGINT NOT NULL,
    sid VARCHAR(100) NOT NULL,
    signal_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    decision_mid DOUBLE PRECISION,
    decision_bid DOUBLE PRECISION,
    decision_ask DOUBLE PRECISION,
    decision_spread_bps DOUBLE PRECISION,
    decision_expected_slippage_bps DOUBLE PRECISION,
    decision_exec_risk_norm DOUBLE PRECISION,
    book_sanity_flags JSONB,
    tca_ready BOOLEAN DEFAULT FALSE,
    payload_jsonb JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (decision_ts_ms, sid)
);

CREATE INDEX IF NOT EXISTS idx_trade_decisions_tca_symbol ON trade_decisions_tca(symbol, decision_ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_trade_decisions_tca_signal_id ON trade_decisions_tca(signal_id);

CREATE TABLE IF NOT EXISTS ml_predictions (
    ts_ms BIGINT NOT NULL,
    sid VARCHAR(100) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    model_ver VARCHAR(50),
    mode VARCHAR(50),
    p_edge DOUBLE PRECISION,
    p_min DOUBLE PRECISION,
    p_margin DOUBLE PRECISION,
    allow BOOLEAN,
    bucket VARCHAR(50),
    missing BOOLEAN,
    latency_us BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (ts_ms, sid)
);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_symbol ON ml_predictions(symbol, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_sid ON ml_predictions(sid);
