-- Migration: trade_profile_decisions
-- Stores per-signal profile routing audit for offline analysis and auto-promotion.
-- Created: 2026-05-10
--
-- Apply with:
--   psql $DATABASE_URL -f 20260510_50_trade_profile_decisions.sql

BEGIN;

CREATE TABLE IF NOT EXISTS trade_profile_decisions (
    ts                          TIMESTAMPTZ         NOT NULL,
    signal_id                   TEXT                NOT NULL,
    symbol                      TEXT                NOT NULL,
    regime                      TEXT                NOT NULL,
    regime_bucket               TEXT                NOT NULL,    -- trend|range|thin|mixed
    kind                        TEXT                NOT NULL,
    side                        TEXT                NOT NULL,
    profile                     TEXT                NOT NULL,
    decision                    TEXT                NOT NULL,    -- ALLOW|DENY|SHADOW
    reason_code                 TEXT                NOT NULL,

    -- Edge / confidence
    p_edge                      DOUBLE PRECISION,
    confidence                  DOUBLE PRECISION,
    ev_bps                      DOUBLE PRECISION,
    cost_bps                    DOUBLE PRECISION,
    net_edge_bps                DOUBLE PRECISION,               -- ev_bps - cost_bps
    spread_bps                  DOUBLE PRECISION,
    slippage_bps                DOUBLE PRECISION,
    max_expected_slippage_bps   DOUBLE PRECISION,

    -- Trade parameters
    stop_atr_mult               DOUBLE PRECISION,
    tp_rr                       TEXT,
    trailing_profile            TEXT,
    execution_policy            TEXT,
    risk_multiplier             DOUBLE PRECISION,
    is_canary                   BOOLEAN             DEFAULT FALSE,
    profile_mode                TEXT,               -- LIVE|SHADOW

    -- Model / schema
    model_ver                   TEXT,
    schema_ver                  TEXT,
    features_json               JSONB,

    created_at                  TIMESTAMPTZ         DEFAULT now()
);

-- Hypertable (skip if already exists)
SELECT create_hypertable(
    'trade_profile_decisions',
    'ts',
    if_not_exists => TRUE
);

-- Primary lookup: symbol × regime × kind × profile, newest first
CREATE INDEX IF NOT EXISTS idx_tpd_srk
    ON trade_profile_decisions (symbol, regime_bucket, kind, profile, ts DESC);

-- Signal lookup for TCA join
CREATE INDEX IF NOT EXISTS idx_tpd_signal
    ON trade_profile_decisions (signal_id);

-- Profile-level aggregation for auto-promotion
CREATE INDEX IF NOT EXISTS idx_tpd_profile_ts
    ON trade_profile_decisions (profile, ts DESC);

-- Canary isolation
CREATE INDEX IF NOT EXISTS idx_tpd_canary
    ON trade_profile_decisions (is_canary, profile, ts DESC)
    WHERE is_canary = TRUE;

-- Retention: keep 90 days of audit rows
SELECT add_retention_policy(
    'trade_profile_decisions',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

COMMIT;
