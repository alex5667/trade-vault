-- =============================================================================
-- Migration 052: Phase 8 — Horizon ATR Gate (hz_gate) observability columns.
-- 
-- Adds hz_gate_* columns to signal_decisions for canary/enforce routing audit.
-- All columns are nullable with fail-open defaults.
-- 
-- Run:  psql -d trade_db -f migrations/052_hz_gate_phase8.sql
-- Roll: DROP the columns (safe, no FK deps).
-- =============================================================================

-- Phase 8 hz_gate observability fields
ALTER TABLE signal_decisions
    ADD COLUMN IF NOT EXISTS hz_gate_mode      TEXT    DEFAULT 'SHADOW',
    ADD COLUMN IF NOT EXISTS hz_gate_active    SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hz_gate_share     NUMERIC(6,4) DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS hz_gate_in_canary SMALLINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hz_gate_veto      SMALLINT DEFAULT 0;

-- Phase 6 horizon-aware ML features (for per-signal attribution analysis)
ALTER TABLE signal_decisions
    ADD COLUMN IF NOT EXISTS hz_atr_tf_ms            BIGINT  DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hz_atr_stop_pct         NUMERIC(10,4) DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS hz_hold_target_ms_norm  NUMERIC(10,6) DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS hz_vol_ratio_fast_slow  NUMERIC(10,4) DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS hz_max_signal_age_ratio NUMERIC(10,4) DEFAULT 0.0;

-- Partial index for canary analysis: only rows where gate was active
CREATE INDEX IF NOT EXISTS idx_signal_decisions_hz_active
    ON signal_decisions (ts_ms, symbol)
    WHERE hz_gate_active = 1;

COMMENT ON COLUMN signal_decisions.hz_gate_mode IS 'Phase 8: Horizon ATR gate mode: SHADOW|CANARY|ENFORCE';
COMMENT ON COLUMN signal_decisions.hz_gate_active IS 'Phase 8: 1 if this signal was in canary/enforce routing';
COMMENT ON COLUMN signal_decisions.hz_gate_share IS 'Phase 8: Canary share active at time of signal (0..1)';
COMMENT ON COLUMN signal_decisions.hz_gate_in_canary IS 'Phase 8: 1 if sticky hash routed this signal to canary bucket';
COMMENT ON COLUMN signal_decisions.hz_gate_veto IS 'Phase 8: 1 if hz_gate blocked the signal (ENFORCE only)';
COMMENT ON COLUMN signal_decisions.hz_atr_tf_ms IS 'Phase 6: ATR timeframe selected by ATRTFSelector (ms)';
COMMENT ON COLUMN signal_decisions.hz_atr_stop_pct IS 'Phase 6: ATR / entry_price * 100 (%)';
COMMENT ON COLUMN signal_decisions.hz_hold_target_ms_norm IS 'Phase 6: hold_target_ms / 3600000 (fraction of hour)';
COMMENT ON COLUMN signal_decisions.hz_vol_ratio_fast_slow IS 'Phase 6: fast_vol / slow_vol from horizon_contract';
COMMENT ON COLUMN signal_decisions.hz_max_signal_age_ratio IS 'Phase 6: (now_ms - ts_ms) / max_signal_age_ms';

-- Analytics view: canary vs baseline comparison by symbol/session
CREATE OR REPLACE VIEW v_hz_gate_canary_delta AS
SELECT
    date_trunc('hour', to_timestamp(ts_ms / 1000.0)) AS ts_hour,
    symbol,
    hz_gate_mode,
    hz_gate_active,
    COUNT(*) AS signal_count,
    AVG(hz_atr_stop_pct) AS avg_atr_stop_pct,
    AVG(hz_hold_target_ms_norm) AS avg_hold_target_norm,
    AVG(hz_vol_ratio_fast_slow) AS avg_vol_ratio,
    AVG(hz_max_signal_age_ratio) AS avg_age_ratio,
    SUM(hz_gate_veto) AS veto_count
FROM signal_decisions
WHERE ts_ms > EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') * 1000
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, signal_count DESC;

COMMENT ON VIEW v_hz_gate_canary_delta IS
    'Phase 8: Canary vs baseline delta for hz_gate routing audit. 7-day rolling window.';
