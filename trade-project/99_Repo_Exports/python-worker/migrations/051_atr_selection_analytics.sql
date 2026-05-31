-- 051_atr_selection_analytics.sql
-- Phase 5: ATR selection metadata for post-trade analytics

ALTER TABLE trades_closed 
  ADD COLUMN IF NOT EXISTS atr_sel_tf text DEFAULT '',
  ADD COLUMN IF NOT EXISTS atr_sel_src text DEFAULT '',
  ADD COLUMN IF NOT EXISTS atr_sel_age_ms bigint DEFAULT 0;

-- Analytics view for ATR selection quality
-- Correlates selected ATR with realized MAE (Max Adverse Excursion)
CREATE OR REPLACE VIEW v_atr_selection_quality AS
SELECT 
    symbol,
    atr_sel_src,
    atr_sel_tf,
    count(*) as trade_count,
    avg(atr) as avg_atr_value,
    avg(mae_bps) as avg_mae_bps,
    avg(mae_pnl / NULLIF(risk_usd, 0)) as avg_mae_r,
    -- Normalized MAE relative to ATR. 
    -- If MAE significantly exceeds ATR, the ATR might be too tight or lagged.
    avg(mae_bps / NULLIF(atr / entry_px * 10000, 0)) as mae_to_atr_ratio,
    avg(atr_sel_age_ms) as avg_data_age_ms
FROM trades_closed
WHERE atr > 0 AND entry_px > 0
GROUP BY symbol, atr_sel_src, atr_sel_tf;
