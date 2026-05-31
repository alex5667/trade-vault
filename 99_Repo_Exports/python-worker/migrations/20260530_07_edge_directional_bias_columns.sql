-- Added 2026-05-30: EdgeCostGate directional p_min bias provenance columns.
-- Closes the autocal feedback loop: edge_directional_bias_autocal_v1 reads
-- `edge_directional_bias_value` off trades:closed to split baseline (=0)
-- from applied (>0) buckets per (direction × regime). Without these the
-- phase ladder cannot advance past OBSERVE.
--
-- Sources (priority): EdgeCostGate._stamp_bias_on_ctx → ctx.indicators →
-- signal_payload.indicators → domain.handlers.finalize_trade transfer →
-- TradeClosed attrs → services.analytics_db._edge_directional_bias_*_db().
ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS edge_directional_bias_value         DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS edge_directional_bias_countertrend  BOOLEAN          NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS edge_directional_bias_source        TEXT             NOT NULL DEFAULT 'none';

-- Index for autocal's per-direction analytics (it groups by direction × regime
-- via entry_regime/market_regime fallback; the bias value itself is the split).
CREATE INDEX IF NOT EXISTS idx_trades_closed_edb_bias
    ON trades_closed(exit_ts DESC, direction, entry_regime)
    INCLUDE (edge_directional_bias_value, edge_directional_bias_countertrend, r_multiple)
    WHERE edge_directional_bias_source <> 'none';
