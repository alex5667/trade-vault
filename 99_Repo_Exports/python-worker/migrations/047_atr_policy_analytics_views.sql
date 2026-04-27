-- 047_atr_policy_analytics_views.sql

-- Add columns if they do not exist
ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS atr_policy_ver integer,
  ADD COLUMN IF NOT EXISTS atr_policy_tag text,
  ADD COLUMN IF NOT EXISTS atr_policy_source text,
  ADD COLUMN IF NOT EXISTS atr_policy_scenario text,
  ADD COLUMN IF NOT EXISTS atr_policy_regime text,
  ADD COLUMN IF NOT EXISTS atr_policy_bucket text,
  ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode text,
  ADD COLUMN IF NOT EXISTS atr_trailing_mode text,
  ADD COLUMN IF NOT EXISTS atr_recovery_run_id text,
  ADD COLUMN IF NOT EXISTS atr_restore_cert_id text,
  ADD COLUMN IF NOT EXISTS atr_restore_cert_status text,
  ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json jsonb;

-- A. Базовый attribution view по policy
CREATE OR REPLACE VIEW v_atr_policy_trade_attribution AS
SELECT
    t.symbol,
    CASE WHEN t.is_virtual THEN 'virtual' ELSE 'live' END AS kind,
    t.source,
    t.atr_policy_ver,
    t.atr_policy_tag,
    t.atr_policy_source,
    t.atr_policy_scenario,
    t.atr_policy_regime,
    t.atr_policy_bucket,
    t.atr_stop_ttl_mode,
    t.atr_trailing_mode,
    t.atr_recovery_run_id,
    t.atr_restore_cert_status,
    count(*) AS n_trades,
    avg(t.pnl_pct * 10000) AS avg_pnl_bps,
    avg(p0.slippage_bps_est) AS avg_slippage_bps,
    avg(t.mae_pnl) AS avg_mae_pnl,
    avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
    avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END) AS stop_rate,
    avg(CASE WHEN t.close_reason = 'tp1_hit' THEN 1.0 ELSE 0.0 END) AS tp1_rate
FROM trades_closed t
LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
GROUP BY
    t.symbol, t.is_virtual, t.source,
    t.atr_policy_ver, t.atr_policy_tag, t.atr_policy_source,
    t.atr_policy_scenario, t.atr_policy_regime, t.atr_policy_bucket,
    t.atr_stop_ttl_mode, t.atr_trailing_mode,
    t.atr_recovery_run_id, t.atr_restore_cert_status;

-- B. Recovery / certification view
CREATE OR REPLACE VIEW v_atr_policy_recovery_impact AS
SELECT
    t.atr_recovery_run_id,
    t.atr_restore_cert_status,
    t.symbol,
    t.atr_policy_ver,
    count(*) AS n_trades,
    avg(t.pnl_pct * 10000) AS avg_pnl_bps,
    avg(p0.slippage_bps_est) AS avg_slippage_bps,
    avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END) AS stop_rate,
    avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
FROM trades_closed t
LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
GROUP BY
    t.atr_recovery_run_id,
    t.atr_restore_cert_status,
    t.symbol,
    t.atr_policy_ver;

-- C. Cohort compare view для policy modes
CREATE OR REPLACE VIEW v_atr_policy_mode_cohorts AS
SELECT
    t.symbol,
    t.atr_policy_scenario,
    t.atr_policy_regime,
    t.atr_policy_bucket,
    t.atr_stop_ttl_mode,
    t.atr_trailing_mode,
    count(*) AS n_trades,
    avg(t.pnl_pct * 10000) AS avg_pnl_bps,
    avg(p0.slippage_bps_est) AS avg_slippage_bps,
    avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
    avg(CASE WHEN t.close_reason = 'tp1_hit' THEN 1.0 ELSE 0.0 END) AS tp1_rate,
    avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END) AS stop_rate
FROM trades_closed t
LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
GROUP BY
    t.symbol,
    t.atr_policy_scenario,
    t.atr_policy_regime,
    t.atr_policy_bucket,
    t.atr_stop_ttl_mode,
    t.atr_trailing_mode;
