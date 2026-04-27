-- 048_atr_policy_promotion_v2.sql

CREATE OR REPLACE VIEW v_atr_policy_promotion_inputs AS
SELECT
  t.symbol,
  t.atr_policy_scenario AS scenario,
  t.atr_policy_regime AS regime,
  t.atr_policy_bucket AS bucket,
  t.atr_policy_ver,
  t.atr_restore_cert_status,
  t.atr_stop_ttl_mode,
  t.atr_trailing_mode,
  avg(t.pnl_pct * 10000) AS avg_pnl_bps,
  avg(p0.slippage_bps_est) AS avg_slippage_bps,
  avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END) AS stop_rate,
  avg(CASE WHEN t.close_reason = 'tp1_hit' THEN 1.0 ELSE 0.0 END) AS tp1_rate,
  avg(CASE WHEN t.pnl_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
  avg(t.mae_pnl) AS avg_mae_pct,
  count(*) AS n_trades
FROM trades_closed t
LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
WHERE t.exit_ts >= now() - interval '14 days'
GROUP BY
  t.symbol,
  t.atr_policy_scenario,
  t.atr_policy_regime,
  t.atr_policy_bucket,
  t.atr_policy_ver,
  t.atr_restore_cert_status,
  t.atr_stop_ttl_mode,
  t.atr_trailing_mode;
