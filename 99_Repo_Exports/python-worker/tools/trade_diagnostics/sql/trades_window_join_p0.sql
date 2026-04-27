SELECT
  tc.order_id,
  tc.source, tc.symbol, tc.direction, tc.entry_tag,
  tc.entry_ts_ms, tc.exit_ts_ms, tc.entry_price, tc.exit_price,
  tc.lot,
  COALESCE(tc.notional_usd, tc.lot * tc.entry_price) AS notional_usd,
  tc.pnl_net, tc.pnl_gross, tc.fees,
  COALESCE(tc.duration_ms, tc.exit_ts_ms - tc.entry_ts_ms) AS duration_ms,
  COALESCE(tc.close_reason,'')::text AS close_reason,
  COALESCE(tc.baseline_exit_reason,'')::text AS baseline_exit_reason,
  COALESCE(tc.mfe_pnl, 0.0) AS mfe_pnl,
  COALESCE(tc.mae_pnl, 0.0) AS mae_pnl,
  COALESCE(tc.giveback, 0.0) AS giveback,
  COALESCE(tc.missed_profit, 0.0) AS missed_profit,
  COALESCE(tc.health_avg_l2_age_ms, 0.0) AS health_avg_l2_age_ms,
  COALESCE(tc.health_l2_stale_ratio_now, 0.0) AS health_l2_stale_ratio_now,
  COALESCE(tc.health_l2_stale_ratio_tick, 0.0) AS health_l2_stale_ratio_tick,

  p0.scenario, p0.regime, p0.session, p0.entry_reason,
  p0.mae_bps, p0.mfe_bps, p0.time_to_mfe_ms, p0.hold_ms,
  p0.spread_bps_at_entry, p0.slippage_bps_est, p0.book_age_ms,
  p0.features_json

FROM trades_closed tc
LEFT JOIN trades_closed_p0 p0
  ON p0.order_id = tc.order_id AND p0.exit_ts_ms = tc.exit_ts_ms
WHERE tc.exit_ts >= to_timestamp(%(from_ms)s / 1000.0)
  AND tc.exit_ts <  to_timestamp(%(to_ms)s   / 1000.0)
ORDER BY tc.exit_ts DESC;
