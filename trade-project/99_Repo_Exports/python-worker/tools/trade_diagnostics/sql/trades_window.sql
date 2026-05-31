-- trades_window.sql for schema 006 (trades_closed hypertable: exit_ts)
-- Required output aliases for report script:
-- order_id, sid, source, symbol, direction, entry_tag,
-- pnl_net, pnl_gross, fees, fees_bps,
-- entry_px, exit_px, lot, notional_usd,
-- entry_ts_ms, exit_ts_ms, duration_ms,
-- close_reason, baseline_exit_reason,
-- mfe_pnl, mae_pnl, giveback, missed_profit,
-- health_avg_l2_age_ms, health_l2_stale_ratio_now, health_l2_stale_ratio_tick

SELECT
  tc.order_id::text                              AS order_id,
  COALESCE(tc.sid,'')::text                      AS sid,
  COALESCE(tc.source,'')::text                   AS source,
  tc.symbol::text                                AS symbol,
  COALESCE(tc.direction,'')::text                AS direction,
  COALESCE(tc.entry_tag,'')::text                AS entry_tag,

  tc.pnl_net                                     AS pnl_net,
  tc.pnl_gross                                   AS pnl_gross,
  tc.fees                                        AS fees,

  tc.entry_price                                 AS entry_px,
  tc.exit_price                                  AS exit_px,
  tc.lot                                         AS lot,
  COALESCE(tc.notional_usd, tc.lot * tc.entry_price) AS notional_usd,

  tc.entry_ts_ms                                 AS entry_ts_ms,
  tc.exit_ts_ms                                  AS exit_ts_ms,
  COALESCE(tc.duration_ms, tc.exit_ts_ms - tc.entry_ts_ms) AS duration_ms,

  COALESCE(tc.close_reason,'')::text             AS close_reason,
  COALESCE(tc.baseline_exit_reason,'')::text     AS baseline_exit_reason,

  COALESCE(tc.mfe_pnl, 0.0)                      AS mfe_pnl,
  COALESCE(tc.mae_pnl, 0.0)                      AS mae_pnl,
  COALESCE(tc.giveback, 0.0)                     AS giveback,
  COALESCE(tc.missed_profit, 0.0)                AS missed_profit,

  COALESCE(tc.health_avg_l2_age_ms, 0.0)         AS health_avg_l2_age_ms,
  COALESCE(tc.health_l2_stale_ratio_now, 0.0)    AS health_l2_stale_ratio_now,
  COALESCE(tc.health_l2_stale_ratio_tick, 0.0)   AS health_l2_stale_ratio_tick,

  -- P0 Columns (Joined)
  p0.scenario::text                              AS scenario,
  p0.regime::text                                AS regime,
  p0.session::text                               AS session,
  p0.entry_reason::text                          AS entry_reason,
  COALESCE(p0.mae_bps, 0.0)                      AS mae_bps,
  COALESCE(p0.mfe_bps, 0.0)                      AS mfe_bps,
  COALESCE(p0.time_to_mfe_ms, 0)                 AS time_to_mfe_ms,
  COALESCE(p0.hold_ms, 0)                        AS hold_ms,
  COALESCE(p0.spread_bps_at_entry, 0.0)          AS spread_bps_at_entry,
  COALESCE(p0.slippage_bps_est, 0.0)             AS slippage_bps_est,
  COALESCE(p0.book_age_ms, 0)                    AS book_age_ms,
  COALESCE(p0.features_json, '{}'::jsonb)        AS features

FROM trades_closed tc
LEFT JOIN trades_closed_p0 p0
  ON p0.order_id = tc.order_id AND p0.exit_ts_ms = tc.exit_ts_ms
WHERE tc.exit_ts >= to_timestamp(%(from_ms)s / 1000.0)
  AND tc.exit_ts <  to_timestamp(%(to_ms)s   / 1000.0)
ORDER BY tc.exit_ts DESC;
