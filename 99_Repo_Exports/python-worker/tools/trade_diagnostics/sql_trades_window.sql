SELECT
    exit_ts_ms,
    order_id,
    sid,
    symbol,
    strategy,
    source,
    pnl_net,
    pnl_gross,
    fees,
    entry_price as entry_px,
    exit_price as exit_px,
    lot,
    notional_usd,
    direction,
    entry_tag,
    mfe_pnl,
    mae_pnl,
    giveback,
    missed_profit,
    health_avg_l2_age_ms,
    health_l2_stale_ratio_now,
    health_l2_stale_ratio_tick,
    close_reason
FROM trades_closed
WHERE exit_ts_ms >= :from_ms AND exit_ts_ms < :to_ms
ORDER BY exit_ts_ms DESC
