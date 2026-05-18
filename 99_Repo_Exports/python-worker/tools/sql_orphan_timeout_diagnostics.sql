-- Diagnostic SQL for orphan timeout and time-exit issues (2026-05-17)
-- Use with TimescaleDB / PostgreSQL

-- ============================================================================
-- 1. Baseline: All timeout/orphan exits by session and day-of-week
-- ============================================================================
SELECT
    COALESCE(session, 'na') AS session,
    EXTRACT(ISODOW FROM to_timestamp(exit_ts_ms / 1000.0))::int AS dow,
    COALESCE(close_reason_raw, close_reason, 'UNKNOWN') AS reason,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE pnl_net < 0)::float / COUNT(*) AS loss_rate,
    COUNT(*) FILTER (WHERE pnl_net < 0) AS loss_count,
    AVG(pnl_net) AS avg_pnl_net,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pnl_net) AS median_pnl,
    AVG(hold_ms) AS avg_hold_ms,
    MAX(hold_ms) AS max_hold_ms,
    AVG(mae_bps) AS avg_mae_bps,
    AVG(mfe_bps) AS avg_mfe_bps
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '30 days') * 1000
  AND (
      close_reason_raw ILIKE '%TIME%'
      OR close_reason_raw ILIKE '%ORPHAN%'
      OR close_reason ILIKE '%TIME%'
      OR close_reason ILIKE '%ORPHAN%'
  )
GROUP BY 1, 2, 3
ORDER BY loss_rate DESC, n DESC;


-- ============================================================================
-- 2. Focus: Asia session timeout losses (>55% loss rate = BAD)
-- ============================================================================
SELECT
    symbol,
    session,
    close_reason_raw,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE pnl_net < 0)::float / COUNT(*) AS loss_rate,
    SUM(CASE WHEN pnl_net < 0 THEN pnl_net ELSE 0 END) AS total_loss_net,
    AVG(hold_ms) / 60000.0 AS avg_hold_min,
    AVG(pnl_bps) AS avg_pnl_bps
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '7 days') * 1000
  AND session = 'asia'
  AND (close_reason_raw ILIKE '%TIME%' OR close_reason_raw ILIKE '%ORPHAN%')
GROUP BY 1, 2, 3
ORDER BY loss_rate DESC;


-- ============================================================================
-- 3. Focus: Weekend timeout losses
-- ============================================================================
SELECT
    symbol,
    EXTRACT(ISODOW FROM to_timestamp(exit_ts_ms / 1000.0))::int AS dow,
    CASE
        WHEN EXTRACT(ISODOW FROM to_timestamp(exit_ts_ms / 1000.0))::int >= 5 THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS dow_type,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE pnl_net < 0)::float / COUNT(*) AS loss_rate,
    AVG(pnl_net) AS avg_pnl_net,
    AVG(hold_ms) / 60000.0 AS avg_hold_min
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '14 days') * 1000
  AND (close_reason_raw ILIKE '%TIME%' OR close_reason_raw ILIKE '%ORPHAN%')
GROUP BY 1, 2, 3
ORDER BY loss_rate DESC;


-- ============================================================================
-- 4. Stale price forced-closes (should be 0 in production)
-- ============================================================================
SELECT
    symbol,
    session,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE close_reason_raw LIKE '%NO_PRICE%')::float / COUNT(*) AS no_price_rate,
    AVG(pnl_net) AS avg_pnl_net,
    AVG(pnl_bps) AS avg_pnl_bps
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '7 days') * 1000
  AND (
      close_reason_raw LIKE '%ORPHAN_TIMEOUT%'
      OR close_reason_raw LIKE '%ORPHAN_FORCED%'
  )
GROUP BY 1, 2
ORDER BY no_price_rate DESC;


-- ============================================================================
-- 5. Trailing-active positions force-closed (should NOT happen)
-- ============================================================================
-- Note: relies on trailing_active flag being set at close time
SELECT
    symbol,
    COUNT(*) AS n_trailing_closed,
    SUM(CASE WHEN pnl_net < 0 THEN 1 ELSE 0 END) AS n_trailing_loss,
    AVG(pnl_net) AS avg_pnl_net
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '7 days') * 1000
  AND (close_reason_raw ILIKE '%ORPHAN%' OR close_reason_raw ILIKE '%TIME%')
GROUP BY 1;


-- ============================================================================
-- 6. Real vs Virtual position orphan closes (should only close virtual)
-- ============================================================================
SELECT
    CASE
        WHEN is_virtual THEN 'VIRTUAL'
        ELSE 'REAL'
    END AS pos_type,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE close_reason_raw ILIKE '%ORPHAN%') AS n_orphan_closed,
    COUNT(*) FILTER (WHERE close_reason_raw ILIKE '%ORPHAN%')::float / COUNT(*) AS orphan_close_rate,
    AVG(pnl_net) AS avg_pnl_net
FROM trades_closed
WHERE exit_ts_ms >= EXTRACT(EPOCH FROM now() - interval '14 days') * 1000
GROUP BY 1;


-- ============================================================================
-- 7. Continuous Aggregate for 1-hour monitoring (if enabled)
-- ============================================================================
-- CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_timeout_exit_1h
-- WITH (timescaledb.continuous) AS
-- SELECT
--     time_bucket('1 hour', to_timestamp(exit_ts_ms / 1000.0)) AS bucket,
--     symbol,
--     COALESCE(session, 'na') AS session,
--     close_reason_raw,
--     COUNT(*) AS n,
--     COUNT(*) FILTER (WHERE pnl_net < 0)::float / COUNT(*) AS loss_rate,
--     AVG(pnl_net) AS avg_pnl_net,
--     AVG(hold_ms) AS avg_hold_ms
-- FROM trades_closed
-- WHERE close_reason_raw ILIKE '%TIME%'
--    OR close_reason_raw ILIKE '%ORPHAN%'
-- GROUP BY bucket, symbol, session, close_reason_raw;


-- ============================================================================
-- 8. Compare old behavior vs new (by timestamp of deploy, if available)
-- ============================================================================
-- OLD (before fix): uses 2-min orphan timeout, allows -2.0 bps time-exit
-- NEW (after fix): uses signal-based TTL, -0.0 bps time-exit default
SELECT
    EXTRACT(ISODOW FROM to_timestamp(exit_ts_ms / 1000.0))::int AS dow,
    COALESCE(session, 'na') AS session,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE pnl_net < 0)::float / COUNT(*) AS loss_rate_old,
    SUM(CASE WHEN pnl_net < 0 THEN pnl_net ELSE 0 END) AS total_loss_old
FROM trades_closed
WHERE exit_ts_ms < (SELECT EXTRACT(EPOCH FROM now()) * 1000 - INTERVAL '7 days' SECOND) * 1000
  AND (close_reason_raw ILIKE '%TIME%' OR close_reason_raw ILIKE '%ORPHAN%')
GROUP BY 1, 2
ORDER BY loss_rate_old DESC;
