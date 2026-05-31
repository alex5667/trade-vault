-- Phase 0 pre-Phase 1 reliability/quality analytics.
-- Read-only. Run from any psql against scanner_analytics.
-- Each block is independent — execute selectively in psql via \i or paste a single block.
--
-- Notes:
--   * Phase 5 full reliability (confidence_pct → realized_winrate) is currently BLOCKED:
--     signal_confidence_scores is empty in scanner_analytics, and decision_snapshot.extra
--     does not carry confidence. Re-run that block after confidence is persisted per-trade.
--   * Source view: signal_outcome_join (defined in 20260519_01/_03 migrations).
--
-- Usage:
--   psql -h scanner-postgres -U postgres -d scanner_analytics -f phase0_reliability.sql
--   or: docker exec -i scanner-postgres psql -U postgres -d scanner_analytics < phase0_reliability.sql


-- ============================================================
-- 1. close_reason × outcome summary (last 14d)
-- ============================================================
\echo === 1. close_reason summary ===
SELECT close_reason,
       COUNT(*) AS n,
       ROUND(AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate,
       ROUND(AVG(r_multiple)::numeric, 3) AS avg_r,
       ROUND(SUM(pnl_net)::numeric, 2)    AS pnl,
       ROUND(AVG(hold_ms)::numeric, 0)    AS avg_hold_ms
FROM signal_outcome_join
WHERE is_final_close
  AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
GROUP BY close_reason ORDER BY pnl ASC;


-- ============================================================
-- 2. timeout_class deep-dive (only TIMEOUT closes, last 14d)
--    Use to gate Phase 2 smart-timeout design — biggest target = timeout_no_followthrough.
-- ============================================================
\echo === 2. timeout_class ===
SELECT timeout_class,
       COUNT(*) AS n,
       ROUND(AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate,
       ROUND(AVG(r_multiple)::numeric, 3) AS avg_r,
       ROUND(SUM(pnl_net)::numeric, 2)    AS pnl,
       ROUND(AVG(mfe_pnl / NULLIF(one_r_money, 0))::numeric, 3) AS avg_mfe_r,
       ROUND(AVG(hold_ms)::numeric, 0)    AS avg_hold_ms
FROM signal_outcome_join
WHERE close_reason = 'TIMEOUT' AND is_final_close
  AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
GROUP BY timeout_class ORDER BY n DESC;


-- ============================================================
-- 3. R-multiple reliability bins (proxy for Phase 5 reliability)
--    Realized winrate per r_multiple band. Use to see if r_multiple ranking
--    aligns with actual win probability.
-- ============================================================
\echo === 3. r_multiple reliability bins ===
WITH binned AS (
  SELECT
    CASE
      WHEN r_multiple IS NULL THEN 'null'
      WHEN r_multiple <= -1.0 THEN '<= -1.0R'
      WHEN r_multiple <= -0.5 THEN '(-1.0, -0.5]'
      WHEN r_multiple <  0    THEN '(-0.5, 0)'
      WHEN r_multiple <  0.25 THEN '[0, 0.25)'
      WHEN r_multiple <  0.5  THEN '[0.25, 0.5)'
      WHEN r_multiple <  1.0  THEN '[0.5, 1.0)'
      ELSE '>= 1.0R'
    END AS r_bin,
    *
  FROM signal_outcome_join
  WHERE is_final_close
    AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
)
SELECT r_bin,
       COUNT(*) AS n,
       ROUND(AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate,
       ROUND(AVG(r_multiple)::numeric, 3) AS avg_r,
       ROUND(SUM(pnl_net)::numeric, 2)    AS pnl
FROM binned
GROUP BY r_bin
ORDER BY MIN(r_multiple) NULLS LAST;


-- ============================================================
-- 4. Regime × close_reason matrix (PnL distribution)
--    After regime backfill (20260519_01 + 20260519_02), all 5 regimes are populated.
-- ============================================================
\echo === 4. regime x close_reason ===
SELECT regime, close_reason,
       COUNT(*) AS n,
       ROUND(AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate,
       ROUND(AVG(r_multiple)::numeric, 3) AS avg_r,
       ROUND(SUM(pnl_net)::numeric, 2)    AS pnl
FROM signal_outcome_join
WHERE is_final_close
  AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
GROUP BY regime, close_reason
HAVING COUNT(*) >= 20
ORDER BY pnl ASC LIMIT 25;


-- ============================================================
-- 5. MFE/MAE/giveback summary by side × close_reason
--    Useful for tuning trailing/BE policy (Phase 2).
-- ============================================================
\echo === 5. mfe/mae/giveback by side x close_reason ===
SELECT side, close_reason,
       COUNT(*) AS n,
       ROUND(AVG(mfe_pnl / NULLIF(one_r_money, 0))::numeric, 3) AS avg_mfe_r,
       ROUND(AVG(mae_pnl / NULLIF(one_r_money, 0))::numeric, 3) AS avg_mae_r,
       ROUND(AVG(giveback / NULLIF(one_r_money, 0))::numeric, 3) AS avg_giveback_r,
       ROUND(AVG(r_multiple)::numeric, 3)                       AS avg_r
FROM signal_outcome_join
WHERE is_final_close
  AND one_r_money > 0
  AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
GROUP BY side, close_reason
HAVING COUNT(*) >= 30
ORDER BY side, close_reason;


-- ============================================================
-- 6. Bad-buckets refreshed view (top-30)
-- ============================================================
\echo === 6. bad_signal_buckets top-30 ===
SELECT symbol, kind, side, session, regime, close_reason, n, winrate, avg_r, pnl_net, avg_hold_ms
FROM bad_signal_buckets
ORDER BY pnl_net ASC LIMIT 30;


-- ============================================================
-- 7. (BLOCKED) confidence_pct reliability — placeholder
--    Re-enable when signal_confidence_scores is populated, or when
--    confidence_pct is added to trades_closed/_p0 / decision_snapshot.extra.
-- ============================================================
\echo === 7. confidence reliability (BLOCKED placeholder) ===
SELECT 'BLOCKED: signal_confidence_scores is empty; no per-trade confidence persisted'
       AS status;
