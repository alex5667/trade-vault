-- Migration: 049_create_exec_slip_stats_mv
-- Purpose: Restore missing slippage evaluation materialized view for promoter/rollback/freezer.
-- Date: 2026-04-21

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_exec_slippage_eval_1h_stats AS
SELECT
  time_bucket('1 hour', ts) as t,
  sym,
  exec_regime_bucket,
  count(*)::bigint as n,
  percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
  percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
  avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
FROM v_exec_slippage_eval
GROUP BY 1,2,3
WITH NO DATA;

-- Unique index on (t, sym, exec_regime_bucket) is required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS mv_exec_slippage_eval_1h_stats_ux
  ON mv_exec_slippage_eval_1h_stats(t, sym, exec_regime_bucket);

-- Note: Initial refresh should be handled by the application or manual step after migration
-- REFRESH MATERIALIZED VIEW mv_exec_slippage_eval_1h_stats;
