-- P90: Extended hourly stats for v_exec_slippage_eval (decomp + model residuals)
--
-- This MV is optional and backward-compatible: it keeps the original columns
-- (resid_p95_bps/resid_p99_bps/edge_neg_share) and adds *_model_* variants.
--
-- Usage:
--   - refresher:  EXEC_SLIP_STATS_MV=mv_exec_slippage_eval_1h_stats_v2
--   - exporter:   ENFORCE_STATE_EXPORTER_DB_MV=mv_exec_slippage_eval_1h_stats_v2

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_exec_slippage_eval_1h_stats_v2 AS
SELECT
  time_bucket('1 hour', ts) AS t,
  sym,
  exec_regime_bucket,
  count(*)::bigint AS n,

  percentile_cont(0.95) WITHIN GROUP (ORDER BY slippage_residual_bps) AS resid_p95_bps,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY slippage_residual_bps) AS resid_p99_bps,
  avg(CASE WHEN edge_minus_expected_bps < 0 THEN 1 ELSE 0 END) AS edge_neg_share,

  percentile_cont(0.95) WITHIN GROUP (ORDER BY slippage_residual_model_bps) AS resid_model_p95_bps,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY slippage_residual_model_bps) AS resid_model_p99_bps,
  avg(CASE WHEN edge_minus_expected_model_bps < 0 THEN 1 ELSE 0 END) AS edge_neg_share_model

FROM v_exec_slippage_eval
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_exec_slippage_eval_1h_stats_v2_ux
  ON mv_exec_slippage_eval_1h_stats_v2 (t, sym, exec_regime_bucket);
