DROP MATERIALIZED VIEW IF EXISTS mv_exec_slippage_eval_1h_stats;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_exec_slippage_eval_1h_stats AS
SELECT
  sym,
  time_bucket('1 hour', ts) AS t,
  exec_regime_bucket,
  count(*)::bigint AS n,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY slippage_residual_bps) AS resid_p95_bps,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY slippage_residual_bps) AS resid_p99_bps,
  avg(CASE WHEN edge_minus_expected_bps < 0 THEN 1.0 ELSE 0.0 END) AS edge_neg_share
FROM v_exec_slippage_eval
GROUP BY 1,2,3;

CREATE INDEX IF NOT EXISTS mv_exec_slippage_eval_1h_stats_sym_t_idx
  ON mv_exec_slippage_eval_1h_stats (sym, t DESC);

CREATE INDEX IF NOT EXISTS mv_exec_slippage_eval_1h_stats_bucket_t_idx
  ON mv_exec_slippage_eval_1h_stats (exec_regime_bucket, t DESC);
