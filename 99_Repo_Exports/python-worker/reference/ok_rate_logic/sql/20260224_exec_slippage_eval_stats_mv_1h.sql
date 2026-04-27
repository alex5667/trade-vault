-- P80: fast stats source for promoter/rollback/freezer.
-- Materialized view built from v_exec_slippage_eval.
--
-- NOTE: Keep this definition aligned with services/archivers/sql/20260224_exec_slippage_eval_stats_mv_1h.sql.
-- v_exec_slippage_eval exposes:
--   - realized_slip_worse_bps
--   - expected_slip_decomp_bps
--   - edge_minus_expected_bps
--
-- Requires timescaledb extension for time_bucket.
create materialized view if not exists mv_exec_slippage_eval_1h_stats as
select
  time_bucket('1 hour', ts) as t,
  sym,
  exec_regime_bucket,
  count(*)::bigint as n,
  -- V8 fix: residual = difference between realized and model-expected slippage (correct column names)
  percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
  percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
  -- V8 fix: use edge_minus_expected_bps (not legacy edge_minus_expected_bps)
  avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
from v_exec_slippage_eval
group by 1,2,3
with no data;

create unique index if not exists mv_exec_slippage_eval_1h_stats_ux
  on mv_exec_slippage_eval_1h_stats(t, sym, exec_regime_bucket);
