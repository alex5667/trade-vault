-- P80: fast stats source for promoter/rollback/freezer.
-- Materialized view built from v_exec_slippage_eval.
--
-- Run once in analytics DB, then refresh periodically (see refresh_exec_slip_stats_p80.py).
-- We store hourly p95/p99 residuals and edge_neg_share; guardrails use max() across lookback.
--
-- Requires timescaledb extension for time_bucket.
create materialized view if not exists mv_exec_slippage_eval_1h_stats as
select
  time_bucket('1 hour', ts) as t,
  sym,
  exec_regime_bucket,
  count(*)::bigint as n,
  percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
  percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
  avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
from v_exec_slippage_eval
group by 1,2,3
with no data;

create unique index if not exists mv_exec_slippage_eval_1h_stats_ux
  on mv_exec_slippage_eval_1h_stats(t, sym, exec_regime_bucket);
