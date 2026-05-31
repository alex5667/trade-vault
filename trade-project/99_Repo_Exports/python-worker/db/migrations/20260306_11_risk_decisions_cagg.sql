-- P4.6: Continuous aggregates for risk decision quality and SLOs.

-- 1-Hour Continuous Aggregate
create materialized view if not exists risk_decision_summary_1h
with (timescaledb.continuous) as
select
  time_bucket('1 hour', ts) as bucket,
  tier,
  level,
  count(*) as decision_count,
  coalesce(sum(case when allow_trade_publish then 1 else 0 end), 0) as allow_count,
  coalesce(sum(case when not allow_trade_publish then 1 else 0 end), 0) as deny_count,
  coalesce(sum(case when clamp_ratio < 0.999 then 1 else 0 end), 0) as clamp_count,
  coalesce(sum(case when jsonb_path_exists(reasons_jsonb, '$[*] ? (@ == "confidence_below_tier_floor")') then 1 else 0 end), 0) as confidence_denial_count,
  coalesce(avg(clamp_ratio), 1.0) as avg_clamp_ratio,
  coalesce(avg(decision_latency_ms), 0) as decision_latency_avg_ms,
  coalesce(max(decision_latency_ms), 0) as decision_latency_max_ms,
  max(ts) as latest_created_ts
from risk_decisions
group by bucket, tier, level;

-- Add refresh policy for 1h CAGG (refresh last 2 hours every 5 minutes)
select add_continuous_aggregate_policy('risk_decision_summary_1h',
  start_offset => interval '3 hours',
  end_offset => interval '1 hour',
  schedule_interval => interval '5 minutes',
  if_not_exists => true);

-- 24-Hour Continuous Aggregate
create materialized view if not exists risk_decision_summary_24h
with (timescaledb.continuous) as
select
  time_bucket('24 hours', ts) as bucket,
  tier,
  level,
  count(*) as decision_count,
  coalesce(sum(case when allow_trade_publish then 1 else 0 end), 0) as allow_count,
  coalesce(sum(case when not allow_trade_publish then 1 else 0 end), 0) as deny_count,
  coalesce(sum(case when clamp_ratio < 0.999 then 1 else 0 end), 0) as clamp_count,
  coalesce(sum(case when jsonb_path_exists(reasons_jsonb, '$[*] ? (@ == "confidence_below_tier_floor")') then 1 else 0 end), 0) as confidence_denial_count,
  coalesce(avg(clamp_ratio), 1.0) as avg_clamp_ratio,
  coalesce(avg(decision_latency_ms), 0) as decision_latency_avg_ms,
  coalesce(max(decision_latency_ms), 0) as decision_latency_max_ms,
  max(ts) as latest_created_ts
from risk_decisions
group by bucket, tier, level;

-- Add refresh policy for 24h CAGG (refresh last 3 days every 1 hour)
select add_continuous_aggregate_policy('risk_decision_summary_24h',
  start_offset => interval '3 days',
  end_offset => interval '1 hour',
  schedule_interval => interval '1 hour',
  if_not_exists => true);
