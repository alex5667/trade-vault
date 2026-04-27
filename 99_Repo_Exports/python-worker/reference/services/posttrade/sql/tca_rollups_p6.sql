-- P6 gap-closure: persisted TCA rollups for nightly/reporting workloads.
--
-- Goal: avoid repeated full scans of tca_fill_metrics for common 1h / 1d rollups.
--
-- We prefer Timescale continuous aggregates where available, but some deployments
-- may reject ordered-set percentile aggregates inside CAGG depending on version.
-- Therefore this file installs a safe two-layer approach:
--   1) low-risk CAGG with count/avg/min/max
--   2) percentile materialized views refreshed by a scheduler / nightly job

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE MATERIALIZED VIEW IF NOT EXISTS tca_fill_metrics_1h_base
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(INTERVAL '1 hour', ts) AS bucket,
  sym, venue, session, tf, kind, side,
  count(*)::bigint AS n,
  avg(is_bps) AS is_avg_bps,
  avg(eff_spread_bps) AS eff_spread_avg_bps,
  avg(realized_spread_1s_bps) AS realized_spread_1s_avg_bps,
  avg(perm_impact_1s_bps) AS perm_impact_1s_avg_bps,
  min(is_bps) AS is_min_bps,
  max(is_bps) AS is_max_bps,
  min(eff_spread_bps) AS eff_spread_min_bps,
  max(eff_spread_bps) AS eff_spread_max_bps
FROM tca_fill_metrics
GROUP BY 1,2,3,4,5,6,7
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS tca_fill_metrics_1h_base_ux
  ON tca_fill_metrics_1h_base(bucket, sym, venue, session, tf, kind, side);

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy(
    'tca_fill_metrics_1h_base',
    start_offset => INTERVAL '14 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '30 minutes'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tca_fill_metrics_1h_percentiles AS
SELECT
  time_bucket(INTERVAL '1 hour', ts) AS bucket,
  sym, venue, session, tf, kind, side,
  count(*)::bigint AS n,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY is_bps) AS is_p50_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY is_bps) AS is_p95_bps,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY is_bps) AS is_p99_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY eff_spread_bps) AS eff_spread_p95_bps,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY realized_spread_1s_bps) AS realized_spread_1s_p50_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY perm_impact_1s_bps) AS perm_impact_1s_p95_bps,
  avg(CASE WHEN realized_spread_1s_bps < 0 THEN 1 ELSE 0 END) AS realized_spread_1s_neg_share
FROM tca_fill_metrics
GROUP BY 1,2,3,4,5,6,7
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_tca_fill_metrics_1h_percentiles_ux
  ON mv_tca_fill_metrics_1h_percentiles(bucket, sym, venue, session, tf, kind, side);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tca_fill_metrics_1d_percentiles AS
SELECT
  time_bucket(INTERVAL '1 day', ts) AS bucket,
  sym, venue, session, tf, kind, side,
  count(*)::bigint AS n,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY is_bps) AS is_p50_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY is_bps) AS is_p95_bps,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY is_bps) AS is_p99_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY eff_spread_bps) AS eff_spread_p95_bps,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY realized_spread_1s_bps) AS realized_spread_1s_p50_bps,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY perm_impact_1s_bps) AS perm_impact_1s_p95_bps,
  avg(CASE WHEN realized_spread_1s_bps < 0 THEN 1 ELSE 0 END) AS realized_spread_1s_neg_share
FROM tca_fill_metrics
GROUP BY 1,2,3,4,5,6,7
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_tca_fill_metrics_1d_percentiles_ux
  ON mv_tca_fill_metrics_1d_percentiles(bucket, sym, venue, session, tf, kind, side);
