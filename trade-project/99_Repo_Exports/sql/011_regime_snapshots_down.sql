-- Down migration: 011_regime_snapshots
-- Drops the regime_snapshots hypertable, its continuous aggregate, and all policies.

SELECT remove_continuous_aggregate_policy('regime_snapshots_1h', if_exists => TRUE);
SELECT remove_retention_policy('regime_snapshots_1h', if_exists => TRUE);
SELECT remove_retention_policy('regime_snapshots', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS regime_snapshots_1h CASCADE;

DROP TABLE IF EXISTS regime_snapshots CASCADE;
