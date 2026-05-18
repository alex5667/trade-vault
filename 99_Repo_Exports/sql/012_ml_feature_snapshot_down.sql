-- 012_ml_feature_snapshot_down.sql
SELECT remove_retention_policy('ml_feature_snapshot', if_not_exists => TRUE);
SELECT remove_compression_policy('ml_feature_snapshot', if_not_exists => TRUE);
DROP TABLE IF EXISTS ml_feature_snapshot CASCADE;
