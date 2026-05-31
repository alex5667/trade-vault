-- metrics_raw: time-series store for all signal quality / DQ metrics.
-- Used by: posttrade workers, nightly calibration, ops dashboards.

CREATE TABLE IF NOT EXISTS metrics_raw (
  ts              timestamptz NOT NULL,
  sym             text        NOT NULL,
  venue           text        NOT NULL,
  session         text        NOT NULL,
  tf              text        NOT NULL,
  kind            text        NOT NULL,
  side            text        NOT NULL,
  metric_name     text        NOT NULL,
  metric_value    double precision NOT NULL,
  source_stage    text        NOT NULL,
  run_id          text        NULL,
  extra           jsonb       NULL
);

SELECT create_hypertable('metrics_raw', 'ts', if_not_exists => TRUE);

ALTER TABLE metrics_raw SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sym,venue,metric_name,source_stage',
  timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('metrics_raw', INTERVAL '7 days', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_raw_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(INTERVAL '5 minutes', ts) AS bucket,
  sym,
  venue,
  source_stage,
  metric_name,
  avg(metric_value) AS metric_avg,
  min(metric_value) AS metric_min,
  max(metric_value) AS metric_max
FROM metrics_raw
GROUP BY 1,2,3,4,5;
