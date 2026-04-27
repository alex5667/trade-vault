-- Migration: of_gate_metrics + quarantine + Timescale rollups (P2/P3)
-- Purpose:
--   1) Archive Redis stream `metrics:of_gate` into Postgres/Timescale (per-event rows)
--   2) Archive DQ quarantine stream `quarantined:metrics:of_gate` separately (dirty rows)
--   3) Provide continuous aggregates (5m/1h) + retention policies (Timescale only)
--
-- Safe to run multiple times (all DDL uses IF NOT EXISTS / fail-open blocks).
-- Works without TimescaleDB: Timescale-specific blocks are wrapped in EXCEPTION handlers.

-- Raw metrics table (one row per of_gate evaluation event)
CREATE TABLE IF NOT EXISTS of_gate_metrics (
  stream_id TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  scenario_v4 TEXT NOT NULL,
  schema_version INT NOT NULL,
  ok SMALLINT NOT NULL,
  ok_soft SMALLINT NOT NULL,
  missing_legs JSONB,
  reason_code TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  PRIMARY KEY (stream_id, ts)
);

-- Quarantine table (DQ-flagged / dirty rows archived separately)
CREATE TABLE IF NOT EXISTS of_gate_metrics_quarantine (
  stream_id TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  source_stream TEXT NOT NULL,
  symbol TEXT,
  scenario_v4 TEXT,
  schema_version INT,
  ok SMALLINT,
  ok_soft SMALLINT,
  dq_code TEXT NOT NULL,
  err TEXT,
  payload_json JSONB NOT NULL,
  PRIMARY KEY (stream_id, ts)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS of_gate_metrics_symbol_ts_idx ON of_gate_metrics (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS of_gate_metrics_scenario_ts_idx ON of_gate_metrics (scenario_v4, ts DESC);
CREATE INDEX IF NOT EXISTS of_gate_metrics_reason_ts_idx ON of_gate_metrics (reason_code, ts DESC);

CREATE INDEX IF NOT EXISTS of_gate_q_dq_code_ts_idx ON of_gate_metrics_quarantine (dq_code, ts DESC);
CREATE INDEX IF NOT EXISTS of_gate_q_symbol_ts_idx ON of_gate_metrics_quarantine (symbol, ts DESC);

-- Timescale hypertables (safe: no-op if TimescaleDB extension isn't installed)
DO $$
BEGIN
  BEGIN
    PERFORM create_hypertable('of_gate_metrics', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
  EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'Timescale not installed: skip create_hypertable(of_gate_metrics)';
  END;

  BEGIN
    PERFORM create_hypertable('of_gate_metrics_quarantine', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
  EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'Timescale not installed: skip create_hypertable(of_gate_metrics_quarantine)';
  END;
END $$;

-- Continuous aggregates + policies (Timescale only, safe no-op otherwise)
DO $$
BEGIN
  -- 5-minute rollup: ok_rate per symbol/scenario_v4
  BEGIN
    EXECUTE $$
    CREATE MATERIALIZED VIEW IF NOT EXISTS of_gate_ok_rate_5m
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('5 minutes', ts) AS bucket,
      symbol,
      scenario_v4,
      count(*)::bigint AS eligible_count,
      sum(ok)::bigint AS ok_hard_count,
      sum(ok_soft)::bigint AS ok_soft_count,
      CASE WHEN count(*) = 0 THEN NULL ELSE (sum(ok)::numeric / count(*)::numeric) END AS ok_rate_strict,
      CASE WHEN count(*) = 0 THEN NULL ELSE ((sum(ok)+sum(ok_soft))::numeric / count(*)::numeric) END AS ok_rate_soft,
      CASE WHEN (sum(ok)+sum(ok_soft)) = 0 THEN NULL ELSE (sum(ok_soft)::numeric / (sum(ok)+sum(ok_soft))::numeric) END AS soft_share
    FROM of_gate_metrics
    GROUP BY 1,2,3;
    $$;
  EXCEPTION WHEN others THEN
    NULL;
  END;

  -- 1-hour rollup: ok_rate per symbol/scenario_v4
  BEGIN
    EXECUTE $$
    CREATE MATERIALIZED VIEW IF NOT EXISTS of_gate_ok_rate_1h
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('1 hour', ts) AS bucket,
      symbol,
      scenario_v4,
      count(*)::bigint AS eligible_count,
      sum(ok)::bigint AS ok_hard_count,
      sum(ok_soft)::bigint AS ok_soft_count,
      CASE WHEN count(*) = 0 THEN NULL ELSE (sum(ok)::numeric / count(*)::numeric) END AS ok_rate_strict,
      CASE WHEN count(*) = 0 THEN NULL ELSE ((sum(ok)+sum(ok_soft))::numeric / count(*)::numeric) END AS ok_rate_soft,
      CASE WHEN (sum(ok)+sum(ok_soft)) = 0 THEN NULL ELSE (sum(ok_soft)::numeric / (sum(ok)+sum(ok_soft))::numeric) END AS soft_share
    FROM of_gate_metrics
    GROUP BY 1,2,3;
    $$;
  EXCEPTION WHEN others THEN
    NULL;
  END;

  -- Continuous aggregate refresh policies
  BEGIN
    PERFORM add_continuous_aggregate_policy('of_gate_ok_rate_5m', start_offset => INTERVAL '1 day', end_offset => INTERVAL '5 minutes', schedule_interval => INTERVAL '5 minutes');
  EXCEPTION WHEN undefined_function THEN
    NULL;
  END;

  BEGIN
    PERFORM add_continuous_aggregate_policy('of_gate_ok_rate_1h', start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour');
  EXCEPTION WHEN undefined_function THEN
    NULL;
  END;

  -- Retention: 30 days raw (quarantine + main)
  BEGIN
    PERFORM add_retention_policy('of_gate_metrics', INTERVAL '30 days');
  EXCEPTION WHEN undefined_function THEN
    NULL;
  END;

  BEGIN
    PERFORM add_retention_policy('of_gate_metrics_quarantine', INTERVAL '30 days');
  EXCEPTION WHEN undefined_function THEN
    NULL;
  END;

EXCEPTION WHEN others THEN
  -- Timescale not installed or insufficient permissions: silently skip all CAGG/policy creation.
  NULL;
END $$;
