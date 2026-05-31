-- PostgreSQL initialization script for scanner-infra
-- This script runs when the PostgreSQL container starts for the first time

-- Users 'trading', 'scanner', and 'trade_user' are now securely injected via 00-init-users.sh

-- Create the scanner_analytics database
SELECT 'CREATE DATABASE scanner_analytics'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'scanner_analytics')\gexec

-- Create trade database if it doesn't exist
SELECT 'CREATE DATABASE trade'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'trade')\gexec

-- Grant permissions to the trading user
GRANT ALL PRIVILEGES ON DATABASE trade TO trading;
GRANT ALL PRIVILEGES ON DATABASE scanner_analytics TO trading;

-- Grant permissions to the scanner user
GRANT ALL PRIVILEGES ON DATABASE trade TO scanner;
GRANT ALL PRIVILEGES ON DATABASE scanner_analytics TO scanner;

-- Grant permissions to the trade_user user
GRANT ALL PRIVILEGES ON DATABASE trade TO trade_user;
GRANT ALL PRIVILEGES ON DATABASE scanner_analytics TO trade_user;

-- Connect to the trade database and grant schema permissions
\c trade;
GRANT ALL ON SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;

GRANT ALL ON SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO scanner;

GRANT ALL ON SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trade_user;

-- Create symbol_meta table in trade database
CREATE TABLE IF NOT EXISTS symbol_meta (
    id                      BIGSERIAL PRIMARY KEY,
    exchange                TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    "tickSize"              DOUBLE PRECISION,
    "minPrice"              DOUBLE PRECISION,
    "maxPrice"              DOUBLE PRECISION,
    "stepSize"              DOUBLE PRECISION,
    "minQty"                DOUBLE PRECISION,
    "maxQty"                DOUBLE PRECISION,
    "baseAsset"             TEXT,
    "quoteAsset"            TEXT,
    status                  TEXT,
    "basePrecision"         INTEGER,
    "quotePrecision"        INTEGER,
    "updatedAt"             TIMESTAMPTZ,
    "fetchedAt"             TIMESTAMPTZ,
    "rawJson"               JSONB,
    created_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE(exchange, symbol)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_symbol_meta_exchange_symbol ON symbol_meta(exchange, symbol);

-- Connect to the scanner_analytics database and grant schema permissions
\c scanner_analytics;
GRANT ALL ON SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;

GRANT ALL ON SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO scanner;

GRANT ALL ON SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trade_user;

-- Switch back to the default database
\c postgres;

-- Create TimescaleDB extension in both databases (if available)
-- Note: TimescaleDB needs to be installed separately if required

-- For trade database
\c trade;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- For scanner_analytics database
\c scanner_analytics;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Connect to the scanner_analytics database and create regime tables
\c scanner_analytics;
GRANT ALL ON SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;

GRANT ALL ON SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scanner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO scanner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO scanner;

GRANT ALL ON SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trade_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trade_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trade_user;

-- Create regime_snapshot table for storing historical regime data
CREATE TABLE IF NOT EXISTS regime_snapshot (
    id                      BIGSERIAL PRIMARY KEY,
    symbol                  TEXT NOT NULL,
    timeframe               TEXT NOT NULL,
    ts                      TIMESTAMPTZ NOT NULL,
    adx                     DOUBLE PRECISION,
    "atrPct"                DOUBLE PRECISION,
    regime                  TEXT,
    trend_score             DOUBLE PRECISION DEFAULT 0.0,
    range_score             DOUBLE PRECISION DEFAULT 0.0,
    atr_value               DOUBLE PRECISION,
    atr_quantile            DOUBLE PRECISION,
    volatility_state        TEXT,
    is_trending             BOOLEAN,
    created_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE(symbol, timeframe, ts)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_regime_snapshot_symbol_timeframe_ts ON regime_snapshot(symbol, timeframe, ts);
CREATE INDEX IF NOT EXISTS idx_regime_snapshot_ts ON regime_snapshot(ts);

-- Create regime_quantiles table for storing computed quantiles
CREATE TABLE IF NOT EXISTS regime_quantiles (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol                  TEXT NOT NULL,
    timeframe               TEXT NOT NULL,
    adx_p40                 DOUBLE PRECISION,
    adx_p60                 DOUBLE PRECISION,
    adx_p75                 DOUBLE PRECISION,
    atrp_p25                DOUBLE PRECISION,
    atrp_p50                DOUBLE PRECISION,
    atrp_p75                DOUBLE PRECISION,
    "sampleSize"            INTEGER,
    "updatedAt"             TIMESTAMPTZ DEFAULT now(),
    created_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE(symbol, timeframe)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_symbol_timeframe ON regime_quantiles(symbol, timeframe);

-- Transfer ownership of regime tables to trading user (needed for migrations to create indexes)
ALTER TABLE IF EXISTS regime_snapshot OWNER TO trading;
ALTER TABLE IF EXISTS regime_quantiles OWNER TO trading;

-- Also allow trade_user to own items if needed
ALTER TABLE IF EXISTS regime_snapshot OWNER TO trade_user;
ALTER TABLE IF EXISTS regime_quantiles OWNER TO trade_user;

-- Create calibration_state table for redundant persistent storage of calibrators
CREATE TABLE IF NOT EXISTS calibration_state (
    symbol          TEXT NOT NULL,
    regime          TEXT NOT NULL,
    kind            TEXT NOT NULL, -- 'effq', 'atr', 'dn', 'bookrate'
    ts_ms           BIGINT NOT NULL,
    state_json      JSONB NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(symbol, regime, kind)
);
CREATE INDEX IF NOT EXISTS idx_calibration_state_ts ON calibration_state (ts_ms DESC);

-- Create microbars table for historical warmup (Hypertable candidate)
CREATE TABLE IF NOT EXISTS microbars (
    symbol          TEXT NOT NULL,
    ts_ms           BIGINT NOT NULL,
    o               DOUBLE PRECISION NOT NULL,
    h               DOUBLE PRECISION NOT NULL,
    l               DOUBLE PRECISION NOT NULL,
    c               DOUBLE PRECISION NOT NULL,
    v               DOUBLE PRECISION NOT NULL,
    cvd             DOUBLE PRECISION NOT NULL,
    inserted_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(symbol, ts_ms)
);
-- Enable TimescaleDB hypertable for microbars
SELECT create_hypertable('microbars', 'ts_ms', chunk_time_interval => 86400000, if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_microbars_ts ON microbars (ts_ms DESC);

-- Create decision_snapshot table (A3 patch)
CREATE TABLE IF NOT EXISTS decision_snapshot (
  ts_decision_ms                BIGINT NOT NULL,
  sid                           TEXT   NOT NULL,
  symbol                        TEXT   NOT NULL,
  venue                         TEXT   NOT NULL DEFAULT 'binance',
  session                       TEXT   NOT NULL DEFAULT '',
  tf                            TEXT   NOT NULL DEFAULT '',
  kind                          TEXT   NOT NULL DEFAULT '',
  side                          TEXT   NOT NULL DEFAULT '',
  direction                     TEXT   NOT NULL DEFAULT '',

  decision_bid                  DOUBLE PRECISION NULL,
  decision_ask                  DOUBLE PRECISION NULL,
  decision_mid                  DOUBLE PRECISION NULL,
  decision_spread_bps           DOUBLE PRECISION NULL,

  decision_depth_bid_5          DOUBLE PRECISION NULL,
  decision_depth_ask_5          DOUBLE PRECISION NULL,
  decision_depth_bid_20         DOUBLE PRECISION NULL,
  decision_depth_ask_20         DOUBLE PRECISION NULL,

  decision_book_slope_bid       DOUBLE PRECISION NULL,
  decision_book_slope_ask       DOUBLE PRECISION NULL,
  decision_dws_bps              DOUBLE PRECISION NULL,

  decision_ofi_norm             DOUBLE PRECISION NULL,
  decision_expected_slippage_bps DOUBLE PRECISION NULL,
  decision_exec_risk_norm       DOUBLE PRECISION NULL,

  decision_price                DOUBLE PRECISION NULL,

  tca_ready                     BOOLEAN NOT NULL DEFAULT FALSE,
  book_sanity_flags             TEXT[]  NOT NULL DEFAULT ARRAY[]::TEXT[],

  schema_version                INTEGER NOT NULL DEFAULT 1,
  producer                      TEXT    NOT NULL DEFAULT '',
  ts_insert_ms                  BIGINT  NOT NULL DEFAULT 0,

  extra                         JSONB   NULL,

  UNIQUE (sid, ts_decision_ms)
);

-- Create hypertable if TimescaleDB is available.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable('decision_snapshot', 'ts_decision_ms', if_not_exists => TRUE);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_sid ON decision_snapshot (sid);
CREATE INDEX IF NOT EXISTS idx_decision_snapshot_symbol_ts ON decision_snapshot (symbol, ts_decision_ms DESC);

-- Enable compression for decision_snapshot
ALTER TABLE decision_snapshot SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol'
);

DO $$
BEGIN
  PERFORM add_compression_policy('decision_snapshot', INTERVAL '7 days');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

-- Phase B (P1) — TCA storage schema for Timescale/Postgres
--
-- Tables:
--   - bbo_ts           : BBO time-series snapshots (for mid_{t+Δ})
--   - fills            : normalized fills (entry/exit) joinable by sid
--   - tca_fill_metrics : per-fill TCA metrics (effective/realized spread, impact, IS)
--
-- Conventions:
--   - event-time columns are epoch-ms BIGINT for deterministic joins
--   - a `ts TIMESTAMPTZ` companion is stored for Timescale hypertables
--
-- NOTE: decision_snapshot table is created by decision_snapshot_timescale.sql (A3).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- bbo_ts: best bid/ask + mid at time ts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bbo_ts (
  ts          TIMESTAMPTZ NOT NULL,
  ts_ms       BIGINT NOT NULL,
  sym         TEXT NOT NULL,
  venue       TEXT NOT NULL,
  bid         DOUBLE PRECISION NOT NULL,
  ask         DOUBLE PRECISION NOT NULL,
  mid         DOUBLE PRECISION NOT NULL,
  producer    TEXT NOT NULL DEFAULT '',
  schema_version INT NOT NULL DEFAULT 1,
  stream_id   TEXT NULL,
  PRIMARY KEY (sym, venue, ts_ms, ts)
);

SELECT create_hypertable('bbo_ts', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS bbo_ts_sym_venue_ts_idx
  ON bbo_ts (sym, venue, ts DESC);

-- Compression: keep recent uncompressed for fast writes
ALTER TABLE bbo_ts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sym,venue'
);

-- Optional policy (safe if Timescale installed). Adjust via migrations.
DO $$
BEGIN
  PERFORM add_compression_policy('bbo_ts', INTERVAL '3 days');
EXCEPTION WHEN OTHERS THEN
  -- Fail-open: policy may already exist or Timescale not enabled.
END $$;

-- ---------------------------------------------------------------------
-- fills: normalized execution fills (entry/exit)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fills (
  ts          TIMESTAMPTZ NOT NULL,
  ts_fill_ms  BIGINT NOT NULL,
  sid         TEXT NOT NULL,
  order_id    TEXT NOT NULL,
  sym         TEXT NOT NULL,
  venue       TEXT NOT NULL,
  side        TEXT NOT NULL,  -- LONG|SHORT
  fill_role   TEXT NOT NULL,  -- entry|exit
  px          DOUBLE PRECISION NOT NULL,
  qty         DOUBLE PRECISION NOT NULL,
  fee_bps     DOUBLE PRECISION NOT NULL,

  bid_at_fill DOUBLE PRECISION NULL,
  ask_at_fill DOUBLE PRECISION NULL,
  mid_at_fill DOUBLE PRECISION NULL,

  event_type  TEXT NOT NULL DEFAULT '',
  event_id    TEXT NULL,
  stream_id   TEXT NULL,
  ts_insert_ms BIGINT NOT NULL DEFAULT 0,

  PRIMARY KEY (sid, ts_fill_ms, fill_role, ts)
);

SELECT create_hypertable('fills', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS fills_sym_venue_ts_idx
  ON fills (sym, venue, ts DESC);
CREATE INDEX IF NOT EXISTS fills_sid_ts_idx
  ON fills (sid, ts DESC);

-- ---------------------------------------------------------------------
-- tca_fill_metrics: per-fill TCA metrics
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tca_fill_metrics (
  ts            TIMESTAMPTZ NOT NULL,
  ts_fill_ms    BIGINT NOT NULL,
  sid           TEXT NOT NULL,
  sym           TEXT NOT NULL,
  venue         TEXT NOT NULL,
  side          TEXT NOT NULL,
  fill_role     TEXT NOT NULL,

  -- decision context (from decision_snapshot)
  decision_ts_ms BIGINT NOT NULL,
  session       TEXT NOT NULL,
  tf            TEXT NOT NULL,
  kind          TEXT NOT NULL,
  decision_mid  DOUBLE PRECISION NULL,

  -- mid/bbo at fill (from bbo_ts join)
  mid_t         DOUBLE PRECISION NULL,
  bid_t         DOUBLE PRECISION NULL,
  ask_t         DOUBLE PRECISION NULL,

  -- mid at t+Δ
  mid_t_1s      DOUBLE PRECISION NULL,
  mid_t_5s      DOUBLE PRECISION NULL,

  -- core metrics (bps)
  eff_spread_bps DOUBLE PRECISION NULL,
  realized_spread_1s_bps DOUBLE PRECISION NULL,
  realized_spread_5s_bps DOUBLE PRECISION NULL,
  perm_impact_1s_bps DOUBLE PRECISION NULL,
  perm_impact_5s_bps DOUBLE PRECISION NULL,
  is_bps        DOUBLE PRECISION NULL,

  -- raw execution
  px            DOUBLE PRECISION NOT NULL,
  qty           DOUBLE PRECISION NOT NULL,
  fee_bps       DOUBLE PRECISION NOT NULL,

  ts_insert_ms  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (sid, ts_fill_ms, fill_role, ts)
);

SELECT create_hypertable('tca_fill_metrics', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS tca_fill_metrics_sym_venue_ts_idx
  ON tca_fill_metrics (sym, venue, ts DESC);
CREATE INDEX IF NOT EXISTS tca_fill_metrics_sid_ts_idx
  ON tca_fill_metrics (sid, ts DESC);

-- Optional CAGG example (1m rollups) — keep commented until you confirm volume.
-- CREATE MATERIALIZED VIEW IF NOT EXISTS tca_rollups_1m
-- WITH (timescaledb.continuous) AS
-- SELECT
--   time_bucket('1 minute', ts) AS bucket,
--   sym, venue, session, tf, kind, side,
--   percentile_cont(0.50) WITHIN GROUP (ORDER BY is_bps) AS is_p50_bps,
--   percentile_cont(0.95) WITHIN GROUP (ORDER BY is_bps) AS is_p95_bps,
--   percentile_cont(0.99) WITHIN GROUP (ORDER BY is_bps) AS is_p99_bps
-- FROM tca_fill_metrics
-- WHERE is_bps IS NOT NULL
-- GROUP BY bucket, sym, venue, session, tf, kind, side;


-- Connect to trade database and create tb_labels table (v10.1)
\c trade;

-- Triple-Barrier Labels Table (v10.1)
CREATE TABLE IF NOT EXISTS tb_labels (
  sid            TEXT PRIMARY KEY,
  symbol         TEXT NOT NULL,
  ts_ms          BIGINT NOT NULL,
  direction      TEXT NOT NULL,

  primary_h_ms   INTEGER NOT NULL,
  primary_label  TEXT NOT NULL,         -- TP|SL|TIMEOUT|NO_TICKS
  primary_hit_ms BIGINT NOT NULL,
  primary_ret_bps DOUBLE PRECISION NOT NULL,
  primary_r_mult  DOUBLE PRECISION NOT NULL,
  primary_y_edge  INTEGER NOT NULL,

  horizons_json  JSONB NOT NULL,        -- {"60000": {...}, "180000": {...}, ...}
  ticks_sample   JSONB,                 -- optional: [[ts,price], ...] sampled
  meta           JSONB,                 -- costs/spread/slip/exec_risk_bps, etc.

  created_ms     BIGINT NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS tb_labels_symbol_ts_idx ON tb_labels(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS tb_labels_ts_ms_idx ON tb_labels(ts_ms DESC);
CREATE INDEX IF NOT EXISTS tb_labels_direction_idx ON tb_labels(direction, ts_ms DESC);
CREATE INDEX IF NOT EXISTS tb_labels_primary_label_idx ON tb_labels(primary_label, ts_ms DESC);

-- JSONB index for flexible queries on horizons_json and meta
CREATE INDEX IF NOT EXISTS tb_labels_horizons_gin_idx ON tb_labels USING gin (horizons_json);
CREATE INDEX IF NOT EXISTS tb_labels_meta_gin_idx ON tb_labels USING gin (meta);

-- Convert to hypertable
SELECT create_hypertable('tb_labels', 'ts_ms', chunk_time_interval => 86400000, if_not_exists => TRUE);

-- Enable compression for tb_labels
ALTER TABLE tb_labels SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol'
);

DO $$
BEGIN
  PERFORM add_compression_policy('tb_labels', INTERVAL '7 days');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE tb_labels TO trading;
GRANT ALL PRIVILEGES ON TABLE tb_labels TO scanner;
GRANT ALL PRIVILEGES ON TABLE tb_labels TO trade_user;

-- Switch back to default
\c postgres;

-- Log completion
SELECT 'PostgreSQL initialization completed successfully' as status;BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    arm                 text NOT NULL,
    exposure_n          integer NOT NULL,
    result_n            integer NOT NULL,
    feedback_n          integer NOT NULL,
    avg_quality         double precision NOT NULL,
    avg_usefulness      double precision NOT NULL,
    accepted_rate       double precision NOT NULL,
    result_coverage     double precision NOT NULL,
    feedback_coverage   double precision NOT NULL,
    coverage_multiplier double precision NOT NULL,
    score_raw           double precision NOT NULL,
    score               double precision NOT NULL,
    eligible            integer NOT NULL,
    reason_codes_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_scorecards(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions (
    id               bigserial PRIMARY KEY,
    ts_ms            bigint NOT NULL,
    decision         text NOT NULL,
    reason_code      text NOT NULL,
    winner_arm       text NOT NULL,
    incumbent_arm    text NOT NULL,
    winner_score     double precision NOT NULL,
    incumbent_score  double precision NOT NULL,
    decision_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions(ts_ms DESC);

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    current_mode        text NOT NULL,
    current_primary_arm text NOT NULL,
    target_mode         text NOT NULL,
    target_primary_arm  text NOT NULL,
    apply_strategy      text NOT NULL,
    winner_arm          text NOT NULL,
    winner_score        double precision NOT NULL,
    recommendation_json jsonb NOT NULL,
    evaluation_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    mode_before        text NOT NULL,
    primary_arm_before text NOT NULL,
    mode_after         text NOT NULL,
    primary_arm_after  text NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_journal_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_controller_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_verification_results (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    current_mode         text NOT NULL,
    current_primary_arm  text NOT NULL,
    target_mode          text NOT NULL,
    target_primary_arm   text NOT NULL,
    rollback_mode        text NOT NULL,
    rollback_primary_arm text NOT NULL,
    exposure_stats_json  jsonb NOT NULL,
    apply_event_json     jsonb NOT NULL,
    evaluation_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_verification_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    reason_code         text NOT NULL,
    mode_before         text NOT NULL,
    primary_arm_before  text NOT NULL,
    mode_after          text NOT NULL,
    primary_arm_after   text NOT NULL,
    rollback_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    window_min           integer NOT NULL,
    apply_requests       integer NOT NULL,
    applied_n            integer NOT NULL,
    verified_keep_n      integer NOT NULL,
    rollback_decisions_n integer NOT NULL,
    rollback_applied_n   integer NOT NULL,
    apply_rate           double precision NOT NULL,
    verify_keep_rate     double precision NOT NULL,
    rollback_mttr_p50_sec double precision NOT NULL,
    rollback_mttr_p95_sec double precision NOT NULL,
    rollback_mttr_samples integer NOT NULL,
    reason_codes_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    event_key            text NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    attempts             integer NOT NULL,
    rollback_mode        text NOT NULL,
    rollback_primary_arm text NOT NULL,
    result_json          jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations (
    id           bigserial PRIMARY KEY,
    ts_ms        bigint NOT NULL,
    severity     text NOT NULL,
    summary_json jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations_ts
    ON llm_route_incident_rca_mirror_rca_winner_apply_apply_escalations(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_incident_bundles (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    contour             text NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    trigger_reason_code text NOT NULL,
    bundle_json         jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_ts
    ON llm_governance_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_bundle_id
    ON llm_governance_incident_bundles(bundle_id);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_rca_bridge_decisions (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    route               text NOT NULL,
    destination_stream  text NOT NULL,
    bundle_json         jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_governance_rca_bridge_decisions_ts
    ON llm_governance_rca_bridge_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_governance_rca_bridge_decisions_bundle_id
    ON llm_governance_rca_bridge_decisions(bundle_id);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_vertex_rca_results (
    id            bigserial PRIMARY KEY,
    request_id    text NOT NULL,
    bundle_id     text NOT NULL,
    ts_ms         bigint NOT NULL,
    severity      text NOT NULL,
    provider_mode text NOT NULL,
    result_json   jsonb NOT NULL,
    request_json  jsonb NOT NULL,
    bundle_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_vertex_rca_results_ts
    ON llm_governance_vertex_rca_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_vertex_rca_feedback (
    id               bigserial PRIMARY KEY,
    request_id       text NOT NULL,
    bundle_id        text NOT NULL,
    ts_ms            bigint NOT NULL,
    quality_score    double precision NOT NULL,
    usefulness_score double precision NOT NULL,
    accepted         integer NOT NULL,
    reason_code      text NOT NULL,
    feedback_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_vertex_rca_feedback_ts
    ON llm_governance_vertex_rca_feedback(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_vertex_rca_feedback_rollups (
    id               bigserial PRIMARY KEY,
    ts_ms            bigint NOT NULL,
    window_min       integer NOT NULL,
    n                integer NOT NULL,
    avg_quality      double precision NOT NULL,
    avg_usefulness   double precision NOT NULL,
    accepted_rate    double precision NOT NULL,
    low_quality_rate double precision NOT NULL,
    rollup_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_vertex_rca_feedback_rollups_ts
    ON llm_governance_vertex_rca_feedback_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_vertex_rca_governance_decisions (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    target_bridge_mode text NOT NULL,
    decision_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_vertex_rca_governance_decisions_ts
    ON llm_governance_vertex_rca_governance_decisions(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_experiment_decisions (
    id                bigserial PRIMARY KEY,
    bundle_id         text NOT NULL,
    ts_ms             bigint NOT NULL,
    trigger_type      text NOT NULL,
    trigger_severity  text NOT NULL,
    decision          text NOT NULL,
    reason_code       text NOT NULL,
    primary_arm       text NOT NULL,
    shadow_arms_json  jsonb NOT NULL,
    bundle_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_experiment_decisions_ts
    ON llm_governance_experiment_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_experiment_exposures (
    id               bigserial PRIMARY KEY,
    bundle_id        text NOT NULL,
    ts_ms            bigint NOT NULL,
    trigger_type     text NOT NULL,
    trigger_severity text NOT NULL,
    arm              text NOT NULL,
    is_primary       integer NOT NULL,
    mode             text NOT NULL,
    exposure_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_experiment_exposures_ts
    ON llm_governance_experiment_exposures(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_scorecards (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    arm                 text NOT NULL,
    exposure_n          integer NOT NULL,
    result_n            integer NOT NULL,
    feedback_n          integer NOT NULL,
    avg_quality         double precision NOT NULL,
    avg_usefulness      double precision NOT NULL,
    accepted_rate       double precision NOT NULL,
    result_coverage     double precision NOT NULL,
    feedback_coverage   double precision NOT NULL,
    coverage_multiplier double precision NOT NULL,
    score_raw           double precision NOT NULL,
    score               double precision NOT NULL,
    eligible            integer NOT NULL,
    reason_codes_json   jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_scorecards_ts
    ON llm_governance_scorecards(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_evaluator_decisions (
    id               bigserial PRIMARY KEY,
    ts_ms            bigint NOT NULL,
    decision         text NOT NULL,
    reason_code      text NOT NULL,
    winner_arm       text NOT NULL,
    incumbent_arm    text NOT NULL,
    winner_score     double precision NOT NULL,
    incumbent_score  double precision NOT NULL,
    decision_json    jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_evaluator_decisions_ts
    ON llm_governance_evaluator_decisions(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_decisions (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    current_mode        text NOT NULL,
    current_primary_arm text NOT NULL,
    target_mode         text NOT NULL,
    target_primary_arm  text NOT NULL,
    apply_strategy      text NOT NULL,
    winner_arm          text NOT NULL,
    winner_score        double precision NOT NULL,
    recommendation_json jsonb NOT NULL,
    evaluation_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_decisions_ts
    ON llm_governance_apply_controller_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    mode_before        text NOT NULL,
    primary_arm_before text NOT NULL,
    mode_after         text NOT NULL,
    primary_arm_after  text NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_journal_ts
    ON llm_governance_apply_controller_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_decisions (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    current_mode        text NOT NULL,
    current_primary_arm text NOT NULL,
    target_mode         text NOT NULL,
    target_primary_arm  text NOT NULL,
    apply_strategy      text NOT NULL,
    winner_arm          text NOT NULL,
    winner_score        double precision NOT NULL,
    recommendation_json jsonb NOT NULL,
    evaluation_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_decisions_ts
    ON llm_governance_apply_controller_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    mode_before        text NOT NULL,
    primary_arm_before text NOT NULL,
    mode_after         text NOT NULL,
    primary_arm_after  text NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_journal_ts
    ON llm_governance_apply_controller_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_decisions (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    current_mode        text NOT NULL,
    current_primary_arm text NOT NULL,
    target_mode         text NOT NULL,
    target_primary_arm  text NOT NULL,
    apply_strategy      text NOT NULL,
    winner_arm          text NOT NULL,
    winner_score        double precision NOT NULL,
    recommendation_json jsonb NOT NULL,
    evaluation_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_decisions_ts
    ON llm_governance_apply_controller_decisions(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_apply_controller_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    decision           text NOT NULL,
    reason_code        text NOT NULL,
    mode_before        text NOT NULL,
    primary_arm_before text NOT NULL,
    mode_after         text NOT NULL,
    primary_arm_after  text NOT NULL,
    journal_json       jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_controller_journal_ts
    ON llm_governance_apply_controller_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_verification_results (
    id                  bigserial PRIMARY KEY,
    ts_ms               bigint NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    current_mode        text NOT NULL,
    current_primary_arm text NOT NULL,
    target_mode         text NOT NULL,
    target_primary_arm  text NOT NULL,
    rollback_mode       text NOT NULL,
    rollback_primary_arm text NOT NULL,
    exposure_stats_json jsonb NOT NULL,
    apply_event_json    jsonb NOT NULL,
    evaluation_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_verification_results_ts
    ON llm_governance_verification_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_rollback_journal (
    id                 bigserial PRIMARY KEY,
    ts_ms              bigint NOT NULL,
    reason_code        text NOT NULL,
    mode_before        text NOT NULL,
    primary_arm_before text NOT NULL,
    mode_after         text NOT NULL,
    primary_arm_after  text NOT NULL,
    rollback_json      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_rollback_journal_ts
    ON llm_governance_rollback_journal(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_slo_rollups (
    id                    bigserial PRIMARY KEY,
    ts_ms                 bigint NOT NULL,
    window_min            integer NOT NULL,
    apply_requests        integer NOT NULL,
    applied_n             integer NOT NULL,
    verified_keep_n       integer NOT NULL,
    rollback_decisions_n  integer NOT NULL,
    rollback_applied_n    integer NOT NULL,
    apply_rate            double precision NOT NULL,
    verify_keep_rate      double precision NOT NULL,
    rollback_mttr_p50_sec double precision NOT NULL,
    rollback_mttr_p95_sec double precision NOT NULL,
    rollback_mttr_samples integer NOT NULL,
    reason_codes_json     jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_slo_rollups_ts
    ON llm_governance_slo_rollups(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_retry_results (
    id                   bigserial PRIMARY KEY,
    ts_ms                bigint NOT NULL,
    event_key            text NOT NULL,
    decision             text NOT NULL,
    reason_code          text NOT NULL,
    attempts             integer NOT NULL,
    rollback_mode        text NOT NULL,
    rollback_primary_arm text NOT NULL,
    result_json          jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_retry_results_ts
    ON llm_governance_retry_results(ts_ms DESC);

CREATE TABLE IF NOT EXISTS llm_governance_escalations (
    id           bigserial PRIMARY KEY,
    ts_ms        bigint NOT NULL,
    severity     text NOT NULL,
    summary_json jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_governance_escalations_ts
    ON llm_governance_escalations(ts_ms DESC);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_incident_bundles (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    contour             text NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    trigger_reason_code text NOT NULL,
    bundle_json         jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_ts
    ON llm_governance_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_bundle_id
    ON llm_governance_incident_bundles(bundle_id);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_incident_bundles (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    contour             text NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    trigger_reason_code text NOT NULL,
    bundle_json         jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_ts
    ON llm_governance_incident_bundles(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_governance_incident_bundles_bundle_id
    ON llm_governance_incident_bundles(bundle_id);

COMMIT;


BEGIN;

CREATE TABLE IF NOT EXISTS llm_governance_apply_flow_rca_bridge_decisions (
    id                  bigserial PRIMARY KEY,
    bundle_id           text NOT NULL,
    ts_ms               bigint NOT NULL,
    trigger_type        text NOT NULL,
    trigger_severity    text NOT NULL,
    decision            text NOT NULL,
    reason_code         text NOT NULL,
    route               text NOT NULL,
    destination_stream  text NOT NULL,
    bundle_json         jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_flow_rca_bridge_decisions_ts
    ON llm_governance_apply_flow_rca_bridge_decisions(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_llm_governance_apply_flow_rca_bridge_decisions_bundle_id
    ON llm_governance_apply_flow_rca_bridge_decisions(bundle_id);

COMMIT;

