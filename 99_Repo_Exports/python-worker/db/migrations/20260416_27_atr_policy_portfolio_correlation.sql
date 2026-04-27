-- Migration 20260416_27_atr_policy_portfolio_correlation.sql
-- Description: Phase 5.6 portfolio concentration and correlation tables

CREATE TABLE IF NOT EXISTS atr_policy_factor_clusters (
  id bigserial PRIMARY KEY,
  symbol text NOT NULL,
  factor_cluster text NOT NULL,
  beta_leader text,
  updated_at_ms bigint NOT NULL,
  UNIQUE(symbol)
);

CREATE TABLE IF NOT EXISTS atr_policy_symbol_correlations (
  id bigserial PRIMARY KEY,
  symbol_a text NOT NULL,
  symbol_b text NOT NULL,
  corr_ewma double precision NOT NULL,
  horizon_sec integer NOT NULL,
  regime text NOT NULL DEFAULT 'all',
  updated_at_ms bigint NOT NULL,
  UNIQUE(symbol_a, symbol_b, horizon_sec, regime)
);

CREATE TABLE IF NOT EXISTS atr_policy_portfolio_limits (
  id bigserial PRIMARY KEY,
  scope_kind text NOT NULL,                -- global | venue | factor_cluster | symbol | policy_ver
  venue text,
  symbol text,
  factor_cluster text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  max_open_risk_pct double precision NOT NULL DEFAULT 0,
  max_same_side_risk_pct double precision NOT NULL DEFAULT 0,
  max_factor_cluster_risk_pct double precision NOT NULL DEFAULT 0,
  max_venue_risk_pct double precision NOT NULL DEFAULT 0,
  max_policy_risk_pct double precision NOT NULL DEFAULT 0,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_policy_portfolio_events (
  id bigserial PRIMARY KEY,
  source text,
  venue text,
  symbol text,
  factor_cluster text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  action text NOT NULL,                    -- allow | deny | clip | freeze
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
