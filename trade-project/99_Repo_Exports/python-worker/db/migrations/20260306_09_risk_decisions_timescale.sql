-- P4.4/P4.5: SQL audit storage for risk decisions and latest snapshots (TimescaleDB).
-- These tables back the canary report and support post-hoc investigation of
-- risk-engine decisions (why a signal was denied / why notional was clamped).

-- Full immutable audit trail — one row per decision_id (upserted on replay).
create table if not exists risk_decisions (
  ts                       timestamptz not null default now(),
  decision_id              text not null,
  signal_id                text null,
  sid                      text null,
  symbol                   text not null,
  cluster                  text not null,
  tier                     text not null,
  level                    text not null,
  allow_trade_publish      boolean not null,
  effective_execution_policy text not null,
  requested_notional_usd   double precision not null,
  adjusted_notional_usd    double precision not null,
  leverage_cap             double precision not null,
  risk_multiplier          double precision not null,
  clamp_ratio              double precision not null default 0,
  decision_latency_ms      double precision not null default 0,
  reasons_jsonb            jsonb not null default '[]'::jsonb,
  snapshot_jsonb           jsonb not null default '{}'::jsonb,
  signal_jsonb             jsonb not null default '{}'::jsonb,
  
  unique (decision_id, ts)
);

-- TimescaleDB hypertable (time-partitioned by ts, 1-day chunks)
select create_hypertable('risk_decisions', 'ts', chunk_time_interval => interval '1 day', if_not_exists => true);

-- Retention: 180 days of outcome data
select add_retention_policy('risk_decisions', interval '180 days', if_not_exists => true);

-- Latest snapshot per decision_id (fast current-state lookup for the canary).
-- This acts as a rolling state table, upserted per decision.
create table if not exists risk_snapshot (
  ts                       timestamptz not null default now(),
  decision_id              text not null,
  sid                      text null,
  signal_id                text null,
  symbol                   text not null,
  cluster                  text not null,
  tier                     text not null,
  level                    text not null,
  effective_execution_policy text not null,
  adjusted_notional_usd    double precision not null,
  leverage_cap             double precision not null,
  clamp_ratio              double precision not null default 0,
  decision_latency_ms      double precision not null default 0,
  snapshot_jsonb           jsonb not null default '{}'::jsonb,
  
  unique (decision_id, ts)
);

-- TimescaleDB hypertable (time-partitioned by ts, 1-day chunks)
select create_hypertable('risk_snapshot', 'ts', chunk_time_interval => interval '1 day', if_not_exists => true);

-- Retention: 180 days of outcome data
select add_retention_policy('risk_snapshot', interval '180 days', if_not_exists => true);

-- P0: Fallback migration block for existing production data.
-- Since risk_decision_audit was replaced, any existing production data in risk_decision_audit
-- must be copied over to risk_decisions to prevent data loss.
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'risk_decision_audit') THEN
        INSERT INTO risk_decisions (
            ts, decision_id, signal_id, sid, symbol, cluster, tier, level,
            allow_trade_publish, effective_execution_policy, requested_notional_usd,
            adjusted_notional_usd, leverage_cap, risk_multiplier, clamp_ratio,
            decision_latency_ms, reasons_jsonb, snapshot_jsonb, signal_jsonb
        )
        SELECT 
            to_timestamp(created_ts_ms / 1000.0), -- preserve precise ms
            decision_id, signal_id, sid, symbol, cluster, tier, level,
            allow_trade_publish, effective_execution_policy, requested_notional_usd,
            adjusted_notional_usd, leverage_cap, risk_multiplier, clamp_ratio,
            decision_latency_ms, reasons_jsonb, snapshot_jsonb, signal_jsonb
        FROM risk_decision_audit
        ON CONFLICT (decision_id, ts) DO NOTHING;
    END IF;
END $$;
