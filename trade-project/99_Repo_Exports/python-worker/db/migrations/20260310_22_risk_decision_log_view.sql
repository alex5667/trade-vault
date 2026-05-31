-- Migration 20260310_22: Add compatibility view risk_decision_log
-- Maps risk_decisions to the expected interface used by monitoring scripts.
-- Exposes: confidence (from signal_jsonb), created_at (from created_ts_ms).

CREATE OR REPLACE VIEW risk_decision_log AS
SELECT
    decision_id,
    signal_id,
    sid,
    symbol,
    cluster,
    tier,
    level,
    allow_trade_publish,
    effective_execution_policy,
    requested_notional_usd,
    adjusted_notional_usd,
    leverage_cap,
    risk_multiplier,
    clamp_ratio,
    decision_latency_ms,
    reasons_jsonb,
    snapshot_jsonb,
    signal_jsonb,
    -- confidence ratio [0..1] extracted from signal_jsonb; fallback to risk_multiplier/2
    COALESCE(
        (signal_jsonb->>'confidence')::double precision,
        risk_multiplier / 2.0
    ) AS confidence,
    -- created_at: timestamptz (ts is already TIMESTAMPTZ in hypertable)
    ts AS created_at,
    -- created_ts_ms: backing epoch ms for legacy queries
    trunc(extract(epoch from ts) * 1000)::bigint AS created_ts_ms
FROM risk_decisions;
