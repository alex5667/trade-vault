-- 20260530_06 — confidence_meta_gate decisions audit (Plan 1, Phase 6).
--
-- Durable per-decision record so reviewers can replay any sid without
-- chasing Redis streams. Hypertable + continuous aggregate for the 1h
-- dashboard view; daily aggregate is added in a later migration once we
-- have stable bucket distribution.
--
-- Producer: services.confidence_meta_gate.metrics.emit_decision (via the
-- decision-stream persister sidecar, to be added once SHADOW data piles up).

CREATE TABLE IF NOT EXISTS confidence_meta_gate_decisions (
    ts                          TIMESTAMPTZ      NOT NULL,
    sid                         TEXT             NOT NULL,
    symbol                      TEXT             NOT NULL,
    kind                        TEXT             NOT NULL,
    side                        TEXT             NOT NULL,

    mode                        TEXT             NOT NULL,
    active                      BOOLEAN          NOT NULL,

    legacy_confidence           DOUBLE PRECISION NOT NULL,
    legacy_min_confidence       DOUBLE PRECISION NOT NULL,
    legacy_decision             TEXT             NOT NULL,

    meta_decision               TEXT             NOT NULL,
    active_decision             TEXT             NOT NULL,

    p_win_raw                   DOUBLE PRECISION,
    p_win_calibrated            DOUBLE PRECISION,
    p_win_floor                 DOUBLE PRECISION,

    expected_r                  DOUBLE PRECISION,
    expected_edge_bps           DOUBLE PRECISION,
    risk_multiplier             DOUBLE PRECISION,

    spread_bps                  DOUBLE PRECISION,
    expected_slippage_bps       DOUBLE PRECISION,
    fee_bps                     DOUBLE PRECISION,
    dq_score                    DOUBLE PRECISION,
    regime                      TEXT,
    session                     TEXT,

    model_ver                   TEXT             NOT NULL,
    schema_hash                 TEXT,
    feature_cols_hash           TEXT,

    canary_bucket               INTEGER,
    canary_selected             BOOLEAN,

    reason_codes                JSONB            NOT NULL DEFAULT '[]'::jsonb,
    features_small              JSONB            NOT NULL DEFAULT '{}'::jsonb,

    latency_ms                  DOUBLE PRECISION,
    created_at                  TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (ts, sid)
);

SELECT create_hypertable(
    'confidence_meta_gate_decisions',
    'ts',
    if_not_exists       => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_conf_meta_gate_sid
    ON confidence_meta_gate_decisions(sid);

CREATE INDEX IF NOT EXISTS idx_conf_meta_gate_symbol_ts
    ON confidence_meta_gate_decisions(symbol, ts DESC);

CREATE INDEX IF NOT EXISTS idx_conf_meta_gate_model_ts
    ON confidence_meta_gate_decisions(model_ver, ts DESC);

CREATE INDEX IF NOT EXISTS idx_conf_meta_gate_active_ts
    ON confidence_meta_gate_decisions(active, ts DESC)
    WHERE active IS TRUE;

-- 1-hour continuous aggregate: powers the "is meta-gate adding value?" board.
-- Bounded to 30 days raw retention via add_retention_policy below.
CREATE MATERIALIZED VIEW IF NOT EXISTS confidence_meta_gate_1h
    WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts) AS bucket,
    model_ver,
    mode,
    symbol,
    kind,
    legacy_decision,
    meta_decision,
    active_decision,
    COUNT(*)                                              AS n,
    AVG(p_win_calibrated)                                 AS avg_p_win,
    AVG(expected_r)                                       AS avg_expected_r,
    AVG(latency_ms)                                       AS avg_latency_ms,
    SUM(
        CASE WHEN legacy_decision <> meta_decision THEN 1 ELSE 0 END
    )::DOUBLE PRECISION / COUNT(*)                        AS disagreement_rate
FROM confidence_meta_gate_decisions
GROUP BY bucket, model_ver, mode, symbol, kind,
         legacy_decision, meta_decision, active_decision
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'confidence_meta_gate_1h',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists     => TRUE
);

-- Raw retention: 30 days is enough to debug a canary and audit a promotion.
SELECT add_retention_policy(
    'confidence_meta_gate_decisions',
    INTERVAL '30 days',
    if_not_exists => TRUE
);
