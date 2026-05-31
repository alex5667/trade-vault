-- migration: 20260530_05_signal_gated_out_outcomes.sql
-- Plan 2 / Gap 2: durable Timescale hypertable for confidence-gated-out signals.
--
-- Mirrors stream:signals:gated_out_outcomes (Redis) → SQL so passed/rejected/
-- gated_out outcomes share a queryable join surface. Replaces the prior
-- "Redis-only" design, which capped lifetime at MAXLEN=200k entries.
--
-- Schema rationale:
--   * (sid, ts_ms) PRIMARY KEY — gated_out signals can be re-emitted (PEL replay),
--     INSERT path uses ON CONFLICT DO NOTHING so the persister is idempotent.
--   * label: +1 TP_HIT, 0 TIMEOUT, -1 SL_HIT — matches signal_outcome.label.
--   * realized_r / ret_bps / cost_bps split mirrors the Redis payload (v=2).
--   * sample_policy + selection_weight / selection_prob propagate the v2 ML
--     training metadata so IPS-corrected datasets can be built off this table
--     without re-reading the Redis stream.
-- Added: Plan 2 Gap 2 (2026-05-30)

CREATE TABLE IF NOT EXISTS signal_gated_out_outcomes (
    -- ── Identity ─────────────────────────────────────────────────────────────
    sid                       TEXT             NOT NULL,
    ts_ms                     BIGINT           NOT NULL,   -- decision time (entry); partition key
    ts_close_ms               BIGINT,                      -- when barrier/horizon resolved
    symbol                    TEXT             NOT NULL,
    direction                 SMALLINT         NOT NULL,   -- +1 LONG / -1 SHORT
    kind                      TEXT,                        -- signal kind, optional
    schema_version            SMALLINT         NOT NULL DEFAULT 2,

    -- ── Decision-time inputs (frozen) ────────────────────────────────────────
    entry_px                  DOUBLE PRECISION NOT NULL,
    tp_bps                    DOUBLE PRECISION NOT NULL,
    sl_bps                    DOUBLE PRECISION NOT NULL,
    horizon_ms                INTEGER          NOT NULL,
    confidence                DOUBLE PRECISION,
    min_conf                  DOUBLE PRECISION,

    -- ── Outcome (filled by tracker, persisted as-is) ─────────────────────────
    outcome                   TEXT             NOT NULL,   -- 'TP_HIT' | 'SL_HIT' | 'TIMEOUT'
    label                     SMALLINT         NOT NULL,   -- +1 / 0 / -1
    close_price               DOUBLE PRECISION,
    high_px                   DOUBLE PRECISION,
    low_px                    DOUBLE PRECISION,
    ret_bps                   DOUBLE PRECISION,
    r_mult                    DOUBLE PRECISION,
    tp_hit                    SMALLINT         NOT NULL DEFAULT 0,
    sl_hit                    SMALLINT         NOT NULL DEFAULT 0,

    -- ── Cost-aware label (from tracker v2 payload) ───────────────────────────
    y_edge_cost_aware         SMALLINT,
    cost_bps                  DOUBLE PRECISION,
    cost_fees_bps             DOUBLE PRECISION,
    cost_spread_bps           DOUBLE PRECISION,
    cost_slippage_bps         DOUBLE PRECISION,
    edge_after_cost_bps       DOUBLE PRECISION,

    -- ── v2 ML training metadata (IPS reweighting) ────────────────────────────
    sample_policy             TEXT,
    selection_policy_version  TEXT,
    selection_prob            DOUBLE PRECISION,
    selection_weight          DOUBLE PRECISION,
    virtual_min_conf          DOUBLE PRECISION,
    meets_virtual_threshold   SMALLINT,

    -- ── Bookkeeping ──────────────────────────────────────────────────────────
    ingest_time_ms            BIGINT           NOT NULL,
    PRIMARY KEY (sid, ts_ms)
);

-- TimescaleDB hypertable — same epoch-ms convention as signal_outcome
SELECT create_hypertable(
    'signal_gated_out_outcomes',
    'ts_ms',
    chunk_time_interval => 86400000,   -- 1 day in ms
    if_not_exists        => TRUE
);

SELECT set_integer_now_func(
    'signal_gated_out_outcomes', 'now_ms', replace_if_exists => TRUE
);

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS ix_sgo_sym_kind_time
    ON signal_gated_out_outcomes (symbol, kind, ts_ms DESC);

CREATE INDEX IF NOT EXISTS ix_sgo_outcome
    ON signal_gated_out_outcomes (outcome, ts_ms DESC);

-- Cost-aware positives — fast lookup for "did the gate miss real edge?"
CREATE INDEX IF NOT EXISTS ix_sgo_cost_aware_pos
    ON signal_gated_out_outcomes (symbol, ts_ms DESC)
    WHERE y_edge_cost_aware = 1;

-- ── Retention + compression ──────────────────────────────────────────────────
-- 90 days raw is enough for gate-quality A/B; passed-side keeps 365d.
SELECT add_retention_policy(
    'signal_gated_out_outcomes',
    drop_after      => 7776000000,    -- 90 days in ms
    if_not_exists   => TRUE
);

ALTER TABLE signal_gated_out_outcomes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,kind'
);

SELECT add_compression_policy(
    'signal_gated_out_outcomes',
    compress_after => 604800000,      -- 7 days in ms
    if_not_exists  => TRUE
);

-- ── Unified outcome view ─────────────────────────────────────────────────────
-- Single query surface for passed-vs-gated_out reports. The two source tables
-- have different column shapes; this view normalises to a common schema.
--
-- pipeline_state:
--   'passed'     — signal made it through gates → signal_outcome
--   'gated_out'  — confidence gate vetoed → signal_gated_out_outcomes
--
-- Note: outcome and label semantics align (+1 TP, 0 timeout, -1 SL) so cross-
-- pipeline EV / win-rate / r-multiple aggregates are direct.

CREATE OR REPLACE VIEW signal_outcome_unified AS
SELECT
    'passed'::text             AS pipeline_state,
    sid,
    decision_time_ms           AS ts_ms,
    resolved_time_ms           AS ts_close_ms,
    symbol,
    side                       AS direction,
    kind,
    entry_px,
    NULL::double precision     AS tp_bps,    -- passed-side uses tp_r/sl_r in price units
    NULL::double precision     AS sl_bps,
    ttl_ms                     AS horizon_ms,
    label,
    CASE
        WHEN label = 1  THEN 'TP_HIT'
        WHEN label = -1 THEN 'SL_HIT'
        WHEN label = 0  THEN 'TIMEOUT'
        ELSE NULL
    END                        AS outcome,
    realized_r,
    realized_bps               AS ret_bps,
    calib_prob,
    NULL::smallint             AS y_edge_cost_aware,
    NULL::double precision     AS cost_bps,
    NULL::text                 AS sample_policy,
    NULL::double precision     AS selection_weight
FROM signal_outcome
WHERE label IS NOT NULL

UNION ALL

SELECT
    'gated_out'::text          AS pipeline_state,
    sid,
    ts_ms,
    ts_close_ms,
    symbol,
    direction,
    kind,
    entry_px,
    tp_bps,
    sl_bps,
    horizon_ms,
    label,
    outcome,
    r_mult                     AS realized_r,
    ret_bps,
    NULL::double precision     AS calib_prob,
    y_edge_cost_aware,
    cost_bps,
    sample_policy,
    selection_weight
FROM signal_gated_out_outcomes;
