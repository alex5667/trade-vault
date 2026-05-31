-- migration: 20260530_01_signal_outcome_tb.sql
-- Phase 0: signal_outcome hypertable — triple-barrier labels with point-in-time feature snapshots.
--
-- Design rationale:
--   * decision_time_ms BIGINT (epoch ms) as partition key — matches project-wide epoch_ms convention.
--   * label NULL => open trade; resolver fills label once a barrier is hit.
--   * features JSONB = frozen at decision_time (zero look-ahead) — NOT re-read after the fact.
--   * entry_px = realistic fill (mid + half-spread + slip-prior), not mid.
--   * tp_r / sl_r / r_unit_px separate TP/SL config from absolute price math.
--   * DISTINCT from existing signal_outcomes (plural) table — that table uses is_win=r_multiple>=1.0
--     (post-hoc label). This table uses forward tick replay with proper barriers.
-- Added: Phase 0, 2026-05-30

CREATE TABLE IF NOT EXISTS signal_outcome (
    -- ── Identity ─────────────────────────────────────────────────────────────
    sid                TEXT             NOT NULL,
    decision_time_ms   BIGINT           NOT NULL,   -- epoch ms UTC; partition key
    ingest_time_ms     BIGINT           NOT NULL,
    schema_version     SMALLINT         NOT NULL DEFAULT 1,
    source             TEXT             NOT NULL,   -- handler/strategy id (e.g. crypto-of)
    symbol             TEXT             NOT NULL,
    side               SMALLINT         NOT NULL,   -- +1 long / -1 short
    trace_id           TEXT,
    kind               TEXT,                        -- signal kind (iceberg, delta_spike, …)

    -- ── Point-in-time decision context (frozen at decision_time_ms) ──────────
    features           JSONB            NOT NULL DEFAULT '{}',
    raw_score          DOUBLE PRECISION,            -- primary model score at decision
    calib_prob         DOUBLE PRECISION,            -- P(win) post-calibration (Phase 2)
    regime             TEXT,
    atr_bps            DOUBLE PRECISION,

    -- ── Triple-barrier config (frozen at decision_time_ms) ──────────────────
    ttl_ms             INTEGER          NOT NULL,   -- vertical barrier (horizon)
    tp_r               DOUBLE PRECISION NOT NULL,   -- upper barrier in R (e.g. 1.0)
    sl_r               DOUBLE PRECISION NOT NULL,   -- lower barrier in R (always 1.0)
    r_unit_px          DOUBLE PRECISION NOT NULL,   -- |SL| in price units = 1R
    entry_px           DOUBLE PRECISION NOT NULL,   -- realistic fill px (mid + ½spread + slip)

    -- ── Outcome (filled by resolver) ─────────────────────────────────────────
    resolved_time_ms   BIGINT,                      -- NULL => still open
    label              SMALLINT,                    -- +1 TP, -1 SL, 0 vertical; NULL => open
    realized_r         DOUBLE PRECISION,            -- realized outcome in R units
    realized_bps       DOUBLE PRECISION,            -- realized outcome in bps
    mfe_r              DOUBLE PRECISION,            -- max favorable excursion in R
    mae_r              DOUBLE PRECISION,            -- max adverse excursion in R

    -- ── Execution quality (Phase 4 fill-back) ───────────────────────────────
    expected_px        DOUBLE PRECISION,
    fill_px            DOUBLE PRECISION,
    exec_slippage_bps  DOUBLE PRECISION,
    fees_bps           DOUBLE PRECISION,

    -- ── Quality flags ────────────────────────────────────────────────────────
    quality_flags      INTEGER          NOT NULL DEFAULT 0,
    -- bit 0: tick data incomplete
    -- bit 1: entry_px estimated (no live spread)
    -- bit 2: resolved by timeout (no TP/SL hit)

    PRIMARY KEY (sid, decision_time_ms)
);

-- TimescaleDB hypertable — partition by epoch-ms bigint, 1-day chunks
SELECT create_hypertable(
    'signal_outcome',
    'decision_time_ms',
    chunk_time_interval => 86400000,   -- 1 day in ms
    if_not_exists        => TRUE
);

-- Integer-now function required for retention/CAGG on BIGINT time column
CREATE OR REPLACE FUNCTION now_ms() RETURNS BIGINT
    LANGUAGE SQL STABLE AS $$ SELECT (extract(epoch FROM now()) * 1000)::BIGINT $$;

SELECT set_integer_now_func('signal_outcome', 'now_ms', replace_if_exists => TRUE);

-- ── Indexes ──────────────────────────────────────────────────────────────────

-- Primary analytics query: by symbol+source ordered by time
CREATE INDEX IF NOT EXISTS ix_so_sym_src_time
    ON signal_outcome (symbol, source, decision_time_ms DESC);

-- Resolver: fast lookup of open (unlabelled) records
CREATE INDEX IF NOT EXISTS ix_so_open
    ON signal_outcome (symbol, decision_time_ms)
    WHERE label IS NULL;

-- Regime-slice analytics
CREATE INDEX IF NOT EXISTS ix_so_regime
    ON signal_outcome (source, regime, decision_time_ms DESC)
    WHERE label IS NOT NULL;

-- ── Retention + compression ───────────────────────────────────────────────────

-- Keep 365 days of raw data (drop older chunks)
SELECT add_retention_policy(
    'signal_outcome',
    drop_after      => 31536000000,   -- 365 days in ms
    if_not_exists   => TRUE
);

-- Compress chunks older than 7 days
ALTER TABLE signal_outcome SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,source'
);

SELECT add_compression_policy(
    'signal_outcome',
    compress_after => 604800000,     -- 7 days in ms
    if_not_exists  => TRUE
);
