-- migration: 20260530_03_signal_feature_snapshots.sql
-- Step 1 of Plan 3 — canonical immutable point-in-time FeatureSnapshot store.
--
-- Why separate from signal_outcome:
--   * signal_outcome mixes decision-time (features, entry_px, tp_r) and outcome-time
--     fields (label, realized_r, fill_px). It is mutable — resolver UPDATEs it.
--   * signal_feature_snapshots is APPEND-ONLY: every row is frozen at decision_time
--     and never updated. Joined to signal_outcome / order_execution_events by sid.
--   * Carries schema_hash + feature_cols_hash so trainers can reject samples whose
--     feature schema changed mid-window (no silent skew).
--
-- Storage budget: roughly the same as signal_outcome (features JSONB is the heavy field)
-- — 180-day retention (vs 365 for outcome) keeps total footprint flat.
-- Added: Plan 3 / Step 1, 2026-05-30

CREATE TABLE IF NOT EXISTS signal_feature_snapshots (
    -- ── Identity / partition key ────────────────────────────────────────────
    decision_time_ms      BIGINT           NOT NULL,
    sid                   TEXT             NOT NULL,
    symbol                TEXT             NOT NULL,
    kind                  TEXT,
    side                  SMALLINT         NOT NULL,   -- +1 long / -1 short
    source                TEXT             NOT NULL,
    trace_id              TEXT,

    -- ── Schema fingerprint (immutable per training window) ─────────────────
    schema_name           TEXT             NOT NULL,
    schema_version        TEXT             NOT NULL,
    schema_hash           TEXT             NOT NULL,   -- sha1(sorted schema keys)
    feature_cols_hash     TEXT             NOT NULL,   -- sha1(sorted feature names)

    -- ── Timestamps (epoch ms) ───────────────────────────────────────────────
    event_time_ms         BIGINT,                       -- exchange event time
    ingest_time_ms        BIGINT,                       -- go-worker XADD time
    process_time_ms       BIGINT           NOT NULL,    -- python-worker decision time

    -- ── Submit-side execution context (immutable estimate) ──────────────────
    entry_px_expected     DOUBLE PRECISION,
    mid_px_submit         DOUBLE PRECISION,
    spread_bps_submit     DOUBLE PRECISION,
    expected_slippage_bps DOUBLE PRECISION,

    -- ── Payloads ────────────────────────────────────────────────────────────
    features              JSONB            NOT NULL DEFAULT '{}'::jsonb,
    dq_flags              JSONB            NOT NULL DEFAULT '[]'::jsonb,
    meta                  JSONB            NOT NULL DEFAULT '{}'::jsonb,

    created_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (decision_time_ms, sid)
);

SELECT create_hypertable(
    'signal_feature_snapshots',
    'decision_time_ms',
    chunk_time_interval => 86400000,   -- 1 day in ms
    if_not_exists       => TRUE
);

SELECT set_integer_now_func('signal_feature_snapshots', 'now_ms', replace_if_exists => TRUE);

-- ── Indexes ──────────────────────────────────────────────────────────────────

-- Join key for resolver / TCA / training pipelines
CREATE INDEX IF NOT EXISTS ix_sfs_sid
    ON signal_feature_snapshots (sid);

CREATE INDEX IF NOT EXISTS ix_sfs_sym_time
    ON signal_feature_snapshots (symbol, decision_time_ms DESC);

-- Schema drift detection: scan recent chunks by schema_hash
CREATE INDEX IF NOT EXISTS ix_sfs_schema_hash_time
    ON signal_feature_snapshots (schema_hash, decision_time_ms DESC);

-- ── Retention + compression ──────────────────────────────────────────────────

-- 180 days raw (training windows of 30d × 6 epochs)
SELECT add_retention_policy(
    'signal_feature_snapshots',
    drop_after    => 15552000000,   -- 180 days in ms
    if_not_exists => TRUE
);

-- Compress chunks older than 7 days
ALTER TABLE signal_feature_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,source,schema_hash'
);

SELECT add_compression_policy(
    'signal_feature_snapshots',
    compress_after => 604800000,    -- 7 days in ms
    if_not_exists  => TRUE
);
