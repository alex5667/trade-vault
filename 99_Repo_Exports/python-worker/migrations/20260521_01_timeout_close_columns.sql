-- Migration: timeout_close columns + orphan cleanup flags (2026-05-21)
-- Adds two independent mechanism columns:
--   A) is_orphan_cleanup / exclude_from_ml_labels  → housekeep state cleanup
--   B) timeout_age_ms / timeout_max_hold_ms / ...  → real max-hold timeout exit
-- All idempotent (ADD COLUMN IF NOT EXISTS).

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS is_orphan_cleanup          BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS exclude_from_ml_labels     BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS timeout_age_ms             BIGINT,
    ADD COLUMN IF NOT EXISTS timeout_max_hold_ms        BIGINT,
    ADD COLUMN IF NOT EXISTS timeout_request_ts_ms      BIGINT,
    ADD COLUMN IF NOT EXISTS timeout_close_latency_ms   BIGINT,
    ADD COLUMN IF NOT EXISTS exit_order_ref             TEXT,
    ADD COLUMN IF NOT EXISTS closed_trade_id            TEXT;

-- close_reason_raw / close_reason_detail / exit_policy already existed;
-- add them idempotently in case an older schema is missing them.
ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS close_reason_raw           TEXT,
    ADD COLUMN IF NOT EXISTS close_reason_detail        TEXT,
    ADD COLUMN IF NOT EXISTS exit_policy                TEXT;

-- ── Indexes ──────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_trades_closed_reason_raw_time
    ON trades_closed (close_reason_raw, exit_ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_trades_closed_timeout
    ON trades_closed (symbol, exit_ts_ms DESC)
    WHERE close_reason_raw LIKE 'TIMEOUT_%';

CREATE INDEX IF NOT EXISTS idx_trades_closed_ml_clean
    ON trades_closed (symbol, exit_ts_ms DESC)
    WHERE exclude_from_ml_labels = false;

-- ── Backfill: existing ORPHAN_TIMEOUT* rows → is_orphan_cleanup ──────
-- Safe to run multiple times (WHERE guards prevent double-update).
UPDATE trades_closed
    SET is_orphan_cleanup      = true,
        exclude_from_ml_labels = true
    WHERE close_reason_raw IN (
        'ORPHAN_TIMEOUT',
        'ORPHAN_TIMEOUT_NO_PRICE',
        'ORPHAN_TIMEOUT_STALE_PRICE',
        'ORPHAN_CLEANUP_STALE_MONITOR_STATE',
        'ORPHAN_CLEANUP_NO_PRICE',
        'ORPHAN_CLEANUP_STALE_PRICE'
    )
    AND is_orphan_cleanup = false;
