-- trades_closed: add entry_regime column + backfill from signal_snapshots.
--
-- Problem: live audit shows 94% of trades:closed have market_regime ∈ {"na","*"}.
-- Root cause: trade-monitor → analytics_db pipeline never propagated trading regime
-- (only ATR regimes). Per-regime ML models and calibrators are blocked without it.
--
-- This migration:
--   1. Adds `entry_regime TEXT` column (NULL when unknown — calibrators handle that)
--   2. Backfills from signal_snapshots.regime by sid match for rows where
--      entry_regime IS NULL.
--
-- Forward fix: analytics_db.py INSERT must include entry_regime (see code edit).
--
-- Added: 2026-05-24, migration 20260524_04

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS entry_regime TEXT;

CREATE INDEX IF NOT EXISTS ix_trades_closed_entry_regime
    ON trades_closed (entry_regime)
    WHERE entry_regime IS NOT NULL;

-- Backfill: for each trades_closed row with NULL entry_regime, find matching
-- signal_snapshots.regime by sid. Normalised sid lookup:
--   trades_closed.sid → may carry "of:SYM:TS" or "of:SYM:TS:DIR" or "crypto-of:..."
--   signal_snapshots.sid → same variants
-- Normalise both to "SYMBOL:TS" form before joining.

WITH normalised AS (
    SELECT
        tc.sid AS tc_sid,
        CASE
            WHEN tc.sid LIKE 'crypto-of:%' THEN
                upper(split_part(substring(tc.sid from 11), ':', 1))
                || ':' || split_part(substring(tc.sid from 11), ':', 2)
            WHEN tc.sid ~ '^[a-z][a-z_-]*:[A-Z0-9]+:[0-9]+' THEN
                split_part(tc.sid, ':', 2)
                || ':' || split_part(tc.sid, ':', 3)
            ELSE NULL
        END AS norm_key
    FROM trades_closed tc
    WHERE tc.entry_regime IS NULL
      AND tc.exit_ts_ms IS NOT NULL
),
snap_keys AS (
    SELECT DISTINCT ON (norm_key)
        norm_key,
        regime
    FROM (
        SELECT
            CASE
                WHEN ss.sid LIKE 'crypto-of:%' THEN
                    upper(split_part(substring(ss.sid from 11), ':', 1))
                    || ':' || split_part(substring(ss.sid from 11), ':', 2)
                WHEN ss.sid ~ '^[a-z][a-z_-]*:[A-Z0-9]+:[0-9]+' THEN
                    split_part(ss.sid, ':', 2)
                    || ':' || split_part(ss.sid, ':', 3)
                ELSE NULL
            END AS norm_key,
            regime
        FROM signal_snapshots ss
        WHERE ss.regime IS NOT NULL
          AND ss.regime NOT IN ('na', 'unknown', 'none', 'null', '')
    ) inner_q
    WHERE norm_key IS NOT NULL
    ORDER BY norm_key, regime  -- deterministic when sid appears twice
)
UPDATE trades_closed tc
SET entry_regime = sk.regime
FROM normalised n
JOIN snap_keys sk ON sk.norm_key = n.norm_key
WHERE tc.sid = n.tc_sid
  AND tc.entry_regime IS NULL;

-- Report
DO $$
DECLARE
    n_total INTEGER;
    n_with_regime INTEGER;
BEGIN
    SELECT count(*) INTO n_total FROM trades_closed
    WHERE exit_ts_ms >= (extract(epoch from now()) - 30*86400)::bigint * 1000;
    SELECT count(*) INTO n_with_regime FROM trades_closed
    WHERE exit_ts_ms >= (extract(epoch from now()) - 30*86400)::bigint * 1000
      AND entry_regime IS NOT NULL;
    RAISE NOTICE 'trades_closed last 30d: % rows; % have entry_regime (%.1f%%)',
        n_total, n_with_regime,
        100.0 * n_with_regime / GREATEST(n_total, 1);
END $$;

COMMENT ON COLUMN trades_closed.entry_regime IS
    'Trading regime at signal-emit time. Joined from signal_snapshots by sid. NULL when signal_snapshots record missing or regime was unknown at emit time.';
