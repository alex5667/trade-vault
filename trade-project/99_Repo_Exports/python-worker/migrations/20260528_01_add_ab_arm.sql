-- trades_closed: add entry_regime + ab_arm columns.
--
-- Problem: analytics_db.py INSERT references both columns but they are
-- missing from the live table, causing UndefinedColumn errors on every
-- save_trade_closed call since 2026-05-28.
--
-- entry_regime was added by migration 20260524_04 — idempotent here.
-- ab_arm is the A/B test arm label ('A' / 'B'), defaults to 'A'.
--
-- Added: 2026-05-28, migration 20260528_01

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS entry_regime TEXT,
    ADD COLUMN IF NOT EXISTS ab_arm       TEXT NOT NULL DEFAULT 'A';

CREATE INDEX IF NOT EXISTS ix_trades_closed_entry_regime
    ON trades_closed (entry_regime)
    WHERE entry_regime IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_trades_closed_ab_arm
    ON trades_closed (ab_arm);

COMMENT ON COLUMN trades_closed.entry_regime IS
    'Trading regime at signal-emit time (trending/ranging/choppy/…). NULL when unknown.';

COMMENT ON COLUMN trades_closed.ab_arm IS
    'A/B test arm for strategy experiments. Default A (control). Set via signal meta.ab_arm.';
