-- Migration 031: Add P41 Native Trade Meta fields
-- Adds meta_enforce_cov_bucket and meta_enforce_applied to trades_closed and trades_closed_p0

DO $$
BEGIN
    -- Add columns to trades_closed
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades_closed' AND column_name = 'meta_enforce_cov_bucket') THEN
        ALTER TABLE trades_closed ADD COLUMN meta_enforce_cov_bucket TEXT DEFAULT '';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades_closed' AND column_name = 'meta_enforce_applied') THEN
        ALTER TABLE trades_closed ADD COLUMN meta_enforce_applied INTEGER DEFAULT -1;
    END IF;

    -- Add columns to trades_closed_p0
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades_closed_p0' AND column_name = 'meta_enforce_cov_bucket') THEN
        ALTER TABLE trades_closed_p0 ADD COLUMN meta_enforce_cov_bucket TEXT DEFAULT '';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades_closed_p0' AND column_name = 'meta_enforce_applied') THEN
        ALTER TABLE trades_closed_p0 ADD COLUMN meta_enforce_applied INTEGER DEFAULT -1;
    END IF;
END $$;
