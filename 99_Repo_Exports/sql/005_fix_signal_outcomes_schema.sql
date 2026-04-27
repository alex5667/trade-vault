-- sql/005_fix_signal_outcomes_schema.sql
-- Run this script if you see "column trace_id does not exist" errors
-- even though the table exists.

\c scanner_analytics

DO $$ 
BEGIN 
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='signal_outcomes' AND column_name='trace_id'
    ) THEN
        ALTER TABLE signal_outcomes ADD COLUMN trace_id TEXT DEFAULT '';
        RAISE NOTICE 'Column trace_id added to signal_outcomes';
    ELSE
        RAISE NOTICE 'Column trace_id already exists in signal_outcomes';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name='signal_outcomes' AND column_name='event_id'
    ) THEN
        ALTER TABLE signal_outcomes ADD COLUMN event_id TEXT DEFAULT '';
        RAISE NOTICE 'Column event_id added to signal_outcomes';
    ELSE
        RAISE NOTICE 'Column event_id already exists in signal_outcomes';
    END IF;
END $$;

-- Verify
\d signal_outcomes
