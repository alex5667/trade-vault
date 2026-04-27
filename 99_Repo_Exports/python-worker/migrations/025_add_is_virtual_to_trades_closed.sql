-- migrations/025_add_is_virtual_to_trades_closed.sql

-- Add is_virtual to trades_closed
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS is_virtual BOOLEAN DEFAULT FALSE;

-- Add is_virtual to trades_closed_p0 (for shadow analytics)
ALTER TABLE trades_closed_p0 ADD COLUMN IF NOT EXISTS is_virtual BOOLEAN DEFAULT FALSE;
