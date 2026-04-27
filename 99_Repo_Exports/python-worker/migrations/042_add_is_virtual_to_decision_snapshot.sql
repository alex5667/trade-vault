-- migrations/042_add_is_virtual_to_decision_snapshot.sql
-- Add is_virtual to decision_snapshot

ALTER TABLE decision_snapshot ADD COLUMN IF NOT EXISTS is_virtual BOOLEAN DEFAULT FALSE;
