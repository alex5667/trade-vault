-- 20260522_03_add_p0_strong_gate_ok.sql
-- Added in phase audit-null-cols, migration 20260522_03, 2026-05-22
-- strong_gate_ok was missing from trades_closed_p0 INSERT but present in trades_closed

ALTER TABLE trades_closed_p0
    ADD COLUMN IF NOT EXISTS strong_gate_ok BOOLEAN;

-- Backfill from trades_closed (join on order_id which is unique per close)
UPDATE trades_closed_p0 p0
SET strong_gate_ok = tc.strong_gate_ok
FROM trades_closed tc
WHERE p0.order_id = tc.order_id
  AND p0.strong_gate_ok IS NULL
  AND tc.strong_gate_ok IS NOT NULL;
