-- 20260522_04_add_p0_v_gate_reason.sql
-- v_gate_reason mirrors trades_closed.v_gate_reason for self-contained analytics joins.
-- Companion to 20260522_02_add_v_gate_reason.sql (which added it to trades_closed only).

ALTER TABLE trades_closed_p0
    ADD COLUMN IF NOT EXISTS v_gate_reason TEXT;

-- Backfill from trades_closed (best-effort, join on order_id)
UPDATE trades_closed_p0 p0
SET v_gate_reason = tc.v_gate_reason
FROM trades_closed tc
WHERE p0.order_id = tc.order_id
  AND p0.v_gate_reason IS NULL
  AND tc.v_gate_reason IS NOT NULL;
