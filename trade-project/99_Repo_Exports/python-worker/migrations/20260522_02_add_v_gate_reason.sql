-- Migration 058: add v_gate_reason to trades_closed
-- Added 2026-05-22: gate veto reason from signal validation_reason/gate_reason
ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS v_gate_reason TEXT;
