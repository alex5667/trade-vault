-- Phase 2.7 (2026-05-17): close postgres ERROR storm
-- 1) trades_closed / trades_closed_p0 — add strong_gate_ok used by batch_trade_writer
-- 2) signals — add cols used by regime/signal_logger (session, regime, delta_spike_z, obi, weak_progress, raw_ctx)

ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS strong_gate_ok boolean;

ALTER TABLE trades_closed_p0
  ADD COLUMN IF NOT EXISTS strong_gate_ok boolean;

ALTER TABLE signals
  ADD COLUMN IF NOT EXISTS session       text,
  ADD COLUMN IF NOT EXISTS regime        text,
  ADD COLUMN IF NOT EXISTS delta_spike_z double precision,
  ADD COLUMN IF NOT EXISTS obi           double precision,
  ADD COLUMN IF NOT EXISTS weak_progress double precision,
  ADD COLUMN IF NOT EXISTS raw_ctx       jsonb;
