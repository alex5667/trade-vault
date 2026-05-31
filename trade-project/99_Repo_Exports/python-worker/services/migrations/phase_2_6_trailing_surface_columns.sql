-- Phase 2.6: Add columns for Horizon-Aware ATR Trailing Canary

ALTER TABLE trades_closed 
ADD COLUMN IF NOT EXISTS trailing_surface_applied boolean DEFAULT false,
ADD COLUMN IF NOT EXISTS trailing_surface_reason_code text,
ADD COLUMN IF NOT EXISTS baseline_trailing_offset_atr double precision,
ADD COLUMN IF NOT EXISTS selected_trailing_offset_atr double precision;

ALTER TABLE trades_closed_p0
ADD COLUMN IF NOT EXISTS trailing_surface_applied boolean DEFAULT false,
ADD COLUMN IF NOT EXISTS trailing_surface_reason_code text,
ADD COLUMN IF NOT EXISTS baseline_trailing_offset_atr double precision,
ADD COLUMN IF NOT EXISTS selected_trailing_offset_atr double precision;
