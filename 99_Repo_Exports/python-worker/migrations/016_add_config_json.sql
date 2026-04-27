-- Migration: Add config_json to trades_closed
-- Date: 2026-01-15

\c scanner_analytics;

ALTER TABLE trades_closed
ADD COLUMN IF NOT EXISTS config_json JSONB;
