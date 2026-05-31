-- Migration 20260421_01: Increase numeric precision in candles_archive
-- Purpose: Support high volume values for MEME coins which exceed NUMERIC(20,8) limit of 10^12.

ALTER TABLE candles_archive
  ALTER COLUMN open TYPE NUMERIC(36, 8),
  ALTER COLUMN high TYPE NUMERIC(36, 8),
  ALTER COLUMN low TYPE NUMERIC(36, 8),
  ALTER COLUMN close TYPE NUMERIC(36, 8),
  ALTER COLUMN volume TYPE NUMERIC(36, 8),
  ALTER COLUMN quote_volume TYPE NUMERIC(36, 8),
  ALTER COLUMN taker_buy_base TYPE NUMERIC(36, 8),
  ALTER COLUMN taker_buy_quote TYPE NUMERIC(36, 8);
