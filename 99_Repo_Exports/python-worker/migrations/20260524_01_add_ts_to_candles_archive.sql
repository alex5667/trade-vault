-- Migration 20260524_01: Add canonical event timestamp to candles_archive.
-- Purpose: make generic freshness/fill checks work through a standard `ts` column.
-- Semantics: `ts` is the candle event time and mirrors `close_time` (TIMESTAMPTZ/UTC).

ALTER TABLE candles_archive
    ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ;

CREATE OR REPLACE FUNCTION set_candles_archive_ts()
RETURNS TRIGGER AS $$
BEGIN
    NEW.ts := NEW.close_time;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_candles_archive_ts ON candles_archive;
CREATE TRIGGER trg_set_candles_archive_ts
BEFORE INSERT OR UPDATE OF close_time, ts ON candles_archive
FOR EACH ROW
EXECUTE FUNCTION set_candles_archive_ts();

UPDATE candles_archive
SET ts = close_time
WHERE ts IS NULL
  AND close_time IS NOT NULL;

ALTER TABLE candles_archive
    ALTER COLUMN ts SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_candles_archive_ts
    ON candles_archive (ts DESC) WHERE ts IS NOT NULL;

COMMENT ON COLUMN candles_archive.ts IS
    'Canonical candle event timestamp for freshness checks; mirrors close_time in UTC.';
