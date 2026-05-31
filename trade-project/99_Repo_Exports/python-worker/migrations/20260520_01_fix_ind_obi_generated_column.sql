-- Migration: fix ind_obi generated column (2026-05-20)
-- The original expression read config_json->'indicators'->>'obi' which never exists.
-- Real key in the indicators JSON is 'obi_z' (z-score, always present for CryptoOrderFlow).
-- The column is INCLUDED in idx_trades_closed_ml_v2; Postgres rebuilds it automatically.

ALTER TABLE trades_closed DROP COLUMN IF EXISTS ind_obi;

ALTER TABLE trades_closed
    ADD COLUMN ind_obi double precision
    GENERATED ALWAYS AS (
        ((config_json -> 'indicators'::text) ->> 'obi_z'::text)::double precision
    ) STORED;

-- Recreate the ML index with the fixed column
DROP INDEX IF EXISTS idx_trades_closed_ml_v2;
CREATE INDEX idx_trades_closed_ml_v2
    ON trades_closed (exit_ts DESC, symbol, entry_tag)
    INCLUDE (r_multiple, ind_delta_z, ind_obi, ind_weak_progress, ind_atr_th_bps)
    WHERE r_multiple IS NOT NULL
      AND (tp1_hit = true OR r_multiple > 0);
