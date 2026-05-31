-- scratch/backfill_nulls.sql
-- Backfill script to eliminate systemic NULLs in historical records

-- 1. ml_predictions: backfill p_margin, p_min with 0.0 (safe default for older model versions)
UPDATE ml_predictions
SET p_margin = 0.0
WHERE p_margin IS NULL;

UPDATE ml_predictions
SET p_min = 0.0
WHERE p_min IS NULL;

-- 2. signals: backfill atr_1m, obi, weak_progress
UPDATE signals
SET atr_1m = 0.0
WHERE atr_1m IS NULL;

UPDATE signals
SET obi = 0.0
WHERE obi IS NULL;

UPDATE signals
SET weak_progress = 0.0
WHERE weak_progress IS NULL;
