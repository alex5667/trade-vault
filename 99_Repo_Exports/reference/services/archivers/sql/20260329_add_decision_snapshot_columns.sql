-- timescaledb: 2026xxxx_add_decision_snapshot_columns.sql
-- Извлечение метрик новых гейтов из JSONB поля `extra` в `decision_snapshot` (для ускорения аналитики).

ALTER TABLE decision_snapshot
ADD COLUMN IF NOT EXISTS liquidity_regime TEXT DEFAULT '',
ADD COLUMN IF NOT EXISTS book_stale_ms INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_slippage_bps DOUBLE PRECISION NULL;

-- Опционально: перенос данных из extra
-- UPDATE decision_snapshot
-- SET 
--     liquidity_regime = CAST(extra->>'liquidity_regime' AS TEXT),
--     book_stale_ms = CAST(extra->>'book_stale_ms' AS INTEGER),
--     expected_slippage_bps = CAST(extra->>'expected_slippage_bps' AS DOUBLE PRECISION)
-- WHERE extra IS NOT NULL;
