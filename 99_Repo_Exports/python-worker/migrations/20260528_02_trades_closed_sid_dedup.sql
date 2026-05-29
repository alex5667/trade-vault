-- 2026-05-28 — P0: устранить двойную запись trades_closed по одному sid.
--
-- Контекст: за 24h 119 уникальных sid → 238 rows (ровно ×2). Корневая причина —
-- два инстанса scanner-trade-monitor параллельно открывают и закрывают позицию
-- по одному signal, генерируя разные order_id (pos.id, UUID). Все downstream-
-- калибраторы (p_edge, slippage, IPS, ECE/Brier, post-SL, ML datasets)
-- получают 2× labels на одно событие.
--
-- Шаги:
--   1) Dedupe существующих финальных дублей: оставить min(id) на каждый sid.
--   2) Создать partial UNIQUE INDEX (sid) WHERE is_final_close = true.
--
-- Безопасность: индекс CONCURRENTLY не блокирует таблицу.
-- Откат: DROP INDEX idx_trades_closed_sid_final_uniq.

BEGIN;

-- Сохранить дубли в архивную таблицу до удаления (на случай разбора).
CREATE TABLE IF NOT EXISTS trades_closed_sid_dup_archive_2026_05_28 (
    LIKE trades_closed INCLUDING DEFAULTS
);

INSERT INTO trades_closed_sid_dup_archive_2026_05_28
SELECT *
FROM trades_closed
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY sid ORDER BY id) AS rn
        FROM trades_closed
        WHERE is_final_close = true
          AND sid IS NOT NULL
          AND sid <> ''
    ) t
    WHERE rn > 1
);

DELETE FROM trades_closed
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY sid ORDER BY id) AS rn
        FROM trades_closed
        WHERE is_final_close = true
          AND sid IS NOT NULL
          AND sid <> ''
    ) t
    WHERE rn > 1
);

COMMIT;

-- Partial unique index гарантирует идемпотентность по sid на DB-уровне.
-- CONCURRENTLY — без блокировки таблицы (выносится из транзакции).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_sid_final_uniq
ON trades_closed (sid)
WHERE is_final_close = true
  AND sid IS NOT NULL
  AND sid <> '';
