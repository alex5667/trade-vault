-- Добавление колонки strategy в таблицу trades_closed
-- Выполнить в scanner_analytics базе данных

ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS strategy TEXT NOT NULL DEFAULT '';

-- Проверка
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'trades_closed' AND column_name = 'strategy';
