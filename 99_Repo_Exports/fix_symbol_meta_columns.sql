-- Миграционный скрипт для исправления регистра столбцов в symbol_meta
-- Запускать в базе данных 'trade'

-- Подключиться к базе trade
\c trade;

-- 1. Добавить новые закавыченные столбцы
ALTER TABLE symbol_meta ADD COLUMN IF NOT EXISTS "minPrice" DOUBLE PRECISION;
ALTER TABLE symbol_meta ADD COLUMN IF NOT EXISTS "maxPrice" DOUBLE PRECISION;

-- 2. Скопировать данные из старых столбцов в новые
UPDATE symbol_meta SET "minPrice" = minprice WHERE minprice IS NOT NULL;
UPDATE symbol_meta SET "maxPrice" = maxprice WHERE maxprice IS NOT NULL;

-- 3. Удалить старые незакавыченные столбцы
ALTER TABLE symbol_meta DROP COLUMN IF EXISTS minprice;
ALTER TABLE symbol_meta DROP COLUMN IF EXISTS maxprice;

-- 4. Проверить результат
SELECT 'Миграция завершена успешно' as status;
SELECT column_name FROM information_schema.columns
WHERE table_name = 'symbol_meta' AND table_schema = 'public'
ORDER BY ordinal_position;
