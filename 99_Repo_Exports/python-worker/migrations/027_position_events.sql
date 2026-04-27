-- =============================================================================
-- Migration: 027_position_events.sql
-- Purpose: Долгосрочное хранение промежуточных событий позиций из events:trades
-- =============================================================================
-- Цель: timeline всех событий по позиции для:
-- - Анализ trailing moves (как далеко утащили)
-- - TP hit events
-- - Debugging закрытий
-- - Winrate/ROC анализ для trade_back
-- =============================================================================

CREATE TABLE IF NOT EXISTS position_events (
  -- Первичный ключ: stream_id из Redis (гарантирует идемпотентность)
  stream_id    TEXT PRIMARY KEY,
  
  -- Timestamp поля (epoch ms и UTC timestamp)
  ts_ms        BIGINT NOT NULL,
  ts           TIMESTAMPTZ NOT NULL,

  -- Идентификаторы позиции (MT5 использует position_id вместо order_id)
  position_id  TEXT,
  sid          TEXT,
  symbol       TEXT,

  -- Тип события (TP_HIT, TRAILING_MOVE, SL_ADJUST, POSITION_CLOSED, etc)
  event_type   TEXT NOT NULL,
  
  -- Metadata в виде JSONB (close_reason, trailing profile, etc)
  -- Важно: events:trades имеет поле meta как JSON string
  meta_json    JSONB,
  
  -- Полный payload события (JSONB для гибкого анализа)
  payload_json JSONB NOT NULL,

  -- Служебные поля
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Индексы для типичных запросов
-- =============================================================================

-- 1. Timeline по позиции (основной use-case)
CREATE INDEX IF NOT EXISTS position_events_position_ts_idx 
  ON position_events (position_id, ts DESC) 
  WHERE position_id IS NOT NULL;

-- 2. Фильтрация по типу события
CREATE INDEX IF NOT EXISTS position_events_type_ts_idx 
  ON position_events (event_type, ts DESC);

-- 3. Анализ по symbol + время
CREATE INDEX IF NOT EXISTS position_events_symbol_ts_idx 
  ON position_events (symbol, ts DESC) 
  WHERE symbol IS NOT NULL;

-- 4. JSONB индекс для гибких запросов
CREATE INDEX IF NOT EXISTS position_events_payload_gin_idx 
  ON position_events USING gin (payload_json);

CREATE INDEX IF NOT EXISTS position_events_meta_gin_idx 
  ON position_events USING gin (meta_json) 
  WHERE meta_json IS NOT NULL;

-- =============================================================================
-- Примеры запросов для аналитики
-- =============================================================================
-- 
-- 1. Timeline всех событий по позиции:
--    SELECT ts, event_type, 
--           payload_json->>'price' as price,
--           payload_json->>'new_sl' as new_sl,
--           meta_json->>'close_reason' as close_reason
--    FROM position_events
--    WHERE position_id = '12345678'
--    ORDER BY ts;
--
-- 2. Анализ trailing moves (как далеко утащили):
--    SELECT position_id, 
--           COUNT(*) FILTER (WHERE event_type = 'TRAILING_MOVE') as trailing_count,
--           MAX((payload_json->>'new_sl')::numeric) as max_sl,
--           MIN((payload_json->>'new_sl')::numeric) as min_sl
--    FROM position_events
--    WHERE ts > now() - interval '24 hours'
--      AND event_type IN ('TRAILING_MOVE', 'POSITION_CLOSED')
--    GROUP BY position_id;
--
-- 3. Статистика close_reason:
--    SELECT meta_json->>'close_reason' as reason, COUNT(*)
--    FROM position_events
--    WHERE event_type = 'POSITION_CLOSED'
--      AND ts > now() - interval '7 days'
--    GROUP BY reason
--    ORDER BY count DESC;
--
-- 4. Winrate по arm (из POSITION_CLOSED events):
--    SELECT payload_json->>'ab_arm' as arm,
--           COUNT(*) FILTER (WHERE (payload_json->>'pnl')::numeric > 0) as wins,
--           COUNT(*) as total
--    FROM position_events
--    WHERE event_type = 'POSITION_CLOSED'
--      AND ts > now() - interval '24 hours'
--    GROUP BY arm;
--
-- =============================================================================

-- =============================================================================
-- Опциональная оптимизация: Timescale hypertable
-- =============================================================================
-- Если используете TimescaleDB, можно включить partitioning + compression:
--
-- SELECT create_hypertable('position_events', 'ts', 
--   chunk_time_interval => INTERVAL '7 days',
--   if_not_exists => TRUE
-- );
--
-- -- Compression policy (сжимать chunks старше 7 дней)
-- ALTER TABLE position_events SET (
--   timescaledb.compress,
--   timescaledb.compress_segmentby = 'symbol, event_type',
--   timescaledb.compress_orderby = 'ts DESC'
-- );
--
-- SELECT add_compression_policy('position_events', INTERVAL '7 days');
--
-- -- Retention policy (удалять данные старше 90 дней)
-- SELECT add_retention_policy('position_events', INTERVAL '90 days');
--
-- =============================================================================

