-- =============================================================================
-- Migration: 026_entry_policy_audit.sql
-- Purpose: Долгосрочное хранение entry_policy аудита из Redis Stream
-- =============================================================================
-- Цель: надежное хранение всех entry policy решений для:
-- - Анализ AB-тестов
-- - Дебаг policy gates
-- - Обратное тестирование пороговых значений
-- - Регрессионное тестирование
-- =============================================================================

CREATE TABLE IF NOT EXISTS entry_policy_audit (
  -- Первичный ключ: stream_id из Redis (гарантирует идемпотентность)
  stream_id        TEXT PRIMARY KEY,
  
  -- Timestamp поля (epoch ms и UTC timestamp)
  ts_ms            BIGINT NOT NULL,
  ts               TIMESTAMPTZ NOT NULL,

  -- Идентификаторы сигнала/стратегии
  sid              TEXT,
  symbol           TEXT,
  tf               TEXT,
  strategy         TEXT,
  source           TEXT,

  -- Policy decision (ALLOW / SHADOW / DENY / UNKNOWN)
  decision         TEXT NOT NULL,
  
  -- AB-тестирование и режим
  arm              TEXT,
  ab_group         TEXT,
  scenario         TEXT,  -- continuation / reversal
  regime           TEXT,  -- trend / range / thin

  -- Ключевые метрики качества сигнала (для анализа порогов)
  of_confirm_score DOUBLE PRECISION,
  coh              DOUBLE PRECISION,
  leader_conf      DOUBLE PRECISION,

  -- Микроструктура (для диагностики vetoes)
  spread_z         DOUBLE PRECISION,
  pressure_sps     DOUBLE PRECISION,
  obi_age_ms       BIGINT,

  -- Полный payload (JSONB для гибкого анализа)
  payload_json     JSONB NOT NULL,
  
  -- Служебные поля
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Индексы для типичных запросов
-- =============================================================================

-- 1. Временной анализ (основной)
CREATE INDEX IF NOT EXISTS entry_policy_audit_ts_idx 
  ON entry_policy_audit (ts DESC);

-- 2. Анализ по символу + время
CREATE INDEX IF NOT EXISTS entry_policy_audit_symbol_ts_idx 
  ON entry_policy_audit (symbol, ts DESC);

-- 3. Анализ решений (ALLOW vs SHADOW vs DENY)
CREATE INDEX IF NOT EXISTS entry_policy_audit_decision_ts_idx 
  ON entry_policy_audit (decision, ts DESC);

-- 4. AB-тестирование (анализ по arm)
CREATE INDEX IF NOT EXISTS entry_policy_audit_arm_ts_idx 
  ON entry_policy_audit (arm, ts DESC);

-- 5. JSONB индекс для гибких запросов по payload
CREATE INDEX IF NOT EXISTS entry_policy_audit_payload_gin_idx 
  ON entry_policy_audit USING gin (payload_json);

-- =============================================================================
-- Примеры запросов для аналитики
-- =============================================================================
-- 
-- 1. Winrate по arm за последние 24 часа:
--    SELECT arm, 
--           COUNT(*) FILTER (WHERE decision = 'ALLOW') as allows,
--           COUNT(*) as total
--    FROM entry_policy_audit
--    WHERE ts > now() - interval '24 hours'
--    GROUP BY arm;
--
-- 2. Анализ порогов of_confirm_score:
--    SELECT decision, 
--           percentile_cont(0.5) WITHIN GROUP (ORDER BY of_confirm_score) as median_score
--    FROM entry_policy_audit
--    WHERE ts > now() - interval '7 days' AND of_confirm_score IS NOT NULL
--    GROUP BY decision;
--
-- 3. Частота DENY по причинам:
--    SELECT payload_json->>'reason_code' as reason, COUNT(*)
--    FROM entry_policy_audit
--    WHERE decision = 'DENY' AND ts > now() - interval '24 hours'
--    GROUP BY reason
--    ORDER BY count DESC
--    LIMIT 20;
--
-- =============================================================================

