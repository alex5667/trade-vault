-- 1. Добавление колонок в open_positions
ALTER TABLE IF EXISTS open_positions 
ADD COLUMN IF NOT EXISTS atr_policy_ver INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS atr_policy_tag VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_source VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_scenario VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_regime VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_bucket VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_trailing_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_recovery_run_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_status VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json JSONB DEFAULT '{}'::jsonb;

-- 2. Добавление колонок в closed_trades
ALTER TABLE IF EXISTS trades_closed 
ADD COLUMN IF NOT EXISTS atr_policy_ver INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS atr_policy_tag VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_source VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_scenario VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_regime VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_bucket VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_trailing_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_recovery_run_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_status VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json JSONB DEFAULT '{}'::jsonb;

-- 3. Добавление колонок в paper_trades (Shadow-Control dataset)
ALTER TABLE IF EXISTS paper_trades 
ADD COLUMN IF NOT EXISTS atr_policy_ver INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS atr_policy_tag VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_source VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_scenario VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_regime VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_bucket VARCHAR(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_trailing_mode VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_recovery_run_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_id VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_status VARCHAR(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json JSONB DEFAULT '{}'::jsonb;


-- 4. Создание индексов для быстрой склейки атрибуции
CREATE INDEX IF NOT EXISTS idx_trades_closed_atr_policy_tag ON trades_closed(atr_policy_tag, exit_ts_ms);
CREATE INDEX IF NOT EXISTS idx_trades_closed_atr_policy_ver ON trades_closed(atr_policy_ver, exit_ts_ms);
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'paper_trades' AND table_schema = current_schema()) THEN
    CREATE INDEX IF NOT EXISTS idx_paper_trades_atr_policy_tag ON paper_trades(atr_policy_tag, exit_ts_ms);
  END IF;
END $$;

-- 5. Базовый View для агрегации по версии полиси
CREATE OR REPLACE VIEW v_closed_trades_by_atr_policy AS
SELECT
    atr_policy_ver,
    atr_policy_tag,
    symbol,
    direction,
    COUNT(*) as total_trades,
    SUM(pnl_net) as total_pnl_net,
    AVG(pnl_net) as avg_pnl_net,
    SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::float / GREATEST(COUNT(*), 1) as win_rate,
    MIN(exit_ts_ms) as first_exit_ms,
    MAX(exit_ts_ms) as last_exit_ms
FROM trades_closed
WHERE exit_ts_ms > 0
GROUP BY atr_policy_ver, atr_policy_tag, symbol, direction
ORDER BY total_pnl_net DESC;

-- 6. Аналитический View оценки просадки на Recovery Run
CREATE OR REPLACE VIEW v_closed_trades_by_recovery_cert AS
SELECT
    atr_recovery_run_id,
    atr_restore_cert_id,
    atr_restore_cert_status,
    COUNT(*) as count_trades,
    SUM(pnl_net) as recovery_pnl,
    AVG(pnl_net) as recovery_avg_pnl,
    SUM(duration_ms) / 1000.0 / 60.0 as total_time_in_market_minutes
FROM trades_closed
WHERE atr_recovery_run_id != ''
GROUP BY atr_recovery_run_id, atr_restore_cert_id, atr_restore_cert_status
ORDER BY MAX(exit_ts_ms) DESC;
