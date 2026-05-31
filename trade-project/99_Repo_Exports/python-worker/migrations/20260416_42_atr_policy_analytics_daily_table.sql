-- 20260416_36_atr_policy_analytics_daily_table.sql
-- Creates the daily analytics aggregation table for ATR policy cohorts.
-- Previously only created at runtime by atr_policy_analytics_daily_service.run_once(),
-- which was never wired into docker-compose. The exporter assumes this table exists.

CREATE TABLE IF NOT EXISTS atr_policy_analytics_daily (
    day                    date             NOT NULL,
    symbol                 text             NOT NULL,
    kind                   text             NOT NULL,
    atr_policy_ver         integer          NOT NULL,
    atr_policy_tag         text             NOT NULL,
    atr_policy_scenario    text             NOT NULL,
    atr_policy_regime      text             NOT NULL,
    atr_policy_bucket      text             NOT NULL,
    atr_stop_ttl_mode      text             NOT NULL,
    atr_trailing_mode      text             NOT NULL,
    atr_recovery_run_id    text             NOT NULL,
    atr_restore_cert_status text            NOT NULL,
    n_trades               integer          NOT NULL,
    avg_pnl_bps            double precision,
    avg_slippage_bps       double precision,
    win_rate               double precision,
    stop_rate              double precision,
    tp1_rate               double precision,
    PRIMARY KEY (
        day, symbol, kind,
        atr_policy_ver, atr_policy_tag,
        atr_policy_scenario, atr_policy_regime, atr_policy_bucket,
        atr_stop_ttl_mode, atr_trailing_mode,
        atr_recovery_run_id, atr_restore_cert_status
    )
);

GRANT ALL PRIVILEGES ON TABLE atr_policy_analytics_daily TO trading;
