-- Phase C.2 (P1 outcome-calibrated): aggregation view над signal_outcomes.
--
-- Назначение: каждую ночь промоут-сервис читает rolling 14d метрики из этой
-- view, проверяет гейты (n>=300, ev_r>0.05, bootstrap_ci_low>0), и пишет
-- решение в strategy_bucket_metrics + Redis key autocal:regime_exec:state.
--
-- Контракт колонок совпадает с promotion-сервисом
-- (services/regime_exec_promotion_v1.py).
--
-- View — обычная (не materialized), чтобы избежать вопросов с continuous
-- aggregate refresh policy и leakage в дальнейшем. Промоут-сервис вызывается
-- nightly (cron-таймер) — full scan по 14d узкой таблицы быстрый.

CREATE OR REPLACE VIEW strategy_bucket_outcomes_14d AS
SELECT
    -- bucket dimensions
    so.symbol                                                AS symbol,
    COALESCE(NULLIF(so.regime, ''), 'na')                    AS regime_label,
    COALESCE(NULLIF(so.scenario, ''), 'na')                  AS scenario,
    COALESCE(NULLIF(so.direction, ''), 'LONG')               AS direction,
    -- KPIs
    COUNT(*)                                                 AS n,
    AVG(CASE WHEN so.is_win THEN 1.0 ELSE 0.0 END)::float8   AS win_rate,
    AVG(so.r_multiple)::float8                               AS avg_r,
    AVG(so.r_multiple - COALESCE(so.fees, 0) / NULLIF(so.one_r_money, 0))::float8
                                                              AS ev_r_after_costs,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY so.mfe_pnl / NULLIF(so.one_r_money, 0))::float8
                                                              AS mfe_r_p50,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY so.mfe_pnl / NULLIF(so.one_r_money, 0))::float8
                                                              AS mfe_r_p90,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(so.mae_pnl) / NULLIF(so.one_r_money, 0))::float8
                                                              AS mae_r_p50,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY ABS(so.mae_pnl) / NULLIF(so.one_r_money, 0))::float8
                                                              AS mae_r_p90,
    AVG(CASE WHEN so.close_reason = 'TIMEOUT' THEN 1.0 ELSE 0.0 END)::float8
                                                              AS timeout_rate,
    MAX(so.ts)                                                AS last_ts,
    MIN(so.ts)                                                AS first_ts
FROM signal_outcomes so
WHERE so.ts >= now() - INTERVAL '14 days'
  AND so.is_virtual = FALSE
  AND so.one_r_money IS NOT NULL
  AND so.one_r_money > 0
GROUP BY 1, 2, 3, 4;

-- Bucket health helper — quick alert query for promotion gates.
CREATE OR REPLACE VIEW strategy_bucket_eligibility AS
SELECT
    symbol,
    regime_label,
    scenario,
    direction,
    n,
    win_rate,
    ev_r_after_costs,
    avg_r,
    timeout_rate,
    -- Простые гейты (промоут-сервис применяет полный набор):
    (n >= 300)                            AS gate_n_ok,
    (ev_r_after_costs >= 0.05)            AS gate_ev_ok,
    (avg_r > 0)                           AS gate_avg_r_ok,
    (timeout_rate <= 0.70)                AS gate_timeout_ok,
    (n >= 300 AND ev_r_after_costs >= 0.05 AND avg_r > 0 AND timeout_rate <= 0.70)
                                          AS gate_all_ok
FROM strategy_bucket_outcomes_14d;
