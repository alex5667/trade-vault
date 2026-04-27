# Runbook: World-practice Adverse Realized Drift (adverse_rd) v1

## Что это
**Adverse realized drift** — измерение «adverse selection» после момента сигнала: насколько быстро mid-price уходит *против* направления сигнала в течение короткого горизонта (по умолчанию ~120s), в bps.

Метрика считается **только по факту эмиссии сигналов** (arm) и затем **дозревает на следующих тиках** (update).

## Симптомы
Алерты из `prometheus_alerts_world_practice_adverse_rd_v1.yml`:
- `WorldPracticeAdverseRD_HighMeanBps`: выросла `trade_adverse_rd_mean_bps` (EWMA) в одном из bucket'ов.
- `WorldPracticeAdverseRD_HighBadShare`: доля «плохих» дрейфов `trade_adverse_rd_bad_share` > порога.
- `WorldPracticeAdverseRD_WiringStuck`: `adverse_rd_eval_total` растёт, но `trade_adverse_rd_n` остаётся 0 (счётчик дозреваний есть, статистика не обновляется).

## Быстрый triage (5 минут)
1) **Проверить, есть ли arm-события**:
- `increase(adverse_rd_eval_total{sym="$SYM"}[30m])` > 0 означает, что дозревания происходят.
- Если 0 — возможно, сигналов нет (или arm не вызывается для этого типа сигналов).

2) **Проверить статистику по bucket**:
- `trade_adverse_rd_mean_bps{sym="$SYM"}`
- `trade_adverse_rd_bad_share{sym="$SYM"}`
- `trade_adverse_rd_z{sym="$SYM"}`
- `trade_adverse_rd_n{sym="$SYM"}`

3) **Проверить, не «сломана проводка»**:
- Если `increase(adverse_rd_eval_total[30m])>0`, но `trade_adverse_rd_n==0` длительно → искать исключения/return в месте обновления, либо несоответствие аргументов (arm/update).

## Диагностика причин
### A) Реальная деградация качества исполнения/сигналов
Типичные причины:
- ухудшение ликвидности (спред/глубина), агрессивное движение против входа;
- «погоня» за движением (late entries), плохая синхронизация ts;
- смена режима рынка (volatility, funding, news spikes).

Проверить рядом:
- LOB pressure: `trade_micro_mid_div_bps`, `trade_dw_obi_stability`, `trade_depth_slope_*`.
- Execution risk: `trade_exec_risk_norm`, `trade_expected_slippage_bps`.

### B) Артефакт времени/данных
- сравнить `tick_ts_ms` vs `server_ts_ms`, лаги, монотоничность;
- проверка, что `mid_px_submit` > 0 и соответствует моменту сигнала.

### C) Неподходящие пороги для bucket
- Режимные bucket'ы могут различаться по baseline drift.
- Смотрите `trade_adverse_rd_mean_bps` распределение по времени/сессиям.

## Конфигурация (ключи)
Берутся из `runtime.config`:
- `adverse_rd_horizon_ms` (default 120000)
- `adverse_rd_alpha` (default 0.03)
- `adverse_rd_min_n` (default 40)
- `adverse_rd_mean_th_bps` (default 0.8)
- `adverse_rd_bad_share_th` (default 0.60)
- `adverse_rd_z_th` (default 1.5)
- `adverse_rd_sigma_floor_bps` (default 0.05)
- `adverse_rd_max_pending` (default 4096)

## Митигации
- Временно поднять пороги (mean/bad_share/z) для конкретного `sym`/bucket.
- Снизить частоту сигналов / включить дополнительные гейты (exec_risk, LOB pressure).
- Если `WiringStuck`: откатить изменения или отключить алерт до фикса проводки.

## Rollback / disable
- Отключить правило в bundle (удалить/закомментировать алерт в YAML) или исключить файл из include-list.
- В коде: временно выключить arming (`rd.on_signal`) или `rd.update` блок, сохранив fail-open.
