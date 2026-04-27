# Grafana: World-practice Adverse Realized Drift v1

Файлы:
- `world_practice_adverse_rd_v1.json` — дашборд
- `orderflow_services/runbook_world_practice_adverse_rd_v1.md` — runbook

## Метрики
- `trade_adverse_rd_mean_bps{sym,bucket}` — EWMA mean adverse drift (bps)
- `trade_adverse_rd_sigma_bps{sym,bucket}` — EWMA sigma (bps)
- `trade_adverse_rd_z{sym,bucket}` — robust z-score
- `trade_adverse_rd_bad_share{sym,bucket}` — доля «плохих» дрейфов
- `trade_adverse_rd_n{sym,bucket}` — эффективный sample size
- `trade_adverse_rd_veto{sym,bucket}` — 0/1
- `adverse_rd_eval_total{sym,bucket}` — счётчик дозреваний (rate показывает активность)

## Как читать
- Рост `mean_bps` + рост `bad_share` означает ухудшение качества входов (adverse selection).
- `n` должен быть достаточным (>= `adverse_rd_min_n`), иначе статистика нестабильна.
- Если `eval_rate` > 0, но `n` остаётся 0 → проблема проводки (см. алерт WiringStuck).
