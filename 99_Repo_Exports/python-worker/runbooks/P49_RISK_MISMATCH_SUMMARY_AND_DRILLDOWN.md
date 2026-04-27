# P4.9 — Risk mismatch summary and drilldown

## Что добавлено
- materialized-summary alerts по `risk_mismatch_summary_mv`
- freshness textfile exporter для mismatch summary
- penalty в operator score на основе materialized mismatch summary
- Grafana drilldown dashboard по `sid` / `decision_id`
- retention/partitioning для `risk_mismatch_quarantine_ledger`

## Операторские шаги
1. Проверьте `/api/risk-mismatch-summary/latest`.
2. Если alert ссылается на Risk Drift Drilldown — откройте dashboard и задайте `sid` или `decision_id`.
3. Для ночной очистки сначала запустите `purge_risk_mismatch_hot_tables.py --dry-run`.
