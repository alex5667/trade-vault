# Runbook: Nightly TCA report v1

## Purpose
Summarise post-trade execution quality from `tca_fill_metrics` over 24h and 7d so rollout/promotion decisions do not rely only on inline Redis rollups.

## Outputs
- Redis hash: `state:tca_nightly_report:last`
- Status JSON: `TCA_NIGHTLY_STATUS_PATH`
- Full report JSON: `TCA_NIGHTLY_REPORT_PATH`
- Prometheus exporter: `orderflow_services/tca_nightly_report_exporter_v1.py`

## Main thresholds
- `TCA_REPORT_MAX_IS_P95_BPS`
- `TCA_REPORT_MAX_PERM_IMPACT_P95_BPS`
- `TCA_REPORT_MIN_REALIZED_SPREAD_P50_BPS`
- `TCA_REPORT_MAX_EFF_SPREAD_P95_BPS`

## Triage
1. Check `tca_nightly_report_last_age_seconds` and `tca_nightly_report_rows_total{window="24h"}`.
2. Open the JSON report and inspect `top_offenders_24h`.
3. Cross-check the same dims in `tca_fill_metrics` / `v_exec_slippage_eval`.
4. If breaches coincide with a new deploy, hold promotion and review execution settings / liquidity filters.
