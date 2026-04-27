# World-practice flow v1 (Grafana)

Dashboard JSON: `orderflow_services/grafana/world_practice_flow_v1.json`

Variables:
- `sym`: symbol filter
- `bucket`: exec_regime_bucket filter

Panels:
- taker & cancel rates (EMA)
- cancel-to-trade ratio
- taker_flow_imb_z
- churn score / churn_hi

Runbook: `/runbooks/world_practice_flow_v1.md`
