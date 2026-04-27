# ML Phase 2.7 — operator RCA routing SLO / retry / escalation

## Scope
- scanner_infra only
- advisory / governance only
- no hot-path impact

## Components
- `operator_rca_routing_slo_analytics_v2_7.py`
- `rollback_retry_controller_v2_7.py`
- `rollback_auto_escalation_summarizer_v2_7.py`

## Streams
- input:
  - `stream:ml:operator_rca_routing_verify_results`
  - `stream:ml:operator_rca_routing_rollback_results`
  - `stream:ml:operator_rca_routing_retry_audit`
- output:
  - `stream:ml:operator_rca_routing_slo_rollups`
  - `stream:ml:operator_rca_routing_retry_requests`
  - `stream:ml:operator_rca_routing_escalations`

## Safe start
```bash
export ML_OPERATOR_RCA_ROUTING_RETRY_MAX_ATTEMPTS=3
export ML_OPERATOR_RCA_ROUTING_RETRY_BASE_BACKOFF_SEC=60
export ML_OPERATOR_RCA_ROUTING_RETRY_MAX_BACKOFF_SEC=900
```

## Smoke checks
```bash
curl -s localhost:9880/metrics | grep '^ml_operator_rca_routing_slo_'
curl -s localhost:9881/metrics | grep '^ml_operator_rca_routing_retry_'
curl -s localhost:9882/metrics | grep '^ml_operator_rca_routing_escalation_'
redis-cli HGETALL metrics:ml:operator_rca_routing_slo:last
redis-cli XREVRANGE stream:ml:operator_rca_routing_retry_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_escalations + - COUNT 5
```
