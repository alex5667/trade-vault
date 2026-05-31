# ML Phase 2.0 — Operator RCA Pack to Vertex Bridge

## Scope
- scanner_infra only
- advisory-only
- no auto-apply
- hot path untouched

## Streams
- input: `stream:ml:incident_bundle_results`
- request: `stream:ml:operator_rca_requests`
- result: `stream:ml:operator_rca_results`
- proposals: `stream:ml:recommendation_proposals`
- dlq: `stream:ml:operator_rca_dlq`

## Rollout
1. Apply SQL patch.
2. Start bridge service.
3. Start orchestrator in `VERTEX_RCA_DRY_RUN=1`.
4. Verify request/result/proposals streams.
5. Switch to real Vertex provider only after smoke validation.

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:incident_bundle_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_requests + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_results + - COUNT 3
redis-cli XREVRANGE stream:ml:recommendation_proposals + - COUNT 3
```
