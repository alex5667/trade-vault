# Phase 1.8 — Rollback SLO / Retry / Escalation

## Scope
Only `scanner_infra`. No UI. No hot-path changes.

## Components
- rollback_slo_analytics_v1
- rollback_retry_controller_v1
- rollback_auto_escalation_summarizer_v1

## Safety
- Retry is bounded by attempts and backoff.
- Escalation is advisory and audit-friendly.
- Hard-stop reasons never auto-retry.

## Smoke checks
- `stream:ml:recommendation_rollback_state`
- `stream:ml:recommendation_rollback_verification_results`
- `stream:ml:recommendation_rollback_retry_requests`
- `stream:ml:recommendation_escalations`
