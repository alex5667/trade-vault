---
name: social-observability-rollout
description: Use this skill for production rollout, rollback, SRE, Prometheus metrics, logs, alerts, canary/shadow deployments, latency budgets, failure modes, and operational safety in the social-ai project. Relevant for prompts about observability, rollout, rollback, dashboards, alerts, SLOs, shadow/draft mode, canary, circuit breakers, autopublish safety, production readiness.
---

# Social Observability and Rollout

## Goal
Ensure every significant social-system change is measurable, alertable, and safely releasable — with autopublish gated behind draft/private until SLOs are stable.

## Use this skill for
- Production readiness review
- Metrics/logging/alert design
- Canary/shadow/draft/enforce ramp plans
- Rollback strategy and degraded safe mode
- SLO/SLI design for collectors, workers, LLM, publishers
- Failure-mode and degradation planning

## Mandatory deliverables
- RED metrics (rate/errors/duration) per service + domain metrics
- Structured log fields
- Alert rules with thresholds and rationale
- Rollout stages + rollback triggers

## Preferred rollout ladder
1. Local verification
2. Replay / golden / fixture validation
3. Shadow mode (generate, do not publish)
4. Draft / private / unlisted publish (human review)
5. Canary by platform / account / traffic share
6. Gradual ramp
7. Full enablement (autopublish only after stable SLOs)

## Rollback rules
- Define automatic + manual rollback conditions.
- Define safe fallback mode (e.g. drop to draft-only, pause autopublish).
- Prefer config-gated rollback over emergency code rollback.
- Preserve debuggability after rollback.

## Metrics guidance (domain)
- Ingestion: `social_ingest_events_total`, `social_source_freshness_lag_ms`, `social_duplicate_rate`, `social_schema_validation_failed_total`
- Streams: `social_stream_lag`, `social_stream_pel_pending_total`, `social_stream_claimed_total`, `social_stream_dlq_total`, `social_stream_replay_total`
- LLM: `llm_requests_total`, `llm_latency_ms`, `llm_json_invalid_total`, `llm_schema_reject_total`, `llm_cost_per_plan`, `llm_timeout_total`
- Publish: `publish_jobs_total`, `publish_success_total`, `publish_failure_total`, `publish_retry_total`, `publish_duplicate_blocked_total`, `publish_policy_reject_total`
- Growth: `content_views_1h`, `content_retention_1h`, `content_ctr_24h`, `content_cvr_7d`, `content_revenue_7d`, `content_margin_7d`, `content_ltv_proxy_30d`

## Alerts guidance
Hard-failure, latency regression, data-quality, stale/no-data, LLM-schema-reject spike, publish-failure spike, autopublish-without-approval, governor rollback, KPI degradation.

## Output requirements
Prometheus metric names, sample log fields, alert expressions, rollout steps, rollback steps, post-deploy validation checklist. Be operationally concrete — never "monitor closely" without thresholds.

## Default lane
**claude-haiku-4-5 (fast mode)** for bounded metrics/alert/dashboard boilerplate. Escalate when rollout spans multiple services, autopublish/SLO policy is redesigned, or incident ambiguity needs deeper reasoning.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
