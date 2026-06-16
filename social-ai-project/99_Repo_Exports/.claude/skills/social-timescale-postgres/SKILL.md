---
name: social-timescale-postgres
description: Use this skill for Postgres + TimescaleDB work in the social-ai project: entity tables (content_assets, publish_jobs, platform_posts, commerce_orders, review/policy decisions), hypertables (metric snapshots, engagement/commerce events, governor decisions, experiment exposures/outcomes), continuous aggregates (trend velocity, content-family performance, hook winrate, product ROI), retention, migrations, and outcome-window queries. Relevant for prompts about схема БД, гипертаблицы, continuous aggregates, retention, миграции, outcome windows.
---

# Social Timescale / Postgres

## Goal
Design correct, evolvable, performant storage for social events, content state, outcomes, and aggregates.

## Use this skill for
- Relational entities (state machine, content lineage, commerce attribution)
- Timescale hypertables for time-series metrics/events
- Continuous aggregates and retention policy
- Migrations and backward-compatible schema change
- Outcome-window resolution queries (1h/6h/24h/7d)

## Entity tables (Postgres)
`accounts, platform_accounts, creators, products, campaigns, content_assets, content_briefs, content_scripts, publish_jobs, platform_posts, tracking_links, affiliate_clicks, commerce_orders, review_decisions, policy_decisions`.
- Model content lineage explicitly: `trend → brief → script → asset → publish_job → platform_post → outcome`.
- Store the pipeline state on the owning entity (see state machine in social-project-core).

## Timescale hypertables
`trend_observations, platform_metric_snapshots, engagement_events, publish_status_events, commerce_events, dq_events, governor_decisions, experiment_exposures, experiment_outcomes`.
- Pick `time` column = canonical `epoch_ms`/timestamptz UTC; declare partitioning interval deliberately.
- Index on `(platform, object_id, time)` for snapshot lookups.

## Continuous aggregates
`trend_velocity_1h, trend_velocity_6h, content_family_performance_24h, platform_account_health_24h, hook_family_winrate_7d, product_content_roi_7d, creator_product_fit_30d`.
- Define refresh policy + lag; never read raw hypertables in hot dashboards when an aggregate exists.

## Outcome windows
`1h` early hook/retention signal · `6h` first platform fit · `24h` main social outcome · `7d` commerce/LTV/delayed conversion. Window resolution must be deterministic and replay-safe.

## Rules
- Every schema change is reviewed for backward compatibility; additive first, version payloads (`*_v1`).
- Migrations are reversible; state up + down. Round-trip test schema in CI.
- Be explicit about timestamp columns, units, and timezone.
- Retention: justify drop policy; durable raw archive before trimming streams.
- Provide indexes + EXPLAIN reasoning for any new hot query.

## Output requirements
DDL/migrations, hypertable + aggregate definitions, retention policy, index plan, sample queries, and tests (round-trip + outcome-window correctness).

## Default lane
**claude-haiku-4-5 (fast mode)** for bounded DDL/queries. Escalate for schema/retention/migration redesign or cross-service contract impact.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
