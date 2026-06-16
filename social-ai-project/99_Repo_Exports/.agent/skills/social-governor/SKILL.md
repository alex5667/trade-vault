---
name: social-governor
description: Use this skill to design or review content/trend/publish/platform strategy governors in the social-ai project, ported from the alpha_forecast_v2 governor pattern. Relevant for prompts about off→shadow→canary→enforce, cohort comparison, promotion lift, lower confidence bound, dwell time, rollback, TTL fail-safe, governor metrics, content_strategy_governor, hook_family_governor, platform governors.
---

# Social Governor

## Goal
Safely promote or roll back content/publish strategies using forward-validated cohort comparison — the alpha_forecast_v2 governor pattern adapted to social outcomes.

## Stages
`off → shadow → canary → enforce` (with `rollback`). A strategy never jumps straight to enforce.

## Cohorts
- `admitted` = content the strategy recommended publishing.
- `control` = content the strategy rejected / left in shadow.
- Compare outcomes between cohorts over the relevant outcome window.

## Lift metric
```
content_lift = outcome(admitted) - outcome(control)
outcome =
    0.35 * retention_score
  + 0.25 * engagement_score
  + 0.25 * commerce_score
  + 0.15 * account_growth_score
  - policy_penalty
  - fatigue_penalty
```

## Promotion / rollback discipline (port directly)
- min sample size before any decision
- dwell time per stage (no rapid flapping)
- promote on **lower confidence bound** of lift > threshold, not point estimate
- rollback lift threshold (asymmetric, faster to roll back than to promote)
- TTL fail-safe: stale governor config auto-reverts to safe stage
- each governor writes **only its own strategy-owned config key**; no conflict with other governors
- export Prometheus metrics for stage, lift, rollback, sample size

## Governor catalog
`trend_discovery_governor, content_strategy_governor, hook_family_governor, publish_policy_governor, platform_adapter_governor, commerce_roi_governor` plus per-platform: `youtube_shorts_governor, tiktok_governor, instagram_reels_governor` (same content can win on one platform and lose on another).

### Hook families (for hook_family_governor)
problem-solution, before-after, controversy, checklist, mistake, reaction, comparison, story, tutorial.

## Metrics (rename of afv2_* pattern)
- `content_strategy_gov_stage_idx`
- `content_strategy_gov_lift`
- `content_strategy_gov_rollback_total`
- `content_strategy_gov_sample_size`

## Rules
- A governor without measurable thresholds and a rollback path is incomplete.
- Decisions are explainable: emit reason codes + cohort sizes + lift CI.
- Replay must reproduce governor decisions deterministically (see social-replay).

## Output requirements
Stage config schema, cohort definitions, lift formula + thresholds, dwell/min-sample params, TTL fail-safe, metric names, Grafana panels, rollback alert, and governor-decision replay tests.

## Default lane
**claude-opus + Planning** — governor logic is high-risk and production-affecting.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
