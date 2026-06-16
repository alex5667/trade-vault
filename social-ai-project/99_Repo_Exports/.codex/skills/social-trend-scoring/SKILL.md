---
name: social-trend-scoring
description: Use this skill for trend intelligence and scoring in the social-ai project: trend candidates/features/clusters/ranking, trend_score/commerce_fit/platform_fit/policy_risk scores, feature registry (trend/content/asset/publish/commerce features v1). Start rule-based, not heavy ML. Relevant for prompts about trend scoring, velocity, novelty, platform fit, feature contracts.
---

# Social Trend Scoring

## Goal
Rank trends into actionable content hypotheses using transparent, versioned features — rule-based first, ML later.

## Stream flow
`social:trend:candidates → :features → :clusters → :ranked`.

## Scores (v1)
`trend_score_v1, commerce_fit_score_v1, platform_fit_score_v1, policy_risk_score_v1`.

## Feature inputs (rule-based first)
velocity (1h/6h), novelty, creator_adoption_delta, audio_reuse_delta, comment_sentiment, visual_pattern_id, platform fit, content repeatability, product/commerce fit, policy risk.

## Feature registry (contract)
Versioned feature schemas with canary contract checks (analog of feature-registry-contract-exporter):
`trend_features_v1, content_features_v1, asset_features_v1, publish_features_v1, commerce_features_v1`.
Example:
```json
{
  "schema": "trend_features_v1",
  "keys": ["platform","topic_cluster_id","velocity_1h","velocity_6h",
           "creator_adoption_delta","audio_reuse_delta","comment_sentiment",
           "visual_pattern_id","commerce_fit_score","policy_risk_score"]
}
```

## Rules
- Do not jump to a complex ML model. Start rule-based + features; add ML only with replay-gated validation.
- Scores are explainable: formula, interpretation, failure modes, sensible ranges, reason codes.
- Feature schema changes go through contract check (backward compatible / versioned).
- Robust stats for noisy velocity/engagement inputs.

## Tests
Feature-builder unit tests, schema/contract tests, score-range + monotonicity tests, golden ranked output.

## Default lane
**claude-haiku/sonnet** for rule-based features; escalate for ML-gated scoring.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
