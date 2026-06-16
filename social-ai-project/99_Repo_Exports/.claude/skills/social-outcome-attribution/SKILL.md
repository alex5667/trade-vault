---
name: social-outcome-attribution
description: Use this skill for outcome tracking and commerce attribution in the social-ai project (new code): outcome snapshots 1h/6h/24h/7d, platform metrics storage, commerce event ingestion, attribution join publish_id→post_id→click_id→order_id→margin→LTV, content ROI scoring. Relevant for prompts about outcomes, attribution, conversion, commerce, ROI, LTV.
---

# Social Outcome & Attribution

## Goal
Close the loop from publish to commercial result, so governors learn from real outcomes — not just views.

## Attribution chain
```
publish_id → post_id → click_id → order_id → margin → LTV
```
Minimal entities: `content_asset, publish_job, platform_post, tracking_link, affiliate_click, commerce_order, content_outcome`.

## Outcome windows
Snapshots at `1h / 6h / 24h / 7d` (commerce/LTV mostly 7d+). Window resolution deterministic and replay-safe (path/outcome-window resolution analog of trade entry-fill replay).

## Streams
`social:outcome:1h|6h|24h|7d`, `social:commerce:events`.

## Content ROI / outcome score
Feed the governor outcome formula:
```
outcome = 0.35*retention + 0.25*engagement + 0.25*commerce + 0.15*account_growth - policy_penalty - fatigue_penalty
```
Track both: views/retention/CTR (social) AND CVR/revenue/margin/LTV (commerce). Beware "many views, few sales" — commerce-aware scoring is required.

## Rules
- Idempotent snapshot writes keyed on `(object_id, window)`.
- Defensive delta math on cumulative counters (clamp/flag resets — see social-data-quality-time).
- Attribution joins are explicit and tested; record join confidence.
- Archive raw metric snapshots for replay.

## Observability (growth)
`content_views_1h, content_retention_1h, content_ctr_24h, content_cvr_7d, content_revenue_7d, content_margin_7d, content_ltv_proxy_30d`.

## Tests
Snapshot idempotency, attribution-join correctness, outcome-window resolution, ROI formula, replay of outcome attribution.

## Default lane
**claude-sonnet** for attribution joins; **opus + Planning** for ROI/score model design.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
