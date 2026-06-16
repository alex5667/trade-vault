---
name: social-publish-policy
description: Use this skill for the publish gate, policy critic, brand/compliance checks, disclosure flags, and review queue in the social-ai project — the admission gate analog of the trade forward gate. Relevant for prompts about publish gate, policy risk score, policy critic, disclosure, brand safety, review queue, approve/reject, platform sanctions.
---

# Social Publish Policy & Gate

## Goal
Admit content to publishing only when it passes policy, brand, and platform-compliance checks — the social analog of a forward/admission gate. Human-in-the-loop until SLOs are stable.

## Pipeline position
`asset → policy checks → review queue → operator approve/reject → publish gate → adapter`.
Streams: `social:policy:checks → :results`, `social:review:queue → :decisions`.

## Policy critic (LLM, structured)
Produces `policy_risk_score`, `risk_flags`, `disclosure_flags`, `reason_codes` (JSON, validated). It advises; it never auto-publishes.

## Checks
- Brand policy / tone / prohibited topics
- Platform policy (per platform: TikTok/YouTube/Instagram rules)
- Disclosure requirements (ads, affiliate, AI-generated)
- Rights/licensing (ties to media-rights-check)
- Fatigue / duplication (don't repost near-identical content)

## Publish gate rules
- Gate decision is deterministic given scores + policy config; emit reason codes.
- Hard block on disclosure failures and high policy risk.
- Default to draft/private until autopublish SLO is met; slow rollout to reduce sanction risk.
- Idempotent: a job already published is never re-admitted (`publish_duplicate_blocked_total`).

## Review queue (operator UI)
For each job show: trend evidence, brief, script, caption/title/description, thumbnail, policy result, disclosure flags, platform-specific preview, approve/reject.

## Observability
`publish_policy_reject_total{reason}`, review queue depth, approval latency, autopublish-without-approval alert, policy-critic schema-reject rate.

## Tests
Policy scoring unit tests, gate state-transition tests, disclosure-required cases, idempotency, golden policy-critic outputs.

## Default lane
**claude-sonnet/opus** — policy/gate affects brand safety and platform standing.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
