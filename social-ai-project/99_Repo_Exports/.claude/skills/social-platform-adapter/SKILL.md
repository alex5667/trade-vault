---
name: social-platform-adapter
description: Use this skill to design or review platform publishing/metrics adapters in the social-ai project (TikTok, YouTube, Instagram/Meta, affiliate). These are written from scratch (not ported from trade execution). Relevant for prompts about publish adapter, draft/private upload, OAuth, quota, status lifecycle, webhook, idempotency, platform policy, port/boundary interfaces.
---

# Social Platform Adapter

## Goal
Implement safe, idempotent, observable publishing/metrics adapters behind stable port interfaces. These are **new code** — do not port trade execution logic; only its principles (adapter boundary, retry, status polling, DLQ).

## Adapters
`platform-adapter-tiktok, platform-adapter-youtube, platform-adapter-instagram, platform-adapter-meta, platform-adapter-affiliate`.

## Port interfaces (per platform)
- `PublisherPort` (draft/schedule/publish/cancel/retry)
- `MetricsPort` (collect outcome snapshots)
- `DraftPort` (draft/private/unlisted upload)
Keep the control plane depending on ports, not concrete SDKs.

## PublishCommand contract
```ts
type PublishCommand = {
  action: "draft" | "schedule" | "publish" | "cancel" | "retry";
  jobId: string;
  platform: "tiktok" | "instagram" | "youtube";
  accountId: string;
  assetId: string;
  caption?: string; title?: string; description?: string;
  scheduledAtMs?: number;
  metadata: Record<string, unknown>;
  source: "agent" | "operator" | "governor" | "replay";
  timestampMs: number;
};
```

## Safety-first rollout per platform
- **YouTube first** (clear upload workflow): private/unlisted upload → set title/description/thumbnail → collect metrics → compare titles/thumbnails. Autopublish OFF.
- **TikTok**: draft upload → manual review → direct post only after approval; status via polling/webhook.
- **Instagram**: keep behind `InstagramPublisherPort`/`MetricsPort`/`DraftPort`; MVP = manual export + asset prep + caption + review queue. Autopublish only after a separate Meta API review.

## Rules (each adapter)
- Idempotent publish keyed on `jobId` (`publish_duplicate_blocked_total`).
- Distinguish transient (retry/backoff) vs permanent (DLQ) failures; respect quota.
- Map platform status lifecycle to internal states; never assume success without confirmation.
- Emit a publish audit log (who/what/when/source).
- Policy/disclosure checks before publish (see social-publish-policy).
- Status changes flow back as `social:publish:status` events.

## Observability
`publish_jobs_total`, `publish_success_total`, `publish_failure_total{reason}`, `publish_retry_total`, `publish_duplicate_blocked_total`, `publish_policy_reject_total`.

## Tests
Unit (command mapping), integration (sandbox/mock API), fault injection (quota, 5xx, partial upload), idempotency (double-submit), status-lifecycle mapping.

## Default lane
**claude-sonnet/opus + Planning** — adapters are external-facing and irreversible once publishing.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
