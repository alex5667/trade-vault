# tiktok-publisher (Phase 9)

Draft-first TikTok publish adapter (plan §6.1, §15.2). Implements `PublisherPort`.

**Safe MVP path:** uploads to the creator's **TikTok inbox as a draft** via the Content
Posting API (`/v2/post/publish/inbox/video/init/`), then the creator finishes + posts it
manually in the app. No audited client or direct-post scope required.

- Source: `PULL_FROM_URL` from `job.asset_uri` (public URL; needs a verified domain in prod).
- No pullable URL → placeholder draft (pipeline tracks the job without a real upload).
- No `TIKTOK_ACCESS_TOKEN` → caller falls back to the SandboxPublisher.
- Status via `/v2/post/publish/status/fetch/`.

ENV: `TIKTOK_ACCESS_TOKEN` (OAuth user token, `video.upload` scope), `REDIS_EVENTS_URL`.

Consumes `social:publish:requests` (platform=tiktok) → `social:publish:status`.
Direct post (auto, after audit + approval) is a later step; draft-first stays the default.
