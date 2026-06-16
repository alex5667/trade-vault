---
name: social-go-redis-ingest
description: Use this skill for Go collectors/gateways that ingest social-platform data and publish to Redis Streams in the social-ai project: TikTok/Instagram/YouTube/Ads/owned-account collectors, API adapters, webhook receivers, quota-aware polling, reconnection/backoff, payload contracts, dedupe, idempotent XADD, and goroutine/channel design. Relevant for prompts about Go collectors, social ingestion gateway, Redis XADD/consumer groups, quota, freshness lag, dedupe, webhooks.
---

# Social Go Redis Ingest

## Goal
Design or improve Go ingestion services that are quota-aware, deterministic, resilient, and observable.

## Use this skill for
- Platform API collectors (TikTok, Instagram, YouTube, Ads, owned-account metrics)
- Webhook receivers and the social ingestion / command gateway (`go-social-gateway`)
- Redis Streams publishing (XADD into `social:*:raw`)
- Quota allocation, token buckets, backoff, polling cadence
- Dedupe and freshness tracking
- Hot-path performance in Go

## Design rules
- Keep parsing, validation, normalization, and publishing as separate stages — not one opaque function.
- Use explicit structs for each platform's API payload and a single internal **EventEnvelope**.
- Preserve source timestamps; add `ingest_time_ms` separately. Never overwrite platform time.
- Make reconnect/poll logic bounded, observable, jittered, and quota-aware (respect rate-limit headers / token buckets in `redis-rate-limit`).
- Distinguish transient network/quota errors (retry/backoff) from schema/data errors (quarantine).
- Dedupe at ingest: key on `(platform, object_type, object_id, snapshot_ts)`.

## Preferred architecture
1. Source loop (poll or webhook intake) with quota gate
2. Decode/validate stage
3. Normalize/envelope stage (-> EventEnvelope v1)
4. Dedupe stage
5. Publish stage (XADD with explicit MAXLEN)
6. Metrics/logging stage

## Redis Streams contract guidance
- Define stream names explicitly: `social:tiktok:raw`, `social:instagram:raw`, `social:youtube:raw`, `social:ads:raw`, `social:owned:raw`.
- Define payload schema with field names + units; version it (`social_event.v1`).
- Set retention/MAXLEN explicitly and justify it; durable raw goes to the social-event archive (Timescale/MinIO) because stream retention is short.
- Use consumer groups downstream; producers stay simple and idempotent.

## Reliability rules
- Heartbeat/quota watchdog per source.
- Reconnect / re-poll with exponential backoff + cap + jitter.
- Idempotent shutdown path; flush in-flight before exit.
- DLQ/quarantine for malformed payloads (`social:quarantine`).

## Observability (minimum)
- `social_ingest_events_total{platform,source}`
- `social_ingest_errors_total{platform,reason}`
- `social_source_freshness_lag_ms{platform,source}`
- `social_duplicate_rate{platform}`
- `social_quota_remaining{platform,account}`
- `social_schema_validation_failed_total{schema}`
- publish latency + end-to-end lag

## Tests required
- Unit: per-platform decode/normalize
- Integration: Redis XADD path + consumer read-back
- Fault injection: quota exhaustion, 429s, malformed frames, slow Redis
- Dedupe: duplicate snapshots collapse correctly

## Output style
Concrete file diffs, structs, stream/key names, config keys, benchmark guidance.

## Default lane
Assume **claude-haiku-4-5 (fast mode)** by default. Escalate only on triggers below.

## Escalate to claude-sonnet-4-6/opus-4-6 if
- the change alters cross-service contracts or end-to-end sequencing
- root cause spans Go, Redis, and downstream consumers
- a full ingest architecture redesign is needed

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
