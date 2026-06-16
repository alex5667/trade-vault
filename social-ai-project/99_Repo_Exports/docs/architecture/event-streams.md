# Event Streams (plan §10)

Canonical names in `packages/redis-streams/streams.{ts,py}`. Renames require a contract check.

## Raw ingestion
`social:tiktok:raw` · `social:instagram:raw` · `social:youtube:raw` · `social:ads:raw` · `social:owned:raw`

## Enrichment
`social:media:downloaded` → `:transcribed` → `:ocr` → `:frames` → `:enriched`

## Trend intelligence
`social:trend:candidates` → `:features` → `:clusters` → `:ranked`

## Content generation
`social:brief:requests` / `:results` · `social:script:requests` / `:results` · `social:asset:render_requests` / `:render_results`

## Policy & approval
`social:policy:checks` / `:results` · `social:review:queue` / `:decisions`

## Publishing
`social:publish:requests` · `:attempts` · `:status` · `:failures`

## Outcomes
`social:outcome:1h|6h|24h|7d` · `social:commerce:events`

## DLQ / quarantine / replay
`social:dlq` · `social:quarantine` · `social:replay:requests` / `:results`

## Conventions
- Producer writes a valid envelope (`schemas/social_event.v1.json` and friends).
- Consumer validates schema; bad event → `social:quarantine`; unprocessable → `social:dlq`.
- Consumer groups with PEL recovery, claim/reclaim, idempotent processing.
- Short stream retention; durable raw archived to Timescale/MinIO before trimming.
- Metrics: `social_stream_lag`, `social_stream_pel_pending_total`, `social_stream_dlq_total`, `social_stream_replay_total`.
