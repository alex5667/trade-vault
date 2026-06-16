---
name: social-media-pipeline
description: Use this skill for the media processing pipeline in the social-ai project (new code): media ingest/normalize/transcribe(ASR)/OCR/frame-sampler/thumbnail/render/quality-check/rights-check, MinIO/S3 storage, Qdrant visual embeddings. Relevant for prompts about видео обработка, ASR, OCR, кадры, thumbnail, render, медиа pipeline. MVP can skip full video generation.
---

# Social Media Pipeline

## Goal
Turn raw media into enriched, searchable, publish-ready assets — built incrementally, off the hot ingestion path.

## Stages (services)
`media-ingest, media-normalize, media-transcribe, media-ocr, media-frame-sampler, media-thumbnail-generator, media-render, media-quality-check, media-rights-check`.

## MVP discipline (do NOT start with video generation)
- MVP enrichment = caption + title + metadata + manual notes.
- Then add ASR (transcribe) → OCR → frame sampling → visual embeddings (Qdrant).
- Asset placeholders / manual upload before automated render.

## Stream flow
`social:media:downloaded → :transcribed → :ocr → :frames → :enriched`. Render: `social:asset:render_requests → :render_results`.

## Storage
- MinIO/S3: video, frames, thumbnails, transcripts, rendered assets (store `asset_hash`).
- Qdrant: visual/creative embeddings for reference search + creative memory (RAG).
- Timescale/Postgres: descriptors, transcripts metadata, render-job status.

## Rules
- Heavy work async via consumer groups; never block ingestion.
- Content-addressed assets (hash) for dedupe + idempotent reprocessing.
- `media-rights-check` + `media-quality-check` gate before an asset is publish-eligible.
- Every artifact archived (book/media archive) for replay.
- Deterministic, versioned processors for golden replay.

## Observability
processing latency per stage, queue depth, failure/quarantine rate, transcode error rate, embedding throughput, storage growth.

## Tests
Unit per processor, integration (stream→processor→storage), golden (fixed input → stable transcript/OCR/descriptors), quality/rights gate cases.

## Default lane
**claude-haiku** for a single bounded processor; **sonnet/opus + Planning** for end-to-end media architecture.

# Language Preferences
**CRITICAL REQUIREMENT:** Always respond to the user in Russian (на русском языке).
