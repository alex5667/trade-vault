# media-transcribe (Phase 3 — ASR MVP)

Speech-to-text over downloaded media (plan §10.2). `social:media:downloaded` →
`social:media:transcribed` (`media_transcript.v1`).

Backends (same Stub-vs-real pattern as the LLM client):
- `MEDIA_ASR_PROVIDER=stub` (default) — deterministic transcript, no model/ffmpeg; dev/golden/CI.
- `MEDIA_ASR_PROVIDER=whisper` — `faster-whisper` (`WHISPER_MODEL=base|small|...`), ffmpeg in image.

Off the hot ingestion path. Empty/failed audio still emits a transcript with `reason_codes`
so the content pipeline can proceed on caption+metadata (plan §Phase 3 MVP: start without
full video processing). Heavier media (OCR, frames, embeddings) lands later.
