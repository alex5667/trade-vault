# media-frame-sampler (Phase 3, §6.2)

Samples frames from downloaded media. `social:media:downloaded` → `social:media:frames`
(`media_frames.v1`). Frames feed OCR + visual descriptors (Qdrant) later.

- `MEDIA_SAMPLER_PROVIDER=stub` (default) — deterministic synthetic frame list, no ffmpeg.
- `MEDIA_SAMPLER_PROVIDER=ffmpeg` — extracts `FRAME_COUNT` evenly-spaced JPEGs via ffmpeg.

Off the hot path. Empty/short media still emits a frames doc with `reason_codes`.
