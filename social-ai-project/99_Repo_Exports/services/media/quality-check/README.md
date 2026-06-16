# media-quality-check (Phase 3, §6.2)

Asset-integrity gate. `social:media:frames` → `social:media:quality` (`media_quality.v1`).
Pairs with media-rights-check; the publish gate consumes both for publish-eligibility.

`assess_quality`: `no_frames` → fail; duration out of [1s, 600s] → needs_review; else pass.
Resolution/bitrate/audio probing is added with the ffmpeg backend later.
