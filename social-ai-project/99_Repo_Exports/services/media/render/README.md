# media-render (Phase 3, §6.2 — template-first)

`social:asset:render_requests` → `social:asset:render_results` (`render_job.v1`).
Template-first per plan: NO end-to-end video generation. Builds a deterministic render
manifest (template + script + shot list + voiceover plan) and a content-addressed
`asset_hash` (same brief+manifest → same asset, idempotent). A real ffmpeg/MoviePy/Piper
renderer fills in the bytes later behind the same contract.
