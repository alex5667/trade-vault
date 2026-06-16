# media-ocr (Phase 3, §6.2)

On-screen text extraction over sampled frames. `social:media:frames` → `social:media:ocr`
(`media_ocr.v1`). Captures hooks/CTAs/price overlays — key signals for trend/policy scoring.

- `MEDIA_OCR_PROVIDER=stub` (default) — deterministic overlay text, no image decoding; dev/CI.
- `MEDIA_OCR_PROVIDER=easyocr` — easyocr over local frame images (uncomment easyocr in requirements).

Frames with no text are fine — output carries `reason_codes=[ocr_no_text]`.
