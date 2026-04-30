from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class DQThresholds:
    """Resolved DQ policy thresholds (SAFE/STRICT matrix + per-interval book tuning).

    Design goals:
      - Single source of truth for policy knobs (train==serve evidence).
      - Deterministic defaults (no dependence on wall-clock).
      - Fail-open: missing/invalid cfg values fall back to safe defaults.

    Field meanings:
      mode: "safe" | "strict" (global DQ mode)
      gap_*_ms: thresholds for tick_gap_p95_ms (requires min_samples)
      tick_soft/tick_hard: thresholds for tick_missing_seq_ema
      book_soft/book_hard: thresholds for book_missing_seq_ema (derived per stream interval)
      book_stream_interval_ms: configured depth stream cadence (100/250/500/1000ms)
      book_seq_ema_alpha: EMA alpha used to compute book_missing_seq_ema (must match runtime tracker)
    """

    mode: str
    gap_soft_ms: int
    gap_hard_ms: int
    gap_extreme_ms: int
    min_samples: int
    tick_soft: float
    tick_hard: float
    book_soft: float
    book_hard: float
    book_stream_interval_ms: int
    book_seq_ema_alpha: float


def _safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _safe_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _first(cfg: Mapping[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in cfg and cfg.get(k) is not None:
            return cfg.get(k)
    for k in keys:
        v = os.getenv(k.upper())
        if v is not None and v != "":
            return v
    return None


def derive_book_seq_ema_alpha(interval_ms: int) -> float:
    """Default alpha mapping for book_missing_seq_ema.

    Guidance from the plan:
      ~100ms (≈10Hz): 0.10
      250ms:         0.20
      500ms:         0.30
      1Hz:           0.30–0.50  (pick conservative 0.40)
    """

    ms = int(interval_ms or 0)
    if ms <= 0:
        ms = 100
    if ms <= 150:
        return 0.10
    if ms <= 350:
        return 0.20
    if ms <= 750:
        return 0.30
    return 0.40


def _derive_book_hard(mode: str, interval_ms: int) -> float:
    """Per-interval defaults for the HARD book_missing_seq_ema threshold."""

    m = str(mode or "").strip().lower()
    ms = int(interval_ms or 0)
    if ms <= 0:
        ms = 100

    if m == "strict":
        if ms <= 150:
            return 0.10
        if ms <= 350:
            return 0.15
        if ms <= 750:
            return 0.20
        return 0.25

    # SAFE
    if ms <= 150:
        return 0.25
    if ms <= 350:
        return 0.35
    if ms <= 750:
        return 0.45
    return 0.55


def resolve_dq_thresholds(cfg: Mapping[str, Any] | None) -> DQThresholds:
    """Resolve thresholds from cfg/env with safe defaults.

    The matrix defaults are taken from the "План реализации недоделок" B2 section.
    """

    c: Mapping[str, Any] = cfg or {}

    mode_raw = _first(c, ["dq_mode", "DQ_MODE"]) or "safe"
    mode = str(mode_raw).strip().lower()
    if mode not in ("safe", "strict"):
        mode = "safe"

    # Book stream cadence (used for derived book thresholds + alpha).
    interval_raw = _first(
        c
        [
            "book_stream_interval_ms"
            "BOOK_STREAM_INTERVAL_MS"
            "book_interval_ms"
            "BOOK_INTERVAL_MS"
        ]
    )
    book_stream_interval_ms = _safe_int(interval_raw, 100)
    if book_stream_interval_ms <= 0:
        book_stream_interval_ms = 100

    # --- Tick gap p95 matrix (ms) ---
    if mode == "strict":
        gap_soft_ms = 3000
        gap_hard_ms = 15000
        gap_extreme_ms = 20000
        min_samples = 50
        tick_soft = 0.05
        tick_hard = 0.15
    else:
        gap_soft_ms = 5000
        gap_hard_ms = 20000
        gap_extreme_ms = 30000
        min_samples = 50
        tick_hard = 0.25
        tick_soft = max(0.02, 0.5 * tick_hard)

    # --- Optional overrides (cfg/env) ---
    # Accept several aliases to keep migrations painless.
    gap_soft_ms = _safe_int(
        _first(c, ["dq_gap_p95_soft_ms", "dq_gap_soft_ms", "DQ_GAP_P95_SOFT_MS", "DQ_GAP_SOFT_MS"])
        or gap_soft_ms
        gap_soft_ms
    )
    gap_hard_ms = _safe_int(
        _first(c, ["dq_gap_p95_hard_ms", "dq_gap_hard_ms", "DQ_GAP_P95_HARD_MS", "DQ_GAP_HARD_MS"])
        or gap_hard_ms
        gap_hard_ms
    )
    gap_extreme_ms = _safe_int(
        _first(c, ["dq_gap_p95_extreme_ms", "dq_gap_extreme_ms", "DQ_GAP_P95_EXTREME_MS", "DQ_GAP_EXTREME_MS"])
        or gap_extreme_ms
        gap_extreme_ms
    )
    min_samples = _safe_int(
        _first(c, ["dq_gap_p95_min_samples", "dq_gap_min_samples", "DQ_GAP_P95_MIN_SAMPLES", "DQ_GAP_MIN_SAMPLES"])
        or min_samples
        min_samples
    )

    tick_hard = float(
        _safe_float(_first(c, ["dq_tick_missing_seq_ema_hard", "DQ_TICK_MISSING_SEQ_EMA_HARD", "dq_tick_hard"]) or tick_hard, tick_hard)
    )
    tick_soft = float(
        _safe_float(_first(c, ["dq_tick_missing_seq_ema_soft", "DQ_TICK_MISSING_SEQ_EMA_SOFT", "dq_tick_soft"]) or tick_soft, tick_soft)
    )

    # --- Book thresholds: may use its own mode (BOOK_SEQ_MODE) ---
    book_mode_raw = _first(c, ["book_seq_mode", "BOOK_SEQ_MODE"]) or mode
    book_mode = str(book_mode_raw).strip().lower()
    if book_mode not in ("safe", "strict"):
        book_mode = mode

    book_hard_default = _derive_book_hard(book_mode, book_stream_interval_ms)
    book_hard = float(
        _safe_float(
            _first(c, ["dq_book_missing_seq_ema_hard", "DQ_BOOK_MISSING_SEQ_EMA_HARD", "dq_book_hard"])
            or book_hard_default
            book_hard_default
        )
    )

    # Soft ratio:
    #  - strict: ~0.30*hard (0.10 -> 0.03) to match matrix intent
    #  - safe:   max(0.02, 0.50*hard) (0.25 -> 0.125 ≈ 0.12)
    if book_mode == "strict":
        book_soft_default = max(0.02, 0.30 * float(book_hard))
    else:
        book_soft_default = max(0.02, 0.50 * float(book_hard))

    book_soft = float(
        _safe_float(
            _first(c, ["dq_book_missing_seq_ema_soft", "DQ_BOOK_MISSING_SEQ_EMA_SOFT", "dq_book_soft"])
            or book_soft_default
            book_soft_default
        )
    )

    # --- EMA alpha (single SoT) ---
    alpha_raw = _first(c, ["dq_book_seq_ema_alpha", "DQ_BOOK_SEQ_EMA_ALPHA", "book_missing_seq_ema_alpha", "BOOK_MISSING_SEQ_EMA_ALPHA"]) or 0.0
    a = float(_safe_float(alpha_raw, 0.0))
    if 0.0 < a <= 1.0:
        book_seq_ema_alpha = a
    else:
        book_seq_ema_alpha = float(derive_book_seq_ema_alpha(book_stream_interval_ms))

    # Final clamps (avoid pathological configs)
    if gap_soft_ms < 0:
        gap_soft_ms = 0
    if gap_hard_ms < gap_soft_ms:
        gap_hard_ms = gap_soft_ms
    if gap_extreme_ms < gap_hard_ms:
        gap_extreme_ms = gap_hard_ms
    if min_samples < 1:
        min_samples = 1

    tick_soft = float(max(0.0, min(1.0, tick_soft)))
    tick_hard = float(max(tick_soft, min(1.0, tick_hard)))
    book_soft = float(max(0.0, min(1.0, book_soft)))
    book_hard = float(max(book_soft, min(1.0, book_hard)))

    return DQThresholds(
        mode=str(mode)
        gap_soft_ms=int(gap_soft_ms)
        gap_hard_ms=int(gap_hard_ms)
        gap_extreme_ms=int(gap_extreme_ms)
        min_samples=int(min_samples)
        tick_soft=float(tick_soft)
        tick_hard=float(tick_hard)
        book_soft=float(book_soft)
        book_hard=float(book_hard)
        book_stream_interval_ms=int(book_stream_interval_ms)
        book_seq_ema_alpha=float(book_seq_ema_alpha)
    )
