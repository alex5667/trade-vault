from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

"""
Confidence pct provider (0..100) — single hot-path callable.

Goal:
  - убрать duck-typing и перебор сигнатур ИЗ ГОРЯЧЕГО ПУТИ;
  - один раз “связать” calibrator API в __init__;
  - дальше в генерации сигналов дергать только self._conf_pct_fn(...).

Supports real project calibrators:
  - RollingPercentileCalibrator  (handlers/crypto_orderflow_calibration.py)
  - CalibrationService           (handlers/calibration_service.py)  [если у него есть подходящий метод]

Design:
  build_confidence_pct_fn(calibrator, cap_pct) -> fn(kind, symbol, final_score, ts_ms) -> pct
  - все проверки наличия методов делаем ОДИН РАЗ при сборке fn
  - fn всегда fail-open и всегда возвращает float в [0..cap_pct]
"""


def _clamp_pct(v: Any, *, cap_pct: float) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if not math.isfinite(x):
        return 0.0
    if x < 0.0:
        return 0.0
    cap = float(cap_pct) if math.isfinite(cap_pct) and cap_pct > 0 else 95.0
    return float(min(x, cap))


def _fallback_pct_from_final_score(final_score: float, *, cap_pct: float) -> float:
    """
    Fallback only.
    Монотонная, гладкая, детерминированная функция  abs(final_score) -> [0..cap_pct].
    (Это НЕ модель — только безопасный фоллбек.)
    """
    fs = float(final_score)
    if not math.isfinite(fs):
        return 0.0
    x = abs(fs)
    # 0..inf -> 0..1 via 1-exp(-x/k)
    k = 8.0
    conf01 = 1.0 - math.exp(-max(0.0, x) / k)
    return _clamp_pct(conf01 * 100.0, cap_pct=cap_pct)


def build_confidence_pct_fn(
    calibrator: Any | None,
    *,
    cap_pct: float = 95.0,
) -> Callable[[str, str, float, int], float]:
    """
    Returns a single callable for the hot path:
      fn(kind, symbol, final_score, ts_ms) -> confidence_pct [0..cap_pct]

    Binding is performed ONCE. No hasattr/TypeError probing in the hot path.
    """
    if calibrator is None:
        return lambda kind, symbol, final_score, ts_ms: _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)

    # ------------------------------------------------------------------
    # 1) RollingPercentileCalibrator (most used for confidence_pct)
    # Expected behavior: abs(final_score) -> percentile rank (0..100)
    #
    # We intentionally DO NOT depend on an exact method name in the handler.
    # Instead we bind one of the known method shapes here ONCE.
    # ------------------------------------------------------------------
    fn = None

    # Preferred: confidence_pct(kind=..., symbol=..., final_score=..., ts_ms=...)
    m = getattr(calibrator, "confidence_pct", None)
    if callable(m):
        def _call_confidence_pct(kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
            try:
                v = m(kind=kind, symbol=symbol, final_score=float(final_score), ts_ms=int(ts_ms))
                return _clamp_pct(v, cap_pct=cap_pct)
            except Exception:
                return _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
        return _call_confidence_pct

    # Common in rolling-percentile implementations: pct(kind=..., symbol=..., value=...)
    m = getattr(calibrator, "pct", None)
    if callable(m):
        def _call_pct(kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
            try:
                v = m(kind=kind, symbol=symbol, value=float(final_score))
                return _clamp_pct(v, cap_pct=cap_pct)
            except Exception:
                return _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
        return _call_pct

    # Alternative naming: score_to_pct(kind, symbol, score) or score_to_confidence(...)
    m = getattr(calibrator, "score_to_pct", None)
    if callable(m):
        def _call_score_to_pct(kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
            try:
                v = m(kind=kind, symbol=symbol, score=float(final_score))
                return _clamp_pct(v, cap_pct=cap_pct)
            except Exception:
                return _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
        return _call_score_to_pct

    m = getattr(calibrator, "score_to_confidence", None)
    if callable(m):
        def _call_score_to_conf(kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
            try:
                v = m(score=float(final_score), kind=kind, symbol=symbol)
                return _clamp_pct(v, cap_pct=cap_pct)
            except Exception:
                return _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
        return _call_score_to_conf

    # ------------------------------------------------------------------
    # 2) CalibrationService (если вдруг он используется для confidence)
    # Expected: calibrate(kind, symbol, score, ts_ms) -> pct
    # ------------------------------------------------------------------
    m = getattr(calibrator, "calibrate", None)
    if callable(m):
        def _call_calibrate(kind: str, symbol: str, final_score: float, ts_ms: int) -> float:
            try:
                v = m(kind=(kind or ""), symbol=(symbol or ""), score=float(final_score), ts_ms=int(ts_ms))
                return _clamp_pct(v, cap_pct=cap_pct)
            except Exception:
                return _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
        return _call_calibrate

    # Unknown calibrator shape -> safe fallback.
    return lambda kind, symbol, final_score, ts_ms: _fallback_pct_from_final_score(final_score, cap_pct=cap_pct)
