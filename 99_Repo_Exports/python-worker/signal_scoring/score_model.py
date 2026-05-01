from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from ._helpers import _is_finite, _safe_float, _clamp


@dataclass(frozen=True)
class ScoreOutput:
    conf_factor: float
    final_score: float
    confidence_pct: float
    parts: Dict[str, Any]


class ScoreModel:
    """
    One-axis scoring contract:
      conf_factor ∈ [0..1]
      final_score = raw_score * conf_factor
      confidence_pct = calibration(final_score, kind, symbol)  (fallback mapping if no calibrator)
    """

    def __init__(self, cfg: Any, calibrator: Optional[Any] = None) -> None:
        self.cfg = cfg
        self.calibrator = calibrator

    def compute_conf_factor(
        self,
        *,
        ctx: Any,
        kind: Any,
        side: int,
        quality_flags: Dict[str, Any],
    ) -> Tuple[float, Dict[str, Any]]:
        # Base factor
        f = 1.0
        parts: Dict[str, Any] = {}

        # Fail-closed on hard veto flags
        if quality_flags.get("veto") is True:
            return 0.0, {"veto": True, "veto_reason": quality_flags.get("veto_reason", "")}

        # L2 contribution
        l2_ok = bool(quality_flags.get("l2_ok", True))
        l2_reason = str(quality_flags.get("l2_reason", ""))
        if not l2_ok:
            # stale L2 should crush confidence; other fails penalize sharply
            if l2_reason == "stale_l2":
                f *= float(getattr(self.cfg, "CONF_L2_STALE_FACTOR", 0.0))
            else:
                f *= float(getattr(self.cfg, "CONF_L2_FAIL_FACTOR", 0.25))
        else:
            f *= float(getattr(self.cfg, "CONF_L2_OK_FACTOR", 1.0))
        parts["l2_ok"] = l2_ok
        parts["l2_reason"] = l2_reason

        # Liquidity / book quality (expected [0..1], robust clamp)
        book_q = _safe_float(getattr(ctx, "book_quality", quality_flags.get("book_quality", 1.0)), 1.0)
        book_q = _clamp(book_q, 0.0, 1.0)
        f *= _clamp(book_q, 0.1, 1.0)  # do not zero out unless veto
        parts["book_quality"] = book_q

        # Micro quality (expected [0..1])
        micro_q = _safe_float(getattr(ctx, "micro_quality", quality_flags.get("micro_quality", 1.0)), 1.0)
        micro_q = _clamp(micro_q, 0.0, 1.0)
        f *= _clamp(micro_q, 0.2, 1.0)
        parts["micro_quality"] = micro_q

        # Regime penalty if ranging (optional)
        regime = getattr(ctx, "market_regime", None)
        regime_score = _safe_float(getattr(ctx, "market_regime_score", 0.0), 0.0)  # [-1..+1] or [0..1] depending
        parts["market_regime"] = str(regime) if regime is not None else ""
        parts["market_regime_score"] = regime_score

        # If you keep a boolean "is_range", use it; else infer from score
        is_range = bool(getattr(ctx, "is_range", False))
        if is_range:
            f *= float(getattr(self.cfg, "CONF_RANGE_FACTOR", 0.75))
            parts["range_gate"] = True
        else:
            parts["range_gate"] = False

        # Geometry (optional, [0..1])
        geom = _safe_float(quality_flags.get("geometry", getattr(ctx, "geometry_score", 1.0)), 1.0)
        geom = _clamp(geom, 0.0, 1.0)
        f *= _clamp(geom, 0.25, 1.0)
        parts["geometry_score"] = geom

        # L3 (optional, [0..1])
        l3 = _safe_float(quality_flags.get("l3", getattr(ctx, "l3_quality", 1.0)), 1.0)
        l3 = _clamp(l3, 0.0, 1.0)
        f *= _clamp(l3, 0.25, 1.0)
        parts["l3_quality"] = l3

        # Final clamp
        f = _clamp(f, 0.0, 1.0)
        parts["conf_factor"] = f
        return f, parts

    def calibrate_confidence_pct(self, *, ctx: Any, kind: Any, final_score: float) -> Tuple[float, Dict[str, Any]]:
        # Preferred: external calibration service (if present).
        # Contract: output should be [0..100] and stable even when calibrator missing.
        symbol = getattr(ctx, "symbol", None)
        parts: Dict[str, Any] = {"symbol": symbol, "kind": str(kind)}

        if self.calibrator is not None:
            # Try common calibrator shapes safely
            try:
                # 1) calibrate(kind, symbol, value) -> dict or float
                out = self.calibrator.calibrate(kind=kind, symbol=symbol, value=final_score)
                if isinstance(out, dict):
                    pct = float(out.get("confidence_pct", out.get("pct", 0.0)))
                    parts["calib"] = out
                    return _clamp(pct, 0.0, 100.0), parts
                pct = float(out)
                return _clamp(pct, 0.0, 100.0), parts
            except Exception as e:
                parts["calib_error"] = repr(e)

        # Fallback: simple monotonic mapping (minimal and deterministic)
        # Scale: abs(final_score) * k, clamp 0..100
        k = float(getattr(self.cfg, "CONFIDENCE_K", 35.0))
        pct = _clamp(abs(_safe_float(final_score, 0.0)) * k, 0.0, 100.0)
        parts["fallback_k"] = k
        return pct, parts

    def score(
        self,
        *,
        ctx: Any,
        kind: Any,
        side: int,
        raw_score: float,
        quality_flags: Dict[str, Any],
    ) -> ScoreOutput:
        raw_score = _safe_float(raw_score, 0.0)
        conf_factor, conf_parts = self.compute_conf_factor(ctx=ctx, kind=kind, side=side, quality_flags=quality_flags)
        final_score = raw_score * conf_factor
        confidence_pct, calib_parts = self.calibrate_confidence_pct(ctx=ctx, kind=kind, final_score=final_score)
        parts = {"conf": conf_parts, "calib": calib_parts, "raw_score": raw_score, "final_score": final_score}
        return ScoreOutput(conf_factor=conf_factor, final_score=final_score, confidence_pct=confidence_pct, parts=parts)
