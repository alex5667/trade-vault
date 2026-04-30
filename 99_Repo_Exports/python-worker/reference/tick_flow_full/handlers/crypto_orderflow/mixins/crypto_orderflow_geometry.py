"""
Geometry and liquidity context logic for CryptoOrderFlowHandler.

This module contains geometry scoring and liquidity context management.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from handlers.crypto_orderflow.logging.logging_utils import _ctx_quality_flags
from signal_scoring.reason_registry import normalize_reason


class CryptoOrderFlowGeometryMixin:
    """
    Mixin class containing geometry and liquidity context logic for CryptoOrderFlowHandler.
    """

    def _update_geometry_liquidity_context(self, ctx: Any) -> None:
        """
        Called from base_handler at bucket boundary right before signal generation.

        Policy:
          - HTF missing => geometry_score = GEOMETRY_MISSING_SCORE01 (neutral), NO veto
          - Fill:
              ctx.geo_zone_hits: list[dict] (wire-friendly)
              ctx.geo_zone_hit: top-1 dict
              ctx.geometry_score: float (0..1)
        """
        if not self._geometry_enabled:
            return
        try:
            price = float(getattr(ctx, "price", 0.0) or 0.0)
            if price <= 0:
                return
            atr = float(getattr(ctx, "atr", 0.0) or 0.0)
        except Exception:
            return

        snap = getattr(ctx, "geometry", None)
        if snap is None:
            # missing HTF/geometry provider: neutral score, add quality flag
            setattr(ctx, "geometry_score", float(self._geometry_missing_score))
            flags = getattr(ctx, "data_quality_flags", None)
            if isinstance(flags, list):
                flags.append("missing_htf")
            return

        # Accept multiple snapshot shapes; normalize to a list of hits.
        hits: List[Dict[str, Any]] = []
        raw_hits = getattr(snap, "geo_zone_hits", None) or getattr(snap, "hits", None) or getattr(snap, "zones", None)
        if raw_hits is None:
            raw_hits = []

        def _to_hit(obj: Any) -> Optional[Dict[str, Any]]:
            try:
                zone_type = str(getattr(obj, "zone_type", None) or getattr(obj, "type", None) or "")
                strength = float(getattr(obj, "zone_strength", None) or getattr(obj, "strength", None) or 0.0)
                level = float(getattr(obj, "level_price", None) or getattr(obj, "price", None) or 0.0)
                if level <= 0:
                    return None
                dist_bps = abs(price - level) / price * 10_000.0
                dist_rel_atr = (abs(price - level) / atr) if atr > 0 else None
                return {
                    "zone_type": zone_type
                    "zone_strength": strength
                    "level_price": level
                    "dist_bps": dist_bps
                    "dist_rel_atr": dist_rel_atr
                }
            except Exception:
                return None

        for z in list(raw_hits)[:64]:
            h = _to_hit(z)
            if h is not None:
                hits.append(h)

        # geometry_score: monotone in distance (closer => higher), strength (stronger => higher).
        def _norm_strength(s: float) -> float:
            # robust clamp: treat negative as 0, cap at 1
            if not math.isfinite(s):
                return 0.0
            return max(0.0, min(1.0, s))

        def _dist_score(d_bps: float) -> float:
            # 0 bps => 1.0 ; 50 bps => ~0.0
            if not math.isfinite(d_bps):
                return 0.0
            return max(0.0, min(1.0, 1.0 - (d_bps / 50.0)))

        best: Optional[Dict[str, Any]] = None
        best_score = -1.0
        for h in hits:
            ds = _dist_score(float(h.get("dist_bps") or 1e9))
            st = _norm_strength(float(h.get("zone_strength") or 0.0))
            score = max(0.0, min(1.0, 0.75 * ds + 0.25 * st))
            h["score01"] = score
            if score > best_score:
                best_score = score
                best = h

        setattr(ctx, "geo_zone_hits", hits)
        setattr(ctx, "geo_zone_hit", best)
        setattr(ctx, "geometry_score", float(best_score if best is not None else self._geometry_missing_score))
