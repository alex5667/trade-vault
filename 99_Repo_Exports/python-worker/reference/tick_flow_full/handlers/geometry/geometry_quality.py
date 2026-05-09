from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any


def clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass(frozen=True)
class GeometryAssessment:
    available: bool
    veto: bool
    score01: float
    flags: list[str]
    reason: str


@dataclass(frozen=True)
class GeoHit:
    zone_type: str
    zone_strength: float
    dist_bps: float
    dist_rel_atr: float | None


def geometry_score_from_hit(hit: GeoHit) -> float:
    """
    3.4: единая нормализация, монотонная по distance и strength:
      - strength↑ => score↑
      - dist_bps↓ => score↑
    """
    s = float(hit.zone_strength)
    if not math.isfinite(s):
        s = 0.0
    s01 = clamp01(s)  # если strength уже 0..1; если нет — лучше нормализовать upstream

    d = float(hit.dist_bps)
    if not math.isfinite(d) or d < 0.0:
        d = 10_000.0
    # 0 bps => 1, 25 bps => ~0.7, 75 bps => ~0.25, 150 bps => ~0.1
    dist01 = clamp01(1.0 - (d / 150.0))

    # итог: мультипликативно (жёстче), чтобы слабая зона или далеко => резко падает
    return clamp01((0.15 + 0.85 * s01) * (0.10 + 0.90 * dist01))


class GeometryQualityPolicy:
    """
    4.1: HTF levels недоступны => geometry_score=0.1 (нейтраль), без veto.
    """

    def __init__(self, *, missing_score01: float | None = None):
        if missing_score01 is None:
            missing_score01 = float(os.getenv("GEO_MISSING_SCORE01", "0.1"))
        self.missing_score01 = float(missing_score01)

    def assess(self, *, ctx: Any) -> GeometryAssessment:
        flags: list[str] = []
        geo = getattr(ctx, "geometry", None)
        hit = getattr(ctx, "geo_zone_hit", None)
        hits = getattr(ctx, "geo_zone_hits", None)

        available = geo is not None or hit is not None or (isinstance(hits, list) and len(hits) > 0)
        if not available:
            flags.append("htf_missing")
            return GeometryAssessment(
                available=False,
                veto=False,
                score01=self.missing_score01,
                flags=flags,
                reason="htf_missing_neutral",
            )

        # если уже рассчитан geometry_score — используем его как truth source
        g = getattr(ctx, "geometry_score", None)
        try:
            if g is not None:
                gf = float(g)
                if math.isfinite(gf):
                    return GeometryAssessment(available=True, veto=False, score01=clamp01(gf), flags=flags, reason="geo_ok_ctx")
        except Exception:
            pass

        # иначе пытаемся собрать top-1 hit из ctx.geo_zone_hit (если это dict/obj)
        if hit is not None:
            try:
                zone_type = str(getattr(hit, "zone_type", None) or hit.get("zone_type") or "unknown")
                zone_strength = float(getattr(hit, "zone_strength", None) or hit.get("zone_strength") or 0.0)
                dist_bps = float(getattr(hit, "dist_bps", None) or hit.get("dist_bps") or 10_000.0)
                dist_rel_atr = getattr(hit, "dist_rel_atr", None) or hit.get("dist_rel_atr")
                dist_rel_atr = float(dist_rel_atr) if dist_rel_atr is not None else None
                score = geometry_score_from_hit(GeoHit(zone_type, zone_strength, dist_bps, dist_rel_atr))
                return GeometryAssessment(available=True, veto=False, score01=score, flags=flags, reason="geo_ok_hit")
            except Exception:
                flags.append("geo_bad_hit")

        # fallback: available but no parsable hit => мягко нейтрально
        flags.append("geo_no_hit")
        return GeometryAssessment(available=True, veto=False, score01=self.missing_score01, flags=flags, reason="geo_no_hit_neutral")


def apply_geometry_policy_to_ctx(*, ctx: Any, assessment: GeometryAssessment) -> None:
    try:
        arr = getattr(ctx, "data_quality_flags", None)
        if arr is None:
            ctx.data_quality_flags = []
            arr = ctx.data_quality_flags
        if isinstance(arr, list):
            for f in assessment.flags:
                if f not in arr:
                    arr.append(f)
    except Exception:
        return
