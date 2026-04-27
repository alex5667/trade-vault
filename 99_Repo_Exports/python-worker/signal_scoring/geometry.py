from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._helpers import _is_finite


def normalize_zone_strength(raw_strength: Any) -> float:
    """
    Normalize strength to [0..1].
    Accepts:
      - already normalized [0..1]
      - percent-like [0..100]
      - None/NaN -> 0
    """
    if not _is_finite(raw_strength):
        return 0.0
    s = float(raw_strength)
    # Heuristic: treat >1 as percent-like scale.
    if s > 1.0:
        s = s / 100.0
    return max(0.0, min(1.0, s))


def distance_to_score(
    *,
    dist_bps: float,
    dist_rel_atr: Optional[float],
    bps_half_life: float = 25.0,
    atr_half_life: float = 0.35,
) -> float:
    """
    Monotonic decreasing distance->score mapping in [0..1].

    We combine bps distance and ATR-relative distance conservatively via max(),
    so score is monotonic in BOTH metrics (if either worsens -> score doesn't increase).
    """
    if not _is_finite(dist_bps) or dist_bps < 0:
        return 0.0
    bps_hl = max(1e-9, float(bps_half_life))
    dist_eff = dist_bps / bps_hl

    if dist_rel_atr is not None and _is_finite(dist_rel_atr) and dist_rel_atr >= 0:
        atr_hl = max(1e-9, float(atr_half_life))
        dist_eff = max(dist_eff, float(dist_rel_atr) / atr_hl)

    # 1/(1+x) is stable, bounded, monotonic decreasing.
    return 1.0 / (1.0 + dist_eff)


def geometry_score(
    *,
    zone_strength01: float,
    dist_bps: float,
    dist_rel_atr: Optional[float],
    bps_half_life: float = 25.0,
    atr_half_life: float = 0.35,
) -> float:
    """
    Final geometry score in [0..1], monotonic:
      - decreases with distance (bps / atr-relative)
      - increases with strength
    """
    s = max(0.0, min(1.0, float(zone_strength01)))
    dscore = distance_to_score(
        dist_bps=dist_bps,
        dist_rel_atr=dist_rel_atr,
        bps_half_life=bps_half_life,
        atr_half_life=atr_half_life,
    )
    return max(0.0, min(1.0, s * dscore))


@dataclass(frozen=True)
class GeoZoneHit:
    zone_type: str
    zone_strength: float  # normalized [0..1]
    zone_price: float
    dist_bps: float
    dist_rel_atr: Optional[float]
    score: float
    meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # keep meta last / readable
        d["meta"] = dict(self.meta or {})
        return d


def compute_geo_hits(
    *,
    price: float,
    atr: Optional[float],
    zones: Iterable[Dict[str, Any]],
    bps_half_life: float = 25.0,
    atr_half_life: float = 0.35,
) -> List[GeoZoneHit]:
    """
    zones: iterable of dicts with at least:
      - type (or zone_type)
      - price (or level/zone_price)
      - strength (or zone_strength)
    Returns sorted hits (best first).
    """
    if not _is_finite(price) or float(price) <= 0:
        return []
    px = float(price)

    atr_f: Optional[float] = None
    if _is_finite(atr) and float(atr) > 0:
        atr_f = float(atr)

    hits: List[GeoZoneHit] = []

    for z in zones:
        if not isinstance(z, dict):
            continue
        zt = (z.get("zone_type") or z.get("type") or z.get("kind") or "unknown")
        lvl = z.get("zone_price")
        if lvl is None:
            lvl = z.get("price")
        if lvl is None:
            lvl = z.get("level")
        if not _is_finite(lvl):
            continue
        lvl_f = float(lvl)
        if lvl_f <= 0:
            continue

        raw_strength = z.get("zone_strength")
        if raw_strength is None:
            raw_strength = z.get("strength")
        s01 = normalize_zone_strength(raw_strength)

        dist_bps = abs(px - lvl_f) / px * 10_000.0
        dist_rel_atr = None
        if atr_f is not None:
            dist_rel_atr = abs(px - lvl_f) / atr_f

        sc = geometry_score(
            zone_strength01=s01,
            dist_bps=dist_bps,
            dist_rel_atr=dist_rel_atr,
            bps_half_life=bps_half_life,
            atr_half_life=atr_half_life,
        )

        meta = dict(z.get("meta") or {})
        # preserve original fields for debugging if needed
        if "source" in z:
            meta.setdefault("source", z.get("source"))

        hits.append(
            GeoZoneHit(
                zone_type=str(zt),
                zone_strength=s01,
                zone_price=lvl_f,
                dist_bps=dist_bps,
                dist_rel_atr=dist_rel_atr,
                score=sc,
                meta=meta,
            )
        )

    hits.sort(key=lambda h: (h.score, -h.zone_strength, -h.dist_bps), reverse=True)
    return hits


def compute_geometry_context(
    *,
    price: Any,
    atr: Any,
    zones: Iterable[Dict[str, Any]],
    bps_half_life: float = 25.0,
    atr_half_life: float = 0.35,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], float]:
    """
    Returns:
      - geo_zone_hits: list[dict] (json-friendly)
      - geo_zone_hit: top-1 dict or None
      - geometry_score: top-1 score in [0..1]
    """
    if not _is_finite(price) or float(price) <= 0:
        return ([], None, 0.0)

    hits = compute_geo_hits(
        price=float(price),
        atr=float(atr) if _is_finite(atr) else None,
        zones=zones,
        bps_half_life=bps_half_life,
        atr_half_life=atr_half_life,
    )
    if not hits:
        return ([], None, 0.0)

    hit_dicts = [h.to_dict() for h in hits]
    top = hit_dicts[0]
    top_score = float(top.get("score", 0.0) or 0.0)
    return (hit_dicts, top, max(0.0, min(1.0, top_score)))
