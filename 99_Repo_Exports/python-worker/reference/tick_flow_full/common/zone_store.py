from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def _dist_bp(px: float, ref: float) -> float:
    if px <= 0 or ref <= 0:
        return 0.0
    mid = 0.5 * (abs(px) + abs(ref))
    if mid <= 0:
        return 0.0
    return float(10000.0 * abs(px - ref) / mid)


@dataclass
class Zone:
    id: str
    type: str              # "LEVEL"|"FVG"|"OB"|...
    src: str               # "daily"|"weekly"|"session"|...
    side: str              # "SUP"|"RES"|"MID"|"NA"
    px_lo: float
    px_hi: float
    ts_ms: int
    weight: float = 1.0
    meta: Dict[str, Any] = field(default_factory=dict)

    def clamp_ref_px(self, px: float) -> float:
        """Nearest point on band [px_lo, px_hi]. If inside, returns px itself (distance 0)."""
        lo = float(min(self.px_lo, self.px_hi))
        hi = float(max(self.px_lo, self.px_hi))
        if px <= 0:
            return 0.0
        if lo <= 0 or hi <= 0:
            return 0.0
        if lo <= px <= hi:
            return float(px)
        return float(lo if px < lo else hi)

    def dist_bp(self, px: float) -> float:
        ref = self.clamp_ref_px(px)
        if ref <= 0:
            return 0.0
        return _dist_bp(px, ref)

    def inside(self, px: float) -> bool:
        lo = float(min(self.px_lo, self.px_hi))
        hi = float(max(self.px_lo, self.px_hi))
        return px > 0 and lo > 0 and hi > 0 and lo <= px <= hi


@dataclass
class ZonePack:
    v: int
    symbol: str
    ts_ms: int
    zones: List[Zone]

    @staticmethod
    def from_json(raw: str) -> Optional["ZonePack"]:
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None
            v = _i(d.get("v", 1), 1)
            sym = _s(d.get("symbol", ""), "")
            ts = _i(d.get("ts_ms", 0), 0)
            zs = []
            for z in (d.get("zones") or []):
                if not isinstance(z, dict):
                    continue
                zs.append(
                    Zone(
                        id=_s(z.get("id", ""), "")
                        type=_s(z.get("type", "LEVEL"), "LEVEL")
                        src=_s(z.get("src", "na"), "na")
                        side=_s(z.get("side", "NA"), "NA")
                        px_lo=_f(z.get("px_lo", 0.0), 0.0)
                        px_hi=_f(z.get("px_hi", 0.0), 0.0)
                        ts_ms=_i(z.get("ts_ms", ts), ts)
                        weight=_f(z.get("weight", 1.0), 1.0)
                        meta=z.get("meta", {}) if isinstance(z.get("meta", {}), dict) else {}
                    )
                )
            if not sym:
                return None
            return ZonePack(v=v, symbol=sym, ts_ms=ts, zones=zs)
        except Exception:
            return None

    def nearest(self, px: float) -> Tuple[Optional[Zone], float, bool]:
        """Return (zone, dist_bp, inside_band). Tie-break: lower dist then higher weight."""
        best: Optional[Zone] = None
        best_d = 1e18
        best_inside = False
        best_w = -1.0
        for z in self.zones:
            d = z.dist_bp(px)
            inside = z.inside(px)
            # If inside => dist is 0 by construction
            if inside:
                d = 0.0
            if d < best_d - 1e-12:
                best = z
                best_d = d
                best_inside = inside
                best_w = float(z.weight)
            elif abs(d - best_d) <= 1e-12:
                # higher weight wins
                if float(z.weight) > best_w:
                    best = z
                    best_d = d
                    best_inside = inside
                    best_w = float(z.weight)
        if best is None or best_d >= 1e17:
            return None, 0.0, False
        return best, float(best_d), bool(best_inside)
