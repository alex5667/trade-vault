from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
import json

def _i(x: Any, d: int = 0) -> int:
    try: return int(x)
    except Exception: return d

def _f(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def _s(x: Any, d: str = "") -> str:
    try: return str(x) if x is not None else d
    except Exception: return d

def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

@dataclass
class SplitABC:
    a: float = 0.80
    b: float = 0.10
    c: float = 0.10

    def normalize(self) -> "SplitABC":
        a = _clamp(float(self.a), 0.0, 1.0)
        b = _clamp(float(self.b), 0.0, 1.0)
        c = _clamp(float(self.c), 0.0, 1.0)
        s = a + b + c
        if s <= 1e-12:
            return SplitABC(1.0, 0.0, 0.0)
        a, b, c = a / s, b / s, c / s
        # keep A as residual if needed (small numeric guard)
        a = max(0.0, 1.0 - b - c)
        return SplitABC(a=a, b=b, c=c)

@dataclass
class EntryPolicyOverridesV1:
    """
    Strict overrides schema for EntryPolicy / AB routing.
    Stored as JSON in Redis:
      cfg:entry_policy:overrides:v1
      cfg:entry_policy:overrides:v1:{group}   (optional per-group)
    """
    ver: int = 1
    enabled: int = 1
    updated_ts_ms: int = 0

    # Freeze override
    freeze_active: int = 0
    freeze_mode: str = "none"     # "none"|"shadow"|"hard"
    freeze_reason: str = ""
    freeze_until_ts_ms: int = 0

    # Active arm override (optional hard-force)
    force_active_arm: str = ""    # "A"|"B"|"C"|"" (empty => no force)
    active_arm_min_hold_ms: int = 0

    # Contextual split policy
    split_range: SplitABC = field(default_factory=lambda: SplitABC(0.80, 0.10, 0.10))
    split_trend: SplitABC = field(default_factory=lambda: SplitABC(0.85, 0.075, 0.075))
    split_thin: SplitABC  = field(default_factory=lambda: SplitABC(0.70, 0.15, 0.15))
    split_chop: SplitABC  = field(default_factory=lambda: SplitABC(0.95, 0.05, 0.00))

    # Triggers / thresholds
    adx_chop_lo_q: float = 0.40
    pressure_hi_sps: float = 0.08     # signals/sec proxy threshold
    spread_z_hi: float = 2.0
    unstable_th_hi: int = 1           # abs_lvl_th_unstable==1 => strict
    news_blocked_hi: int = 1          # if bundle says blocked

    # Hold-down for overrides application (avoid rapid changes)
    overrides_hold_down_ms: int = 60_000

    # Hysteresis for winner switching decisions (extra safety)
    winner_lcb_margin: float = 0.05
    winner_min_n: int = 30

    extra: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_json(raw: str) -> Tuple[Optional["EntryPolicyOverridesV1"], str]:
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None, "not_dict"
        except Exception:
            return None, "bad_json"

        try:
            o = EntryPolicyOverridesV1()
            o.ver = _i(d.get("ver", 1), 1)
            o.enabled = 1 if _i(d.get("enabled", 1), 1) else 0
            o.updated_ts_ms = _i(d.get("updated_ts_ms", 0), 0)

            o.freeze_active = 1 if _i(d.get("freeze_active", 0), 0) else 0
            o.freeze_mode = _s(d.get("freeze_mode", "none"), "none").strip().lower()
            o.freeze_reason = _s(d.get("freeze_reason", ""), "").strip()
            o.freeze_until_ts_ms = _i(d.get("freeze_until_ts_ms", 0), 0)

            o.force_active_arm = _s(d.get("force_active_arm", ""), "").strip().upper()
            o.active_arm_min_hold_ms = max(0, _i(d.get("active_arm_min_hold_ms", 0), 0))

            def _split(key: str, default: SplitABC) -> SplitABC:
                x = d.get(key, None)
                if not isinstance(x, dict):
                    return default.normalize()
                s = SplitABC(a=_f(x.get("a", default.a), default.a)
                             b=_f(x.get("b", default.b), default.b)
                             c=_f(x.get("c", default.c), default.c))
                return s.normalize()

            o.split_range = _split("split_range", o.split_range)
            o.split_trend = _split("split_trend", o.split_trend)
            o.split_thin  = _split("split_thin",  o.split_thin)
            o.split_chop  = _split("split_chop",  o.split_chop)

            o.adx_chop_lo_q = _clamp(_f(d.get("adx_chop_lo_q", o.adx_chop_lo_q), o.adx_chop_lo_q), 0.0, 1.0)
            o.pressure_hi_sps = max(0.0, _f(d.get("pressure_hi_sps", o.pressure_hi_sps), o.pressure_hi_sps))
            o.spread_z_hi = max(0.0, _f(d.get("spread_z_hi", o.spread_z_hi), o.spread_z_hi))
            o.unstable_th_hi = 1 if _i(d.get("unstable_th_hi", o.unstable_th_hi), o.unstable_th_hi) else 0
            o.news_blocked_hi = 1 if _i(d.get("news_blocked_hi", o.news_blocked_hi), o.news_blocked_hi) else 0

            o.overrides_hold_down_ms = max(0, _i(d.get("overrides_hold_down_ms", o.overrides_hold_down_ms), o.overrides_hold_down_ms))
            o.winner_lcb_margin = max(0.0, _f(d.get("winner_lcb_margin", o.winner_lcb_margin), o.winner_lcb_margin))
            o.winner_min_n = max(1, _i(d.get("winner_min_n", o.winner_min_n), o.winner_min_n))

            extra = d.get("extra", {})
            if isinstance(extra, dict):
                o.extra = extra
            return o, "ok"
        except Exception:
            return None, "parse_fail"

    def validate(self) -> Tuple[bool, str]:
        if self.ver != 1:
            return False, "bad_ver"
        if self.freeze_mode not in ("none", "shadow", "hard"):
            self.freeze_mode = "none"
        if self.force_active_arm and self.force_active_arm not in ("A", "B", "C"):
            self.force_active_arm = ""
        # normalize splits again (defensive)
        self.split_range = self.split_range.normalize()
        self.split_trend = self.split_trend.normalize()
        self.split_thin  = self.split_thin.normalize()
        self.split_chop  = self.split_chop.normalize()
        return True, "ok"
