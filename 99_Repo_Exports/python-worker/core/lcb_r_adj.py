from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Tuple
import math

def _f(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def _i(x: Any, d: int = 0) -> int:
    try: return int(x)
    except Exception: return d

def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

@dataclass
class PenaltyCfg:
    # lambda weights (units: R)
    lam_spread: float = 0.03
    lam_pressure: float = 0.05
    lam_cooldown: float = 0.04
    lam_bookstale: float = 0.03
    lam_unstable: float = 0.03
    lam_news: float = 0.05

    # normalizers
    pressure_hi_sps: float = 0.08
    cooldown_hi_sps: float = 0.06
    obi_ttl_ms: int = 5000

def compute_r_and_adj(ev: Dict[str, Any], cfg: PenaltyCfg) -> Tuple[float, float, Dict[str, float]]:
    """
    R = pnl / risk_usd
    R_adj = R - penalties (clipped)
    Penalties are built from entry-time microstructure proxies.
    """
    pnl = _f(ev.get("pnl", 0.0), 0.0)
    risk = _f(ev.get("risk_usd", 0.0), 0.0)
    if risk <= 1e-12:
        return 0.0, 0.0, {"p_spread":0.0,"p_pressure":0.0,"p_cooldown":0.0,"p_bookstale":0.0,"p_unstable":0.0,"p_news":0.0}
    R = pnl / risk

    spread_z = _f(ev.get("entry_spread_z", 0.0), 0.0)
    pressure = _f(ev.get("entry_pressure_sps", 0.0), 0.0)
    cooldown = _f(ev.get("entry_cooldown_sps", 0.0), 0.0)
    obi_age = float(_i(ev.get("entry_obi_age_ms", 0), 0))
    unstable = 1.0 if _i(ev.get("entry_abs_th_unstable", 0), 0) == 1 else 0.0
    news = 1.0 if _i(ev.get("entry_news_blocked", 0), 0) == 1 else 0.0

    # normalize + clip (robust)
    p_spread = _clamp(max(0.0, spread_z), 0.0, 4.0)
    denom_p = max(cfg.pressure_hi_sps, 1e-9)
    p_pressure = _clamp(pressure / denom_p, 0.0, 3.0)
    denom_c = max(cfg.cooldown_hi_sps, 1e-9)
    p_cooldown = _clamp(cooldown / denom_c, 0.0, 3.0)
    denom_o = max(float(cfg.obi_ttl_ms), 1.0)
    p_bookstale = _clamp(obi_age / denom_o, 0.0, 3.0)
    p_unstable = unstable
    p_news = news

    pen = (
        cfg.lam_spread * p_spread
        + cfg.lam_pressure * p_pressure
        + cfg.lam_cooldown * p_cooldown
        + cfg.lam_bookstale * p_bookstale
        + cfg.lam_unstable * p_unstable
        + cfg.lam_news * p_news
    )
    R_adj = R - pen
    return float(R), float(R_adj), {
        "p_spread": float(p_spread),
        "p_pressure": float(p_pressure),
        "p_cooldown": float(p_cooldown),
        "p_bookstale": float(p_bookstale),
        "p_unstable": float(p_unstable),
        "p_news": float(p_news),
        "pen": float(pen),
    }

@dataclass
class RegimeThresholds:
    min_n: int
    z: float
    margin: float
    # tail requirements (worst-K mean LCB)
    min_tail_n: int
    tail_z: float

def thresholds_for(regime: str, scenario: str = "na") -> RegimeThresholds:
    rg = (regime or "na").lower()
    scn = (scenario or "na").lower()
    # defaults
    t = RegimeThresholds(min_n=30, z=1.28, margin=0.05, min_tail_n=20, tail_z=1.28)
    if rg in ("thin","news","illiquid"):
        t = RegimeThresholds(min_n=60, z=1.65, margin=0.07, min_tail_n=40, tail_z=1.65)
    elif rg in ("range",):
        t = RegimeThresholds(min_n=50, z=1.28, margin=0.05, min_tail_n=35, tail_z=1.28)
    elif rg in ("trend","trending_bull","trending_bear"):
        t = RegimeThresholds(min_n=40, z=1.28, margin=0.04, min_tail_n=25, tail_z=1.28)
    # scenario tweak: reversal is more variable => require slightly more samples
    if scn == "reversal":
        t.min_n = int(max(t.min_n, t.min_n + 10))
        t.min_tail_n = int(max(t.min_tail_n, t.min_tail_n + 10))
    return t

def lcb(mean: float, std: float, n: int, z: float) -> float:
    if n <= 1:
        return -999.0
    se = std / math.sqrt(float(n))
    return float(mean - z * se)
