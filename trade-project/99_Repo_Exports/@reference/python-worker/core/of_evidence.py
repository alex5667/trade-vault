from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import math


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


def _get(obj: Any, name: str, default: Any = None) -> Any:
    """Dict-or-attr access helper (replay-friendly)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def compute_sweep_recent(
    *,
    now_ts_ms: int,
    last_sweep: Any,
    cfg: Dict[str, Any],
    indicators: Dict[str, Any],
) -> bool:
    """
    sweep_recent must be time-bounded. Otherwise reversal branch becomes "sticky".

    last_sweep is expected to have:
      - ts_ms (int ms)
      - kind (optional: EQH/EQL)
      - direction_bias (optional: LONG/SHORT)
    """
    if last_sweep is None:
        indicators["sweep_age_ms"] = -1
        return False

    ts = _i(_get(last_sweep, "ts_ms", 0), 0)
    age = (now_ts_ms - ts) if ts > 0 else 10**9
    indicators["sweep_age_ms"] = int(age)

    valid_ms = _i(cfg.get("sweep_valid_ms", 120_000), 120_000)
    if not (0 <= age <= valid_ms):
        return False

    try:
        indicators["sweep_kind"] = _s(_get(last_sweep, "kind", ""), "")
    except Exception:
        pass
    try:
        indicators["sweep_dir_bias"] = _s(_get(last_sweep, "direction_bias", ""), "")
    except Exception:
        pass
    return True


def compute_reclaim_recent(
    *,
    direction: str,
    now_ts_ms: int,
    last_reclaim: Any,
    cfg: Dict[str, Any],
    indicators: Dict[str, Any],
) -> Tuple[bool, int]:
    """
    Reclaim evidence: fresh + direction match.

    last_reclaim is expected to have:
      - ts_ms (int ms)
      - hold_bars (int)
      - direction_bias (str: LONG/SHORT)
      - level/pool_id optional
    """
    if last_reclaim is None:
        indicators["reclaim_age_ms"] = -1
        return False, 0

    ts = _i(_get(last_reclaim, "ts_ms", 0), 0)
    age = (now_ts_ms - ts) if ts > 0 else 10**9
    indicators["reclaim_age_ms"] = int(age)

    valid_ms = _i(cfg.get("reclaim_signal_valid_ms", 120_000), 120_000)
    if not (0 <= age <= valid_ms):
        return False, 0

    bias = _s(_get(last_reclaim, "direction_bias", ""), "").upper()
    if bias and bias != str(direction).upper():
        return False, 0

    hold_bars = _i(_get(last_reclaim, "hold_bars", 0), 0)
    if hold_bars <= 0:
        hold_bars = _i(cfg.get("reclaim_hold_bars", 2), 2)

    # Optional diagnostic fields
    try:
        indicators["reclaim_level"] = float(_get(last_reclaim, "level", 0.0) or 0.0)
    except Exception:
        pass
    try:
        indicators["reclaim_pool_id"] = _s(_get(last_reclaim, "pool_id", ""), "")
    except Exception:
        pass

    return True, hold_bars


def compute_absorption_flags(
    *,
    direction: str,
    absorption: Optional[Dict[str, Any]],
    cfg: Dict[str, Any],
    indicators: Dict[str, Any],
) -> Tuple[bool, float]:
    """
    Absorption evidence is tick-local (no staleness needed).

    absorption dict expected to include:
      - side: "LONG"/"SHORT" or "buy"/"sell" style
      - volume: float

    We apply an optional minimum volume threshold to avoid tiny noisy triggers.
    """
    if not absorption or not absorption.get("side"):
        return False, 0.0

    side = _s(absorption.get("side"), "").upper()
    vol = _f(absorption.get("volume", 0.0), 0.0)
    indicators["absorption_volume"] = float(vol)

    # normalize side names if needed
    if side in ("BUY", "BID"):
        side = "LONG"
    if side in ("SELL", "ASK"):
        side = "SHORT"

    if side != str(direction).upper():
        return False, vol

    vmin = _f(cfg.get("absorption_min_volume", 0.0), 0.0)
    ok = vol >= vmin
    return ok, vol
