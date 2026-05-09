from __future__ import annotations

import math
from typing import Any


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


def compute_obi_flags(
    *,
    direction: str,
    now_ts_ms: int,
    last_event: dict[str, Any] | None,
    cfg: dict[str, Any],
    indicators: dict[str, Any],
) -> tuple[bool, bool, float, float]:
    """
    Single source of truth for OBI evidence.

    Returns:
      (obi_dir_ok, obi_stable, stable_secs, obi_val)

    Rules:
      - require event freshness: age_ms in [0..obi_event_ttl_ms]
      - require direction match: last_event.direction == direction
      - stable if stable_secs >= obi_stable_min_secs
    """
    obi_dir_ok = False
    obi_stable = False
    stable_secs = 0.0
    obi_val = 0.0

    # ----- FALLBACK TO DW OBI -----
    dw_obi_val = float(indicators.get("lob_dw_obi", 0.0) or 0.0)
    dw_obi_stable = int(indicators.get("lob_dw_obi_stable", 0) or 0) == 1
    dw_stable_secs = float(indicators.get("lob_dw_obi_stable_secs", 0.0) or 0.0)
    dw_dir_ok = (dw_obi_val > 0 and str(direction).upper() == "LONG") or (dw_obi_val < 0 and str(direction).upper() == "SHORT")
    # ------------------------------

    if not last_event:
        if dw_dir_ok and dw_obi_stable:
            indicators["obi_age_ms"] = 0
            indicators["obi"] = dw_obi_val
            indicators["obi_stable_secs"] = dw_stable_secs
            return True, True, dw_stable_secs, dw_obi_val
        return obi_dir_ok, obi_stable, stable_secs, obi_val

    ts_ms = _i(last_event.get("ts_ms"), 0)
    age_ms = (now_ts_ms - ts_ms) if ts_ms > 0 else 10**9
    indicators["obi_age_ms"] = int(age_ms)

    ttl_ms = _i(cfg.get("obi_event_ttl_ms", 30000), 30000)  # default aligned with strategy.py
    if not (0 <= age_ms <= ttl_ms):
        if dw_dir_ok and dw_obi_stable:
            indicators["obi"] = dw_obi_val
            indicators["obi_stable_secs"] = dw_stable_secs
            return True, True, dw_stable_secs, dw_obi_val
        return obi_dir_ok, obi_stable, stable_secs, obi_val

    obi_dir = (last_event.get("direction") or "").upper()
    if obi_dir and obi_dir == str(direction).upper():
        obi_dir_ok = True
        obi_val = _f(last_event.get("obi", 0.0), 0.0)
        indicators["obi"] = float(obi_val)

        stable_secs = _f(last_event.get("stable_secs", 0.0), 0.0)
        indicators["obi_stable_secs"] = float(stable_secs)

        # Optional analytics (may be missing)
        indicators["obi_z"] = _f(last_event.get("obi_z", 0.0), 0.0)
        indicators["obi_stacking"] = _f(last_event.get("stacking", 0.0), 0.0)
        indicators["obi_concentration"] = _f(last_event.get("concentration", 0.0), 0.0)

        obi_min = _f(cfg.get("obi_stable_min_secs", 1.0), 1.0)
        obi_stable = stable_secs >= obi_min

        if not obi_stable and dw_dir_ok and dw_obi_stable and dw_stable_secs >= obi_min:
            obi_stable = True
            stable_secs = dw_stable_secs
            obi_val = dw_obi_val
            indicators["obi"] = float(obi_val)
            indicators["obi_stable_secs"] = float(stable_secs)
    else:
        if dw_dir_ok and dw_obi_stable:
            obi_min = _f(cfg.get("obi_stable_min_secs", 1.0), 1.0)
            if dw_stable_secs >= obi_min:
                obi_dir_ok = True
                obi_stable = True
                stable_secs = dw_stable_secs
                obi_val = dw_obi_val
                indicators["obi"] = float(obi_val)
                indicators["obi_stable_secs"] = float(stable_secs)

    return obi_dir_ok, obi_stable, stable_secs, obi_val


def compute_iceberg_flags(
    *,
    direction: str,
    price: float,
    now_ts_ms: int,
    last_event: dict[str, Any] | None,
    cfg: dict[str, Any],
    indicators: dict[str, Any],
) -> tuple[bool, bool, int, float]:
    """
    Single source of truth for Iceberg evidence.

    Returns:
      (iceberg_dir_ok, iceberg_strict, refresh, duration)

    Rules:
      - require event freshness: age_ms in [0..iceberg_event_ttl_ms]
      - require side match to direction:
          LONG  => side == "bid"
          SHORT => side == "ask"
      - strict if refresh>=r_min AND duration>=d_min AND dist_bp<=dist_bp_max
    """
    iceberg_dir_ok = False
    iceberg_strict = False
    refresh = 0
    duration = 0.0

    if not last_event:
        return iceberg_dir_ok, iceberg_strict, refresh, duration

    ts_ms = _i(last_event.get("ts_ms"), 0)
    age_ms = (now_ts_ms - ts_ms) if ts_ms > 0 else 10**9
    indicators["iceberg_age_ms"] = int(age_ms)

    ttl_ms = _i(cfg.get("iceberg_event_ttl_ms", 15000), 15000)
    if not (0 <= age_ms <= ttl_ms):
        return iceberg_dir_ok, iceberg_strict, refresh, duration

    side = (last_event.get("side") or "")
    dir_u = str(direction).upper()
    if (side == "bid" and dir_u == "LONG") or (side == "ask" and dir_u == "SHORT"):
        iceberg_dir_ok = True
        refresh = _i(last_event.get("refresh", 0), 0)
        duration = _f(last_event.get("duration", 0.0), 0.0)
        ipx = _f(last_event.get("price", 0.0), 0.0)

        indicators["iceberg_refresh"] = int(refresh)
        indicators["iceberg_duration"] = float(duration)
        indicators["iceberg_price"] = float(ipx)

        r_min = _i(cfg.get("iceberg_strict_refresh_min", 3), 3)
        d_min = _f(cfg.get("iceberg_strict_duration_min", 1.5), 1.5)
        dist_bp_max = _f(cfg.get("iceberg_strict_dist_bp", 10.0), 10.0)

        dist_bp_now = 0.0
        if price > 0 and ipx > 0:
            mid = 0.5 * (abs(price) + abs(ipx))
            dist_bp_now = (10000.0 * abs(price - ipx) / mid) if mid > 0 else 0.0
        indicators["iceberg_dist_bp"] = float(dist_bp_now)

        iceberg_strict = bool(refresh >= r_min and duration >= d_min and dist_bp_now <= dist_bp_max)
        indicators["iceberg_strict"] = 1 if iceberg_strict else 0

    return iceberg_dir_ok, iceberg_strict, refresh, duration


def compute_ofi_flags(
    *,
    direction: str,
    now_ts_ms: int,
    last_event: Any,
    cfg: dict[str, Any],
    indicators: dict[str, Any],
) -> tuple[bool, bool, float, float, float, float]:
    """
    OFI evidence (first-class):
      - last_event expected dict-like:
          ts_ms, direction, ofi, ofi_z, stable_secs, stability_score, stable
      - returns:
          (ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi, ofi_z, ofi_stability_score)
    """
    ofi_dir_ok = False
    ofi_stable = False
    stable_secs = 0.0
    ofi = 0.0
    ofi_z = 0.0
    stab = 0.0

    if not last_event:
        indicators["ofi_age_ms"] = -1
        indicators["ofi_stable"] = 0
        indicators["ofi_dir_ok"] = 0
        return ofi_dir_ok, ofi_stable, stable_secs, ofi, ofi_z, stab

    # tolerate both dict and object-like
    try:
        ts = _i(last_event.get("ts_ms"), 0) if isinstance(last_event, dict) else _i(getattr(last_event, "ts_ms", 0), 0)
    except Exception:
        ts = 0
    age = (now_ts_ms - ts) if ts > 0 else 10**9
    indicators["ofi_age_ms"] = int(age)

    ttl_ms = _i(cfg.get("ofi_event_ttl_ms", 15_000), 15_000)
    if not (0 <= age <= ttl_ms):
        indicators["ofi_stable"] = 0
        indicators["ofi_dir_ok"] = 0
        return ofi_dir_ok, ofi_stable, stable_secs, ofi, ofi_z, stab

    try:
        ev_dir = (last_event.get("direction") if isinstance(last_event, dict) else getattr(last_event, "direction", "")) or ""
        ofi_dir_ok = str(ev_dir).upper() == str(direction).upper()
    except Exception:
        ofi_dir_ok = False

    try:
        ofi = _f(last_event.get("ofi", 0.0), 0.0) if isinstance(last_event, dict) else _f(getattr(last_event, "ofi", 0.0), 0.0)
        ofi_z = _f(last_event.get("ofi_z", 0.0), 0.0) if isinstance(last_event, dict) else _f(getattr(last_event, "ofi_z", 0.0), 0.0)
        stable_secs = _f(last_event.get("stable_secs", 0.0), 0.0) if isinstance(last_event, dict) else _f(getattr(last_event, "stable_secs", 0.0), 0.0)
        stab = _f(last_event.get("stability_score", 0.0), 0.0) if isinstance(last_event, dict) else _f(getattr(last_event, "stability_score", 0.0), 0.0)
    except Exception:
        pass

    indicators["ofi"] = float(ofi)
    indicators["ofi_z"] = float(ofi_z)
    indicators["ofi_stable_secs"] = float(stable_secs)
    indicators["ofi_stability_score"] = float(stab)

    min_secs = _f(cfg.get("ofi_stable_min_secs", 1.5), 1.5)
    stab_min = _f(cfg.get("ofi_stability_score_min", 0.6), 0.6)

    # last_event may provide its own 'stable' flag
    ev_stable = 0
    try:
        ev_stable = _i(last_event.get("stable", 0), 0) if isinstance(last_event, dict) else _i(getattr(last_event, "stable", 0), 0)
    except Exception:
        ev_stable = 0

    # Trust the detector's own stable flag (ev_stable) when direction is valid and stable_secs ok.
    # stab_min is only enforced when ev_stable is not provided or explicitly 0.
    if ev_stable == 1:
        ofi_stable = bool(ofi_dir_ok and stable_secs >= min_secs)
    else:
        ofi_stable = bool(ofi_dir_ok and stable_secs >= min_secs and stab >= stab_min)

    indicators["ofi_dir_ok"] = 1 if ofi_dir_ok else 0
    indicators["ofi_stable"] = 1 if ofi_stable else 0

    return ofi_dir_ok, ofi_stable, stable_secs, ofi, ofi_z, stab
