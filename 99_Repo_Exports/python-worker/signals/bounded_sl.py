"""bounded_sl.py — SL recalibration vs microstructure noise (plan 2.4).

bounded_sl_dist = max(k_atr * ATR_1m, k_mae * percentile_75(MAE_30d))

The k_atr * ATR_1m component is already produced by compute_levels(). This
module supplies the **MAE-percentile floor** read from the 30d rolling priors
written by `orderflow_services/pit_priors_rolling_v1`.

Redis source:
  HASH  pit_priors:rolling:30d:{SYMBOL}:default:all
  Field p75_mae_bps_30d  (also p50_/p90_ available)

Inputs:
  symbol       — e.g. "BTCUSDT"
  entry        — entry price (positive float)
  cfg          — handler cfg dict; can carry an injected override
                   "mae_p75_bps_30d" for testability
Returns:
  (floor_dist, meta_dict) — floor_dist is in **price units**, ready to be
  compared to stop_dist; meta_dict carries observability fields.

ENV
  BOUNDED_SL_ENABLED        ("0"/"1", default "0")  — feature kill switch.
  BOUNDED_SL_SHADOW         ("0"/"1", default "1")  — when ENABLED=1 but
                              SHADOW=1, only meta is filled, floor is NOT
                              applied by compute_levels.
  BOUNDED_SL_MAE_K          (float, default "1.0")  — multiplier on the
                              percentile (k_mae in the formula above).
  BOUNDED_SL_MAE_P75_CAP_BPS (float, default "200.0") — safety cap on the
                              MAE-derived floor (bps).
  BOUNDED_SL_CACHE_TTL_S    (float, default "60.0")  — in-process TTL cache.
  BOUNDED_SL_MIN_SAMPLES    (int,   default "30")    — require N closed
                              samples in the 30d window before trusting
                              the percentile.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict[str, float]]] = {}
_CACHE_LOCK = threading.Lock()


def _f(v: Any, d: float = 0.0) -> float:
    try:
        if v is None:
            return d
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        r = float(v)
        if r != r:  # NaN
            return d
        return r
    except Exception:
        return d


def _env_float(name: str, default: float) -> float:
    return _f(os.getenv(name), default)


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v is not None and v != "" else default
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw == "":
        return default
    return raw in ("1", "true", "yes", "on")


def _read_priors_from_redis(symbol: str) -> dict[str, float]:
    """Fetch p50/p75/p90/sample_count from pit_priors:rolling:30d HASH.

    Failures (no redis, missing key, parse errors) → empty dict (fail-open).
    """
    sym = (symbol or "").upper().strip()
    if not sym:
        return {}
    try:
        from core.redis_client import get_redis
        r = get_redis()
    except Exception as e:
        logger.debug("bounded_sl: get_redis failed: %s", e)
        return {}

    key = f"pit_priors:rolling:30d:{sym}:default:all"
    try:
        raw_any: Any = r.hgetall(key)  # type: ignore[union-attr]
    except Exception as e:
        logger.debug("bounded_sl: HGETALL %s failed: %s", key, e)
        return {}
    if not isinstance(raw_any, dict) or not raw_any:
        return {}

    # Normalize bytes→str across the whole hash once.
    normalized: dict[str, str] = {}
    for k, v in raw_any.items():
        ks = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        vs = v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else str(v)
        normalized[ks] = vs

    return {
        "p50_mae_bps_30d": _f(normalized.get("p50_mae_bps_30d"), 0.0),
        "p75_mae_bps_30d": _f(normalized.get("p75_mae_bps_30d"), 0.0),
        "p90_mae_bps_30d": _f(normalized.get("p90_mae_bps_30d"), 0.0),
        "sample_count": _f(normalized.get("sample_count"), 0.0),
    }


def _get_priors(symbol: str, ttl_s: float) -> dict[str, float]:
    sym = (symbol or "").upper().strip()
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(sym)
        if cached and (now - cached[0]) < ttl_s:
            return cached[1]
    data = _read_priors_from_redis(sym)
    with _CACHE_LOCK:
        _CACHE[sym] = (now, data)
    return data


def resolve_mae_floor_bps(symbol: str, cfg: dict | None = None) -> tuple[float, dict[str, float]]:
    """Return (floor_bps, meta) for the MAE-percentile floor.

    Priority for the percentile value:
      1. cfg["mae_p75_bps_30d"]  (explicit injection — used by tests / hot path
         that already loaded priors elsewhere)
      2. Redis pit_priors:rolling:30d:{sym}:default:all → p75_mae_bps_30d
      3. 0.0  (no floor available; bounded SL is a no-op)
    """
    cfg = cfg or {}
    k_mae = _env_float("BOUNDED_SL_MAE_K", 1.0)
    cap_bps = _env_float("BOUNDED_SL_MAE_P75_CAP_BPS", 200.0)
    min_samples = _env_int("BOUNDED_SL_MIN_SAMPLES", 30)
    ttl_s = _env_float("BOUNDED_SL_CACHE_TTL_S", 60.0)

    meta: dict[str, float] = {
        "mae_p75_bps": 0.0,
        "mae_p75_bps_raw": 0.0,
        "sample_count": 0.0,
        "floor_bps": 0.0,
        "source": 0.0,  # 0=none, 1=cfg, 2=redis
    }

    injected = cfg.get("mae_p75_bps_30d")
    if injected is not None:
        p75_raw = _f(injected, 0.0)
        if p75_raw > 0.0:
            meta["mae_p75_bps_raw"] = p75_raw
            meta["source"] = 1.0
            meta["sample_count"] = _f(cfg.get("mae_sample_count_30d"), 0.0)
        else:
            return 0.0, meta
    else:
        priors = _get_priors(symbol, ttl_s)
        sample_count = priors.get("sample_count", 0.0)
        p75_raw = priors.get("p75_mae_bps_30d", 0.0)
        meta["mae_p75_bps_raw"] = p75_raw
        meta["sample_count"] = sample_count
        if sample_count < min_samples:
            # not enough samples → no floor
            return 0.0, meta
        if p75_raw <= 0.0:
            return 0.0, meta
        meta["source"] = 2.0

    # Apply k multiplier + safety cap
    floor_bps = p75_raw * max(0.0, k_mae)
    if cap_bps > 0.0 and floor_bps > cap_bps:
        floor_bps = cap_bps

    meta["mae_p75_bps"] = p75_raw  # what we'd use w/o cap, for telemetry
    meta["floor_bps"] = floor_bps
    return floor_bps, meta


def apply_bounded_sl_floor(
    symbol: str,
    entry: float,
    stop_dist: float,
    cfg: dict | None = None,
) -> tuple[float, dict[str, Any]]:
    """Return (new_stop_dist, telemetry).

    Behavior is gated by ENV `BOUNDED_SL_ENABLED`:
      - disabled → returns (stop_dist, telemetry with enabled=0).
      - enabled + shadow=1 → returns (stop_dist, telemetry with would_apply=...).
      - enabled + shadow=0 → returns (max(stop_dist, mae_floor_dist), telemetry).

    Always fail-open: any error inside returns original stop_dist.
    """
    cfg = cfg or {}
    telem: dict[str, Any] = {
        "enabled": 0,
        "shadow": 0,
        "applied": 0,
        "base_dist": stop_dist,
        "final_dist": stop_dist,
        "delta_dist": 0.0,
    }
    try:
        if not _env_bool("BOUNDED_SL_ENABLED", False):
            return stop_dist, telem
        telem["enabled"] = 1
        shadow = _env_bool("BOUNDED_SL_SHADOW", True)
        telem["shadow"] = 1 if shadow else 0

        if entry <= 0.0 or stop_dist <= 0.0:
            return stop_dist, telem

        floor_bps, meta = resolve_mae_floor_bps(symbol, cfg)
        telem.update({
            "mae_p75_bps": meta.get("mae_p75_bps_raw", 0.0),
            "mae_sample_count": meta.get("sample_count", 0.0),
            "mae_floor_bps": floor_bps,
            "mae_floor_source": meta.get("source", 0.0),
        })

        if floor_bps <= 0.0:
            return stop_dist, telem

        mae_floor_dist = entry * floor_bps / 10_000.0
        base_dist_bps = (stop_dist / entry) * 10_000.0 if entry > 0 else 0.0
        telem["base_dist_bps"] = base_dist_bps

        if mae_floor_dist > stop_dist:
            telem["delta_dist"] = mae_floor_dist - stop_dist
            telem["would_apply"] = 1
            if not shadow:
                telem["applied"] = 1
                telem["final_dist"] = mae_floor_dist
                return mae_floor_dist, telem
        else:
            telem["would_apply"] = 0

        return stop_dist, telem
    except Exception as e:
        logger.debug("bounded_sl: apply failed: %s", e)
        return stop_dist, telem


def _reset_cache_for_tests() -> None:
    """Test helper — wipe the in-process priors cache."""
    with _CACHE_LOCK:
        _CACHE.clear()
