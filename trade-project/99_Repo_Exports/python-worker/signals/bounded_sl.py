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
  BOUNDED_SL_MIN_SAMPLES    (int,   default "15")    — require N closed
                              samples in the 30d window before trusting
                              the per-symbol percentile. Lowered 30→15 on
                              2026-05-29 — sparse symbols had floor=0 too
                              often (BOUNDED_SL no-op on the most fragile
                              setups).
  BOUNDED_SL_GROUP_FALLBACK_ENABLED ("0"/"1", default "1") — when the
                              per-symbol sample count is below
                              BOUNDED_SL_MIN_SAMPLES, fall back to a global
                              median floor read from
                              ``pit_priors:rolling:30d:_global:default:all``
                              (or the legacy `_global` key) before giving up.
                              The fallback uses p50 (more conservative than
                              p75) and is gated by
                              BOUNDED_SL_GROUP_MIN_SAMPLES.
  BOUNDED_SL_GROUP_MIN_SAMPLES (int, default "50") — minimum sample count
                              required on the global/group key before its
                              p50 is trusted as a fallback floor.
  BOUNDED_SL_MAX_ATR_MULT   (float, default "4.0")   — 2026-05-27 WR fix:
                              if mae_floor_bps / atr_bps > this multiple,
                              the floor is treated as too-wide for current
                              volatility regime and skipped. Reports of
                              avg SL=9.14 ATR in low-vol windows showed the
                              MAE floor dominating ATR-scaled SL → mirror
                              TP also became 9 ATR → near-zero WR.
  BOUNDED_SL_MAX_ATR_SHADOW ("0"/"1", default "1")   — if 1, the ATR-multiple
                              cap is recorded in telemetry but NOT enforced
                              (shadow). Flip to 0 to enforce.
  BOUNDED_SL_ATR_CAP_MODE   ("skip"/"clamp", default "skip") — 2026-05-29:
                              behaviour when the ATR-multiple cap triggers
                              (ratio = floor_bps/atr_bps > MAX_ATR_MULT).
                              * "skip"  — legacy: drop the floor entirely,
                                          keep ATR-scaled stop_dist. Risk:
                                          on tiny ATR (e.g. 4-5 bps on ETH
                                          in calm sessions) SL collapses to
                                          1.2 ATR ≈ 5 bps and gets noise-hit.
                              * "clamp" — new: keep the floor but clamp it
                                          to max_mult × atr_bps. Yields a
                                          balanced SL = MAX_ATR_MULT × ATR
                                          when the raw floor would be too
                                          wide, instead of falling back to
                                          the unsafe ATR-scaled minimum.
                              Only takes effect when MAX_ATR_SHADOW=0.
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


def _read_group_priors_from_redis() -> dict[str, float]:
    """Best-effort read of a global/group fallback prior.

    Tries `pit_priors:rolling:30d:_global:default:all` first; if absent or
    unparseable, returns {}. Failures are silent (fail-open).
    """
    try:
        from core.redis_client import get_redis
        r = get_redis()
    except Exception:
        return {}

    key = "pit_priors:rolling:30d:_global:default:all"
    try:
        raw_any: Any = r.hgetall(key)  # type: ignore[union-attr]
    except Exception:
        return {}
    if not isinstance(raw_any, dict) or not raw_any:
        return {}

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


def _get_group_priors(ttl_s: float) -> dict[str, float]:
    now = time.time()
    cache_key = "__GROUP__"
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and (now - cached[0]) < ttl_s:
            return cached[1]
    data = _read_group_priors_from_redis()
    with _CACHE_LOCK:
        _CACHE[cache_key] = (now, data)
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
    # 2026-05-29: default lowered 30→15 so thin/new symbols still get a floor.
    min_samples = _env_int("BOUNDED_SL_MIN_SAMPLES", 15)
    ttl_s = _env_float("BOUNDED_SL_CACHE_TTL_S", 60.0)
    group_fallback_enabled = _env_bool("BOUNDED_SL_GROUP_FALLBACK_ENABLED", True)
    group_min_samples = _env_int("BOUNDED_SL_GROUP_MIN_SAMPLES", 50)

    meta: dict[str, float] = {
        "mae_p75_bps": 0.0,
        "mae_p75_bps_raw": 0.0,
        "sample_count": 0.0,
        "floor_bps": 0.0,
        # 0=none, 1=cfg, 2=redis_symbol, 3=group_fallback_p50
        "source": 0.0,
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

        # Per-symbol path (preferred when enough samples).
        if sample_count >= min_samples and p75_raw > 0.0:
            meta["source"] = 2.0
        else:
            # Group fallback: use global p50 (median) as a conservative floor.
            # Median is preferred over p75 for the global key because the global
            # distribution is much wider (mixes calm + meme tape) — using p75
            # there would oversize SL on majors during quiet sessions.
            if not group_fallback_enabled:
                return 0.0, meta
            group = _get_group_priors(ttl_s)
            g_count = group.get("sample_count", 0.0)
            g_p50 = group.get("p50_mae_bps_30d", 0.0)
            meta["group_sample_count"] = g_count
            meta["group_p50_bps_raw"] = g_p50
            if g_count < group_min_samples or g_p50 <= 0.0:
                return 0.0, meta
            p75_raw = g_p50  # treat group median as the working percentile
            meta["mae_p75_bps_raw"] = g_p50
            meta["source"] = 3.0

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
    atr: float | None = None,
) -> tuple[float, dict[str, Any]]:
    """Return (new_stop_dist, telemetry).

    Behavior is gated by ENV `BOUNDED_SL_ENABLED`:
      - disabled → returns (stop_dist, telemetry with enabled=0).
      - enabled + shadow=1 → returns (stop_dist, telemetry with would_apply=...).
      - enabled + shadow=0 → returns (max(stop_dist, mae_floor_dist), telemetry).

    2026-05-27 WR fix — ATR-multiple cap:
      Param `atr` (1m ATR in price units). When provided and >0, the cap
      `BOUNDED_SL_MAX_ATR_MULT` (default 4.0) limits how much the MAE-floor
      can exceed ATR-scaled SL. If `mae_floor_bps / atr_bps > MAX_ATR_MULT`,
      the floor is recorded as `cap_triggered=1` and (when not shadow)
      skipped — preserving the original ATR-scaled stop_dist.

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
        "atr_cap_triggered": 0,
        "atr_cap_shadow": 0,
        "atr_cap_skipped": 0,
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

        # 2026-05-27 WR fix: ATR-multiple cap. In low-vol regimes ATR shrinks
        # but p75(MAE_30d) is computed over the whole month — the floor then
        # dominates ATR-scaled SL → effective 9 ATR SL/TP → ~0% WR.
        if atr is not None and float(atr) > 0.0 and entry > 0.0:
            atr_bps = (float(atr) / entry) * 10_000.0
            telem["atr_bps"] = atr_bps
            if atr_bps > 0.0:
                ratio = floor_bps / atr_bps
                telem["mae_floor_to_atr_mult"] = ratio
                max_mult = _env_float("BOUNDED_SL_MAX_ATR_MULT", 4.0)
                atr_shadow = _env_bool("BOUNDED_SL_MAX_ATR_SHADOW", True)
                telem["atr_cap_shadow"] = 1 if atr_shadow else 0
                telem["atr_cap_max_mult"] = max_mult
                if max_mult > 0.0 and ratio > max_mult:
                    telem["atr_cap_triggered"] = 1
                    cap_mode = (os.getenv("BOUNDED_SL_ATR_CAP_MODE", "skip") or "skip").strip().lower()
                    telem["atr_cap_mode"] = cap_mode
                    if cap_mode == "clamp":
                        # Clamp floor at max_mult × atr_bps instead of skipping.
                        # Recompute mae_floor_dist with the clamped floor so the
                        # downstream max(stop_dist, mae_floor_dist) check uses it.
                        clamped_floor_bps = max_mult * atr_bps
                        telem["atr_cap_clamped"] = 1
                        telem["atr_cap_clamped_floor_bps"] = clamped_floor_bps
                        # Shadow=1 still records the clamp but does not change behaviour.
                        if not atr_shadow:
                            floor_bps = clamped_floor_bps
                            mae_floor_dist = entry * floor_bps / 10_000.0
                            telem["mae_floor_bps"] = floor_bps
                    elif not atr_shadow:
                        # Legacy: skip floor — preserve ATR-scaled stop_dist.
                        telem["atr_cap_skipped"] = 1
                        telem["would_apply"] = 0
                        return stop_dist, telem

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
