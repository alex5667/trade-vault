from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from services.atr_candidate_provider import get_atr_candidate_provider

try:
    from prometheus_client import Counter, Histogram
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Histogram = None  # type: ignore

# ---------------------------------------------------------------------------
# Prometheus metrics (Phase 2)
# ---------------------------------------------------------------------------

_M_SEL_TOTAL = Counter(
    "trade_atr_selector_total",
    "ATR selector invocations by reason_code and source",
    ["reason_code", "source"],
) if Counter is not None else None

_M_SEL_TARGET_TF = Counter(
    "trade_atr_selector_target_tf_ms_total",
    "ATR selector computed target TF distribution",
    ["tf_ms"],
) if Counter is not None else None

_M_SEL_PICKED_TF = Counter(
    "trade_atr_selector_picked_tf_ms_total",
    "ATR selector picked TF distribution",
    ["tf_ms"],
) if Counter is not None else None

_M_SEL_FALLBACK = Counter(
    "trade_atr_selector_fallback_total",
    "ATR selector fallback reason",
    ["reason"],
) if Counter is not None else None

_M_SEL_CANDIDATES = Histogram(
    "trade_atr_selector_candidate_count_hist",
    "Number of multi-TF ATR candidates found in payload",
    buckets=[0, 1, 2, 3, 4, 5, 6, 7],
) if Histogram is not None else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _ensure_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _parse_allowed_tfs() -> List[int]:
    raw = str(
        os.getenv("ATR_HORIZON_ALLOWED_TFS_MS", "15000,30000,60000,180000,300000,900000") or ""
    ).strip()
    min_tf_ms = _safe_int(os.getenv("ATR_HORIZON_MIN_TF_MS", "300000"), 300000)
    out: List[int] = []
    for p in raw.split(","):
        try:
            x = int(p.strip())
            if x > 0:
                out.append(max(x, min_tf_ms))
        except Exception:
            pass
    out = sorted(set(out))
    return out or [min_tf_ms]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RuntimeATRSelectorResult:
    mode: str
    atr_value: float
    atr_tf_ms: int
    atr_window_n: int
    atr_age_ms: int
    atr_source: str
    atr_regime_value: float
    atr_trail_value: float
    atr_regime_tf_ms: int
    atr_trail_tf_ms: int
    atr_pct: float
    vol_ratio_fast_slow: float
    vol_ratio_z: float
    selector_reason_code: str
    selector_reason_details: Dict[str, Any]


# ---------------------------------------------------------------------------
# TF resolution helpers
# ---------------------------------------------------------------------------

def _nearest_allowed_tf(ideal_tf_ms: int, allowed: List[int]) -> int:
    if not allowed:
        return max(1, ideal_tf_ms)
    return min(allowed, key=lambda x: (abs(x - ideal_tf_ms), x))


def _compute_target_tf_ms(
    hold_target_ms: int,
    alpha_half_life_ms: int,
    window_n: int,
    allowed: List[int],
) -> int:
    hold_target_ms = max(0, int(hold_target_ms))
    alpha_half_life_ms = max(0, int(alpha_half_life_ms))
    target_window_ms = max(alpha_half_life_ms, int(hold_target_ms * 1.5))
    if target_window_ms <= 0:
        # bootstrap fallback: no horizon data yet
        return _nearest_allowed_tf(max(300000, allowed[0] if allowed else 300000), allowed)
    ideal_tf_ms = max(1000, int(target_window_ms / max(1, window_n)))
    return _nearest_allowed_tf(ideal_tf_ms, allowed)


# ---------------------------------------------------------------------------
# Candidate key helpers
# ---------------------------------------------------------------------------

def _tf_alias_map() -> Dict[int, str]:
    return {
        15000: "15s",
        30000: "30s",
        60000: "1m",
        180000: "3m",
        300000: "5m",
        900000: "15m",
    },


def _candidate_keys_with_alias(tf_ms: int) -> List[str]:
    alias = _tf_alias_map().get(tf_ms, "")
    out = [
        f"atr_{tf_ms}",
        f"atr_tf_{tf_ms}",
        f"atr_ms_{tf_ms}",
    ]
    if alias:
        out.extend([
            f"atr_{alias}",
            f"atr_tf_{alias}",
        ])
    return out


def _candidate_ts_keys_with_alias(tf_ms: int) -> List[str]:
    alias = _tf_alias_map().get(tf_ms, "")
    out = [
        f"atr_ts_ms_{tf_ms}",
        f"atr_tf_ts_ms_{tf_ms}",
        f"atr_ts_{tf_ms}",
    ]
    if alias:
        out.extend([
            f"atr_ts_ms_{alias}",
            f"atr_tf_ts_ms_{alias}",
        ])
    return out


def _read_first_float(*dicts: Dict[str, Any], keys: List[str]) -> float:
    for d in dicts:
        if not isinstance(d, dict):
            continue
        for k in keys:
            if k in d:
                x = _safe_float(d[k], 0.0)
                if x > 0.0:
                    return x
    return 0.0


def _read_first_int(*dicts: Dict[str, Any], keys: List[str], default: int = 0) -> int:
    for d in dicts:
        if not isinstance(d, dict):
            continue
        for k in keys:
            if k in d:
                x = _safe_int(d[k], default)
                if x > 0:
                    return x
    return default


# ---------------------------------------------------------------------------
# Candidate scanning — delegated to ATRCandidateProvider (Phase 2.1)
# ---------------------------------------------------------------------------

def _build_candidates(
    signal: Dict[str, Any],
    indicators: Dict[str, Any],
    meta: Dict[str, Any],
    now_ms: int,
) -> Dict[int, Tuple[float, int, str]]:
    """
    Delegate multi-TF ATR candidate collection to ATRCandidateProvider.
    Returns {tf_ms: (atr_value, age_ms, source)}.
    Fail-open: on any error returns empty dict (selector falls back to legacy).
    """
    symbol = str(
        signal.get("symbol") or meta.get("symbol") or ""
    ).upper()
    try:
        provider = get_atr_candidate_provider()
        raw = provider.collect(signal=signal, symbol=symbol, now_ms=now_ms)
        out: Dict[int, Tuple[float, int, str]] = {}
        for tf_ms_key, obj in raw.items():
            v = _safe_float(obj.get("value"), 0.0)
            age = _safe_int(obj.get("age_ms"), 0)
            src = str(obj.get("source") or "unknown")
            if v > 0.0:
                out[int(tf_ms_key)] = (v, age, src)
        return out
    except Exception:
        return {}


def _pick_nearest_available(
    target_tf_ms: int,
    candidates: Dict[int, Tuple[float, int, str]],
) -> Optional[Tuple[int, float, int, str]]:
    if not candidates:
        return None
    if target_tf_ms in candidates:
        v, age, src = candidates[target_tf_ms]
        return (target_tf_ms, v, age, src)
    tf = min(candidates.keys(), key=lambda x: (abs(x - target_tf_ms), x))
    v, age, src = candidates[tf]
    return (tf, v, age, src)


def _compute_vol_ratio(candidates: Dict[int, Tuple[float, int, str]]) -> Tuple[float, float]:
    """
    Fast/slow vol ratio: atr at fastest TF / atr at slowest TF.
    Returns (ratio, z_score_placeholder=0.0).
    """
    if len(candidates) < 2:
        return (1.0, 0.0)
    sorted_tfs = sorted(candidates.keys())
    fast = candidates[sorted_tfs[0]][0]   # tuple[0] = atr_value (unchanged)
    slow = candidates[sorted_tfs[-1]][0]
    if fast > 0.0 and slow > 0.0:
        return (float(fast / slow), 0.0)
    return (1.0, 0.0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def select_runtime_atr_profile(
    *,
    signal: Dict[str, Any],
    price: float,
    hold_target_ms: int,
    alpha_half_life_ms: int,
    now_ms: int,
) -> Dict[str, Any]:
    """
    Phase 2 runtime ATR TF selector.

    Picks the best available ATR candidate for the given hold/decay horizon.
    Always fail-open: on any error or missing data, returns a legacy profile
    using signal["atr"] so downstream gates are never broken.

    Shadow mode: call site controls whether the result feeds signal["atr"]
    via ATR_HORIZON_USE_FOR_GATES=0 (default).
    """
    signal = _ensure_dict(signal)
    indicators = _ensure_dict(signal.get("indicators"))
    meta = _ensure_dict(signal.get("meta"))

    window_n = _safe_int(os.getenv("ATR_HORIZON_WINDOW_N", "14"), 14)
    default_tf_ms = _safe_int(os.getenv("ATR_HORIZON_DEFAULT_TF_MS", "300000"), 300000)
    max_candidate_age_ms = _safe_int(
        os.getenv("ATR_HORIZON_CANDIDATE_MAX_AGE_MS", "300000"), 300000
    )
    allowed = _parse_allowed_tfs()

    target_tf_ms = _compute_target_tf_ms(
        hold_target_ms=hold_target_ms,
        alpha_half_life_ms=alpha_half_life_ms,
        window_n=window_n,
        allowed=allowed,
    )

    candidates = _build_candidates(signal, indicators, meta, now_ms=now_ms)

    if _M_SEL_CANDIDATES is not None:
        try:
            _M_SEL_CANDIDATES.observe(len(candidates))
        except Exception:
            pass

    if _M_SEL_TARGET_TF is not None:
        try:
            _M_SEL_TARGET_TF.labels(tf_ms=str(target_tf_ms)).inc()
        except Exception:
            pass

    picked = _pick_nearest_available(target_tf_ms, candidates)

    if picked is not None:
        picked_tf_ms, picked_atr, picked_age_ms, picked_src = picked
        if picked_atr > 0.0 and picked_age_ms <= max_candidate_age_ms:
            is_exact = picked_tf_ms == target_tf_ms
            reason_code = "ATR_SEL_EXACT" if is_exact else "ATR_SEL_NEAREST"
            # atr_source: prefer the raw candidate source for full traceability
            atr_source = str(picked_src or ("selector_exact" if is_exact else "selector_nearest"))
            vol_ratio, vol_ratio_z = _compute_vol_ratio(candidates)

            if _M_SEL_PICKED_TF is not None:
                try:
                    _M_SEL_PICKED_TF.labels(tf_ms=str(picked_tf_ms)).inc()
                except Exception:
                    pass
            if _M_SEL_TOTAL is not None:
                try:
                    _M_SEL_TOTAL.labels(reason_code=reason_code, source=atr_source).inc()
                except Exception:
                    pass

            return asdict(RuntimeATRSelectorResult(
                mode="horizon",
                atr_value=float(picked_atr),
                atr_tf_ms=int(picked_tf_ms),
                atr_window_n=int(window_n),
                atr_age_ms=int(picked_age_ms),
                atr_source=atr_source,
                atr_regime_value=float(picked_atr),
                atr_trail_value=float(picked_atr),
                atr_regime_tf_ms=int(picked_tf_ms),
                atr_trail_tf_ms=int(picked_tf_ms),
                atr_pct=(float(picked_atr / price) if price > 0.0 else 0.0),
                vol_ratio_fast_slow=float(vol_ratio),
                vol_ratio_z=float(vol_ratio_z),
                selector_reason_code=reason_code,
                selector_reason_details={
                    "target_tf_ms": int(target_tf_ms),
                    "picked_tf_ms": int(picked_tf_ms),
                    "hold_target_ms": int(hold_target_ms),
                    "alpha_half_life_ms": int(alpha_half_life_ms),
                    "candidate_n": int(len(candidates)),
                    "picked_source": str(picked_src or "unknown"),
                },
            ))
        else:
            # candidate found but stale
            if _M_SEL_FALLBACK is not None:
                try:
                    _M_SEL_FALLBACK.labels(reason="stale").inc()
                except Exception:
                    pass

    else:
        # no candidates at all
        if _M_SEL_FALLBACK is not None:
            try:
                _M_SEL_FALLBACK.labels(reason="missing").inc()
            except Exception:
                pass

    # -----------------------------------------------------------------------
    # Fail-open: legacy fallback
    # -----------------------------------------------------------------------
    legacy_atr = _safe_float(
        signal.get("atr")
        or indicators.get("atr")
        or meta.get("atr")
        or 0.0,
        0.0,
    )
    legacy_ts_ms = _read_first_int(
        signal, indicators, meta,
        keys=["atr_ts_ms"],
        default=now_ms,
    )
    legacy_age_ms = max(0, now_ms - legacy_ts_ms)

    if _M_SEL_TOTAL is not None:
        try:
            _M_SEL_TOTAL.labels(
                reason_code="ATR_SEL_LEGACY_FALLBACK",
                source="legacy_fallback",
            ).inc()
        except Exception:
            pass
    if _M_SEL_FALLBACK is not None:
        try:
            _M_SEL_FALLBACK.labels(reason="legacy").inc()
        except Exception:
            pass

    return asdict(RuntimeATRSelectorResult(
        mode="legacy",
        atr_value=float(legacy_atr),
        atr_tf_ms=int(default_tf_ms),
        atr_window_n=int(window_n),
        atr_age_ms=int(legacy_age_ms),
        atr_source="legacy_fallback",
        atr_regime_value=float(legacy_atr),
        atr_trail_value=float(legacy_atr),
        atr_regime_tf_ms=int(default_tf_ms),
        atr_trail_tf_ms=int(default_tf_ms),
        atr_pct=(float(legacy_atr / price) if price > 0.0 else 0.0),
        vol_ratio_fast_slow=1.0,
        vol_ratio_z=0.0,
        selector_reason_code="ATR_SEL_LEGACY_FALLBACK",
        selector_reason_details={
            "target_tf_ms": int(target_tf_ms),
            "candidate_n": int(len(candidates)),
            "hold_target_ms": int(hold_target_ms),
            "alpha_half_life_ms": int(alpha_half_life_ms),
        },
    ))
