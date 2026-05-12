from __future__ import annotations

import math
import os
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _hgetall_str(redis_client: Any, key: str) -> dict[str, str]:
    try:
        raw = redis_client.hgetall(key) or {}
    except Exception:
        return {}
    out: dict[str, str] = {}
    try:
        for k, v in dict(raw).items():
            out[_b2s(k)] = _b2s(v)
    except Exception:
        return {}
    return out


def drift_active_key_v1(symbol: str, venue: str, session: str, tf: str) -> str:
    return f"drift:active:v1:{symbol}:{venue}:{session}:{tf}"


def drift_active_key_v2(symbol: str, venue: str, session: str, tf: str, kind: str) -> str:
    return f"drift:active:v2:{symbol}:{venue}:{session}:{tf}:{kind}"


def drift_state_key_v1(symbol: str, venue: str, session: str, tf: str) -> str:
    return f"drift:state:v1:{symbol}:{venue}:{session}:{tf}"


def drift_state_key_v2(symbol: str, venue: str, session: str, tf: str, kind: str) -> str:
    return f"drift:state:v2:{symbol}:{venue}:{session}:{tf}:{kind}"


def load_drift_active_factor(
    redis_client: Any,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
) -> tuple[float, float, str]:
    """
    Read drift alarm state (fail-open).

    Returns:
      (factor, score, feature)

    Backward compatible:
      - if FEATURE_DRIFT_INCLUDE_KIND=1 -> try v2 first
      - then fallback to v1
    """
    if redis_client is None:
        return 1.0, float("nan"), ""

    include_kind = _env_bool("FEATURE_DRIFT_INCLUDE_KIND", False)
    sym = (symbol or "NA").upper()
    ven = (venue or "na").lower()
    sess = (session or "na").lower()
    tfv = (tf or "na").lower()
    knd = (kind or "na").lower()

    def _read(key: str) -> tuple[float, float, str] | None:
        dd = _hgetall_str(redis_client, key)
        if not dd:
            return None
        try:
            f = dd.get("factor") or 1.0
            s = dd.get("score") or float("nan")
            feat = (dd.get("feature") or "")
            if not math.isfinite(f) or f <= 0:
                return None
            return float(f), float(s), str(feat)
        except Exception:
            return None

    if include_kind:
        r2 = _read(drift_active_key_v2(sym, ven, sess, tfv, knd))
        if r2 is not None:
            return r2

    r1 = _read(drift_active_key_v1(sym, ven, sess, tfv))
    if r1 is not None:
        return r1

    return 1.0, float("nan"), ""


def load_drift_baseline_mu(
    redis_client: Any,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    feature: str,
) -> float | None:
    """
    Read baseline mean for feature from drift:state:* (fail-open).

    Field name:
      b_mu:<feature>
    """
    if redis_client is None:
        return None

    include_kind = _env_bool("FEATURE_DRIFT_INCLUDE_KIND", False)
    sym = (symbol or "NA").upper()
    ven = (venue or "na").lower()
    sess = (session or "na").lower()
    tfv = (tf or "na").lower()
    knd = (kind or "na").lower()

    keys = []
    if include_kind:
        keys.append(drift_state_key_v2(sym, ven, sess, tfv, knd))
    keys.append(drift_state_key_v1(sym, ven, sess, tfv))

    fld = f"b_mu:{feature}"
    for key in keys:
        dd = _hgetall_str(redis_client, key)
        if not dd:
            continue
        try:
            mu = dd.get(fld) or 0.0
            if math.isfinite(mu) and mu > 0:
                return float(mu)
        except Exception:
            continue
    return None
