from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import redis

from core.bucket2_v1 import derive_bucket2_label
from core.champion_cfg_validator import validate_champion_cfg
from core.edge_stack_mh_v1 import EdgeStackMHModelV1  # type: ignore
from core.feature_engineering import (
    RobustScalerPack,
    apply_transform,
    bucketize,
    derive_regime_label,
    derive_session_label,
)
from core.meta_model_lr import MetaModelLR
from services.ml_calibration import PlattLogitCalibrator
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

# Prometheus metrics (optional, fail-open if not available)
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    # Mock metrics for when prometheus_client is not available
    class _MockMetric:  # type: ignore
        def labels(self, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
    Counter = Gauge = Histogram = lambda *args, **kwargs: _MockMetric()

# Import centralized metrics from registry (fail-open if not available)
try:
    from services.observability.metrics_registry import (
        ml_confirm_cfg_present,
        ml_confirm_cfg_valid,
        ml_confirm_enforce_share,
        ml_confirm_errors_total,
        ml_confirm_events_total,
        ml_confirm_latency_seconds,
        ml_confirm_model_load_seconds,
        ml_confirm_model_loaded,
        ml_missing_critical_total,
    )
    METRICS_REGISTRY_AVAILABLE = True
except Exception:
    METRICS_REGISTRY_AVAILABLE = False
    # Mock metrics for when registry is not available
    class _MockMetric:
        def labels(self, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
    ml_confirm_events_total = ml_confirm_errors_total = ml_confirm_cfg_present = \
    ml_confirm_cfg_valid = ml_confirm_enforce_share = ml_confirm_model_loaded = \
    ml_confirm_model_load_seconds = ml_confirm_latency_seconds = ml_missing_critical_total = \
    lambda *args, **kwargs: _MockMetric()

try:
    import joblib  # type: ignore
except Exception:  # pragma: no cover
    joblib = None  # type: ignore



def _now_ms() -> int:
    return get_ny_time_millis()


def _make_sid(symbol: str, ts_ms: int) -> str:
    # Canonical SID for joins across streams/tools.
    # NOTE: direction is intentionally NOT part of SID (1 signal per symbol+ts_ms).
    sym = (symbol or "").upper()
    try:
        t = ts_ms
    except Exception:
        t = 0
    return f"crypto-of:{sym}:{t}"


# Process-level shared caches to prevent redundant I/O and thundering herd.
# Keys: model_path or config_key. Values: loaded objects or dicts.
_SHARED_MODELS: dict[str, Any] = {}
_SHARED_CONFIGS: dict[str, Any] = {}
_SHARED_CONFIG_PAYLOADS: dict[str, bytes] = {}  # key -> last raw payload
_SHARED_MODEL_STATS: dict[str, tuple[float, int]] = {} # path -> (mtime, size)



def _normalize_sid(raw_sid: Any, *, symbol: str, ts_ms: int) -> str:
    """Normalize/derive canonical sid for joins.

    Accepts:
      - canonical: crypto-of:SYMBOL:ts_ms
      - legacy: crypto-of:SYMBOL:ts_ms:... (extra suffix)
      - loose: {symbol}|{ts_ms}|{direction} (direction ignored)
    Falls back to _make_sid(symbol, ts_ms).
    """
    s = (raw_sid or '').strip()
    if s.startswith('crypto-of:'):
        # keep only first 3 tokens: crypto-of:SYMBOL:ts
        parts = s.split(':')
        if len(parts) >= 3:
            sym = (parts[1] or '').upper()
            try:
                t = int(parts[2])
            except Exception:
                t = ts_ms if str(ts_ms).isdigit() else 0
            return f'crypto-of:{sym}:{t}'
    if '|' in s:
        parts = s.split('|')
        if len(parts) >= 2:
            sym = (parts[0] or symbol or '').upper()
            try:
                t = int(parts[1])
            except Exception:
                t = ts_ms if str(ts_ms).isdigit() else 0
            return f'crypto-of:{sym}:{t}'
    return _make_sid(symbol, ts_ms)



def _stable_u01(key: str, *, salt: str = "") -> float:
    """Deterministic pseudo-random in [0,1) from (salt|key)."""
    h = hashlib.md5((salt + "|" + key).encode("utf-8")).hexdigest()
    v = int(h[:8], 16)
    return float(v) / float(1 << 32)


def _should_sample(key: str, *, rate: float, salt: str = "") -> bool:
    r = rate
    if r >= 1.0:
        return True
    if r <= 0.0:
        return False
    return _stable_u01(key, salt=salt) < r



def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d



def _safe_loads(s: Any) -> dict[str, Any]:
    """
    Robust JSON loader for Redis-stored cfg.
    Supports both:
      1) canonical JSON object string: {"kind":"...","run_id":"..."}
      2) double-encoded JSON string: "\"{\\\"kind\\\":...}\""
    Always returns dict or {}.
    """
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    if isinstance(s, bytes):
        s = s.decode("utf-8", "ignore")
    raw = str(s).strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    # double-encoded: first pass -> str containing JSON
    if isinstance(obj, str):
        try:
            obj2 = json.loads(obj)
        except Exception:
            return {}
        return obj2 if isinstance(obj2, dict) else {}
    return obj if isinstance(obj, dict) else {}


def _safe_loads_ex(s: Any) -> tuple[dict[str, Any], str, int]:
    """
    Returns: (cfg_dict, err, raw_len)
      err == "" when cfg is a non-empty dict
      err != "" when missing/invalid/empty/not-dict
    
    Supports both:
      1) canonical JSON object string: {"kind":"...","run_id":"..."}
      2) double-encoded JSON string: "\"{\\\"kind\\\":...}\""
    """
    if s is None:
        return {}, "missing", 0
    if isinstance(s, bytes):
        s = s.decode("utf-8", "ignore")
    raw = str(s).strip()
    raw_len = len(raw)
    if not raw:
        return {}, "empty_dict", 0
    try:
        obj = json.loads(raw)
    except Exception as e:
        return {}, f"json_error:{type(e).__name__}", raw_len
    # double-encoded: first pass -> str containing JSON
    if isinstance(obj, str):
        try:
            obj2 = json.loads(obj)
        except Exception as e2:
            return {}, f"json_error_double:{type(e2).__name__}", raw_len
        if not isinstance(obj2, dict):
            return {}, f"not_dict_double:{type(obj2).__name__}", raw_len
        if not obj2:
            return {}, "empty_dict", raw_len
        return obj2, "", raw_len
    if not isinstance(obj, dict):
        return {}, f"not_dict:{type(obj).__name__}", raw_len
    if not obj:
        return {}, "empty_dict", raw_len
    return obj, "", raw_len



def _json_safe(x: Any) -> Any:
    """Best-effort JSON-safe conversion (for replay capture)."""
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            try:
                ks = str(k)
            except Exception:
                ks = "k"
            out[ks] = _json_safe(v)
        return out
    # numpy scalars, decimals, etc.
    try:
        if hasattr(x, "item"):
            return _json_safe(x.item())
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return str(x)



def _scenario_norm(s: str) -> str:
    # нормализация для совместимости с one-hot scenario_v4_*
    # "range_meanrev|..." -> "range_meanrev"
    # "range_meanrev:v2" -> "range_meanrev"
    # "range_meanrev@X" -> "range_meanrev"
    s0 = (s or "").strip().lower()
    if "|" in s0:
        s0 = s0.split("|", 1)[0].strip()
    if " " in s0:
        s0 = s0.split(" ", 1)[0].strip()
    if ":" in s0:
        s0 = s0.split(":", 1)[0].strip()
    if "@" in s0:
        s0 = s0.split("@", 1)[0].strip()
    return s0


def _bucket_from_scenario(s: str) -> str:
    s0 = _scenario_norm(s)
    if "range" in s0 or "meanrev" in s0 or "chop" in s0:
        return "range"
    if "trend" in s0 or "continuation" in s0 or "reversal" in s0:
        return "trend"
    return "other"



def _canon_sid(symbol: str, ts_ms: int, raw_sid: str = "") -> str:
    """Canonical sid for cross-stream joins: crypto-of:{SYMBOL}:{TS_MS} (no direction)."""
    sym = (symbol or "").upper() or "NA"
    try:
        ts = ts_ms
    except Exception:
        ts = 0
    s = (raw_sid or "")
    if s.startswith("crypto-of:"):
        head = s.split("|", 1)[0]
        parts = head.split(":", 2)
        if len(parts) == 3:
            sym2 = (parts[1] or sym).upper()
            try:
                ts2 = int(float(parts[2]))
            except Exception:
                ts2 = ts
            return f"crypto-of:{sym2}:{ts2}"
        return f"crypto-of:{sym}:{ts}"
    if "|" in s:
        # legacy: SYMBOL|TS|DIR
        try:
            p = s.split("|")
            sym2 = (p[0] or sym).upper()
            ts2 = int(float(p[1])) if len(p) > 1 else ts
            return f"crypto-of:{sym2}:{ts2}"
        except Exception:
            return f"crypto-of:{sym}:{ts}"
    return f"crypto-of:{sym}:{ts}"



def _stable_sample(key: str, sample_rate: float, *, salt: str) -> bool:
    """Deterministic sampling based on stable hash of (salt|key)."""
    try:
        r = sample_rate
    except Exception:
        r = 1.0
    if r >= 1.0:
        return True
    if r <= 0.0:
        return False
    u = int.from_bytes(hashlib.blake2b(f"{salt}|{key}".encode(), digest_size=8).digest(), "big")
    thr = int(r * 1_000_000)
    return (u % 1_000_000) < thr


def _mk_crypto_sid(symbol: str, ts_ms: int) -> str:
    """Create canonical SID: crypto-of:{symbol}:{ts_ms}"""
    return f"crypto-of:{symbol}:{ts_ms}"


def _normalize_crypto_sid(raw: object, *, symbol: str, ts_ms: int) -> str:
    """
    Normalize SID to canonical format: crypto-of:{symbol}:{ts_ms}
    
    Supports legacy formats:
      - crypto-of:{symbol}:{ts_ms} (already canonical)
      - {symbol}|{ts}|{dir} (legacy format)
      - {symbol}:{ts} (legacy without prefix)
      - empty -> generate from symbol+ts_ms
    """
    s = str(raw or "").strip()
    if s.startswith("crypto-of:"):
        return s
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0].strip() or symbol).strip()
            try:
                t = int(parts[1])
            except Exception:
                t = ts_ms
            if sym and t > 0:
                return _mk_crypto_sid(sym, t)
    # Accept legacy "SYMBOL:TS" without prefix (not "crypto-of:SYMBOL:TS")
    if s and (":" in s) and (not s.startswith("crypto-of:")) and ("|" not in s):
        p = s.split(":")
        if len(p) >= 2 and p[1].strip().isdigit():
            sym = (p[0].strip() or symbol).strip()
            t = int(p[1].strip())
            if sym and t > 0:
                return _mk_crypto_sid(sym, t)
    if (not s) and symbol and ts_ms > 0:
        return _mk_crypto_sid(symbol, ts_ms)
    return s



def _canonical_sid(indicators: dict[str, Any], symbol: str, ts_ms: int) -> str:
    """Generate canonical SID: crypto-of:{symbol}:{ts_ms}
    
    This is critical for join: metrics:ml_confirm ↔ trades:closed.
    """
    raw_sid = indicators.get("sid", "") or indicators.get("signal_id", "") or indicators.get("signalId", "") or ""
    return _normalize_crypto_sid(raw_sid, symbol=symbol.upper(), ts_ms=ts_ms)



def _stable_hash_u64(s: str) -> int:
    """Generate stable 64-bit hash from string (for deterministic sampling)"""
    h = hashlib.md5(s.encode("utf-8")).digest()[:8]
    return int.from_bytes(h, "big", signed=False)


# _stable_u01 is already defined at line 143 with salt support.



def _get_floor(util_floors: dict[str, Any], bucket: str) -> float:
    """
    champion JSON (v10.4) -> util_floors:
      {
        "global": { "floor": ... },
        "by_bucket": { "range": { "floor": ... }, ... },
        "unc_k": 0.5
      }
    """
    try:
        bb = util_floors.get("by_bucket") or {}
        if isinstance(bb, dict) and bucket in bb and isinstance(bb[bucket], dict):
            return float(bb[bucket].get("floor", util_floors.get("global", {}).get("floor", 0.0)))
        g = util_floors.get("global") or {}
        if isinstance(g, dict):
            return float(g.get("floor", 0.0))
    except Exception:
        pass
    return 0.0



