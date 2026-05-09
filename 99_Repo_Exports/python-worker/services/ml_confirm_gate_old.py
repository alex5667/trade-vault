from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import redis
from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.bucket2_v1 import derive_bucket2_label
from core.edge_stack_mh_v1 import EdgeStackMHModelV1
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

# Prometheus metrics (optional, fail-open if not available)
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    # Mock metrics for when prometheus_client is not available
    class _MockMetric:
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
        t = int(ts_ms)
    except Exception:
        t = 0
    return f"crypto-of:{sym}:{t}"


# Process-level shared caches to prevent redundant I/O and thundering herd.
# Keys: model_path or config_key. Values: loaded objects or dicts.
class BoundedLRUCache(collections.OrderedDict):
    """LRU Cache to prevent OOM when loading many ML models."""
    def __init__(self, maxsize: int = 30, *args: Any, **kwds: Any) -> None:
        self.maxsize = maxsize
        super().__init__(*args, **kwds)

    def __getitem__(self, key: Any) -> Any:
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def get(self, key: Any, default: Any = None) -> Any:
        if key in self:
            self.move_to_end(key)
            return super().__getitem__(key)
        return default

    def __setitem__(self, key: Any, value: Any) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest_key, oldest_value = self.popitem(last=False)
            del oldest_value
            # Explicit garbage collection helps to release native C/C++ memory
            # often used by ML libraries like XGBoost or LightGBM.

_SHARED_MODELS: BoundedLRUCache = BoundedLRUCache(maxsize=30)
_SHARED_CONFIGS: BoundedLRUCache = BoundedLRUCache(maxsize=30)
_SHARED_CONFIG_PAYLOADS: BoundedLRUCache = BoundedLRUCache(maxsize=30)  # key -> last raw payload
_SHARED_MODEL_STATS: BoundedLRUCache = BoundedLRUCache(maxsize=30) # path -> (mtime, size)
_SHARED_CALIBRATORS: BoundedLRUCache = BoundedLRUCache(maxsize=30)
_SHARED_CALIBRATOR_STATS: BoundedLRUCache = BoundedLRUCache(maxsize=30) # path -> (mtime, size)


def _load_model_cached(model_path: str, kind: str, logger: Any = None, force_stat_check: bool = True) -> Any | None:
    """Load model from disk or return from process-level cache if unchanged."""
    if not model_path:
        return None

    # Deep Cache: if model already in process-level memory and we don't want to hit disk
    if not force_stat_check and model_path in _SHARED_MODELS:
        return _SHARED_MODELS[model_path]

    if not os.path.exists(model_path):
        if logger:
            logger.debug(f"ML gate: Model path does not exist: {model_path}")
        return None

    try:
        mtime = os.path.getmtime(model_path)
        size = os.path.getsize(model_path)
    except Exception as e:
        if logger:
            logger.warning(f"ML gate: Failed to get stats for {model_path}: {e}")
        return None

    stats = (mtime, size)

    # Check cache
    if model_path in _SHARED_MODELS and _SHARED_MODEL_STATS.get(model_path) == stats:
        if logger:
            logger.debug(f"ML gate: Using cached model for {model_path} (kind={kind})")
        return _SHARED_MODELS[model_path]

    # Reload needed
    if logger:
        logger.info(f"ML gate: Loading model from {model_path} (kind={kind})")

    model = None
    try:
        if kind == "meta_lr":
            from core.meta_model_lr import MetaModelLR
            model = MetaModelLR.load(model_path)
        elif kind.startswith("util_mh_fastlinear") or model_path.lower().endswith(".json"):
            from core.fast_linear_util_mh import FastLinearUtilMHModel
            model = FastLinearUtilMHModel.load(model_path)
        else:
            if joblib:
                try:
                    model = joblib.load(model_path)
                except ModuleNotFoundError as e:
                    if "catboost" in str(e).lower():
                        if logger:
                            logger.error(f"ML gate: missing optional dependency 'catboost' for model {model_path}. Prediction may fail.")
                        return None
                    raise

        if model:
            # Validation
            kind_low = (kind or "").lower()
            if kind_low.startswith("util_mh"):
                if not hasattr(model, "predict_util") or not hasattr(model, "predict_unc"):
                    if logger:
                        logger.error(f"ML gate: Model at {model_path} missing predict_util/predict_unc methods")
                    return None
            elif kind_low == "edge_stack_v1":
                if not isinstance(model, dict) or model.get("kind") != "edge_stack_v1":
                    if logger:
                        logger.error(f"ML gate: Model at {model_path} is not a valid edge_stack_v1 pack")
                    return None
                required_keys = ["lr", "gbdt", "meta", "feature_cols"]
                if any(k not in model for k in required_keys):
                    if logger:
                        logger.error(f"ML gate: edge_stack_v1 model at {model_path} missing keys: {[k for k in required_keys if k not in model]}")
                    return None

                # Commit 12 (serve-side): EDGE_STACK_STRICT_FEATURE_COLS=1 rejects models with
                # scenario_v4_* one-hots to guarantee low-cardinality feature encoding.
                _strict_env = (os.environ.get("EDGE_STACK_STRICT_FEATURE_COLS", "0") or "0").strip().lower()
                if _strict_env in ("1", "true", "yes"):
                    _fcols = list(model.get("feature_cols", []) or [])
                    _bad = [c for c in _fcols if str(c).startswith("scenario_v4_")]
                    if _bad:
                        if logger:
                            logger.error(
                                f"ML gate: strict feature_cols rejects scenario_v4_* columns "
                                f"(found={_bad[:5]}); set EDGE_STACK_STRICT_FEATURE_COLS=0 to disable"
                            )
                        return None

            _SHARED_MODELS[model_path] = model
            _SHARED_MODEL_STATS[model_path] = stats
            if logger:
                logger.info(f"ML gate: Successfully loaded and cached model from {model_path} (type={type(model).__name__})")
            print(f"DEBUG: Successfully loaded model from {model_path}", flush=True)
    except Exception as e:
        print(f"DEBUG: Failed to load model from {model_path}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        if logger:
            logger.error(f"ML gate: Failed to load model from {model_path}: {e}")

    return model


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
                t = int(ts_ms) if str(ts_ms).isdigit() else 0
            return f'crypto-of:{sym}:{t}'
    if '|' in s:
        parts = s.split('|')
        if len(parts) >= 2:
            sym = (parts[0] or symbol or '').upper()
            try:
                t = int(parts[1])
            except Exception:
                t = int(ts_ms) if str(ts_ms).isdigit() else 0
            return f'crypto-of:{sym}:{t}'
    return _make_sid(symbol, ts_ms)


class _DictPackModelView:
    """Expose dict-pack model keys as attributes for _build_feature_row.

    _build_feature_row is written against an object interface (attrs like
    feature_cols/feature_transforms/robust_scaler/session_cfg/...).
    For edge_stack_v1 we load a dict-pack (joblib) and wrap it into this view
    to keep train==serve feature engineering consistent.
    """

    def __init__(self, pack: dict[str, Any]):
        self.feature_cols = list(pack.get("feature_cols", []) or [])
        tf = pack.get("feature_transforms")
        self.feature_transforms = tf if isinstance(tf, dict) else {}

        # RobustScalerPack accepts either RobustScalerPack or dict params.
        self.robust_scaler = pack.get("robust_scaler")

        sc = pack.get("session_cfg")
        self.session_cfg = sc if isinstance(sc, dict) else {}

        self.spread_bucket_edges = pack.get("spread_bucket_edges")

        lc = pack.get("liq_cfg")
        self.liq_cfg = lc if isinstance(lc, dict) else {}


def _stable_u01(key: str, *, salt: str = "") -> float:
    """Deterministic pseudo-random in [0,1) from (salt|key)."""
    h = hashlib.md5((salt + "|" + key).encode("utf-8")).hexdigest()
    v = int(h[:8], 16)
    return float(v) / float(1 << 32)


def _should_sample(key: str, *, rate: float, salt: str = "") -> bool:
    r = float(rate)
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
    from common.market_mode import is_range_regime
    if is_range_regime(s0):
        return "range"
    if "trend" in s0 or "continuation" in s0 or "reversal" in s0:
        return "trend"
    return "other"


def _find_forbidden_feature_cols(
    feature_cols: list[str],
    *,
    forbid_scenario_v4_onehot: bool,
) -> list[str]:
    """Return list of forbidden feature columns under strict schema rules.

    Purpose:
      - Guard serve path against scenario_v4_* one-hots that cause unbounded
        cardinality and train/serve skew.
      - Called after loading feature_cols from the model pack before inference.
    """
    bad: list[str] = []
    if bool(forbid_scenario_v4_onehot):
        for c in feature_cols:
            cs = str(c)
            if cs.startswith("scenario_v4_"):
                bad.append(cs)
    return bad


def _canon_sid(symbol: str, ts_ms: int, raw_sid: str = "") -> str:
    """Canonical sid for cross-stream joins: crypto-of:{SYMBOL}:{TS_MS} (no direction)."""
    sym = (symbol or "").upper() or "NA"
    try:
        ts = int(ts_ms)
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
        r = float(sample_rate)
    except Exception:
        r = 1.0
    if r >= 1.0:
        return True
    if r <= 0.0:
        return False
    u = int.from_bytes(hashlib.blake2b(f"{salt}|{key}".encode(), digest_size=8).digest(), "big")
    thr = int(r * 1_000_000)
    return int(u % 1_000_000) < thr


def _mk_crypto_sid(symbol: str, ts_ms: int) -> str:
    """Create canonical SID: crypto-of:{symbol}:{ts_ms}"""
    return f"crypto-of:{symbol}:{int(ts_ms)}"


def _normalize_crypto_sid(raw: object, *, symbol: str, ts_ms: int) -> str:
    """
    Normalize SID to canonical format: crypto-of:{symbol}:{ts_ms}
    
    Supports legacy formats:
      - crypto-of:{symbol}:{ts_ms} (already canonical)
      - {symbol}|{ts}|{dir} (legacy format)
      - {symbol}:{ts} (legacy without prefix)
      - empty -> generate from symbol+ts_ms
    """
    s = (raw or "").strip()
    if s.startswith("crypto-of:"):
        return s
    if "|" in s:
        parts = s.split("|")
        if len(parts) >= 2:
            sym = (parts[0].strip() or symbol).strip()
            try:
                t = int(parts[1])
            except Exception:
                t = int(ts_ms)
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
    if (not s) and symbol and int(ts_ms) > 0:
        return _mk_crypto_sid(symbol, int(ts_ms))
    return s


def _canonical_sid(indicators: dict[str, Any], symbol: str, ts_ms: int) -> str:
    """Generate canonical SID: crypto-of:{symbol}:{ts_ms}
    
    This is critical for join: metrics:ml_confirm ↔ trades:closed.
    """
    raw_sid = indicators.get("sid", "") or indicators.get("signal_id", "") or indicators.get("signalId", "") or ""
    return _normalize_crypto_sid(raw_sid, symbol=symbol.upper(), ts_ms=int(ts_ms))


def cache_ml_decision(
    r: redis.Redis,
    *,
    sid: str,
    symbol: str,
    bucket: str,
    p_edge: float,
    enforce: int,
    ok_rule: int,
    missing: int,
    model_ver: str,
    ttl_sec: int = 7 * 24 * 3600,
) -> None:
    """
    Cache ML decision for outcome emitter join.
    
    Writes to ml:dec:{sid} key with TTL (default 7 days).
    This allows outcome emitter to do O(1) join on position close.
    
    Args:
        r: Redis client (decode_responses=True)
        sid: Signal ID (canonical format: crypto-of:SYMBOL:ts_ms)
        symbol: Trading symbol
        bucket: Bucket (trend/range/other)
        p_edge: Predicted edge probability
        enforce: Whether decision was enforced (1) or shadow (0)
        ok_rule: Whether rule gate passed (1) or failed (0)
        missing: Whether critical features were missing (1) or not (0)
        model_ver: Model version string
        ttl_sec: TTL in seconds (default: 7 days)
    """
    key = f"ml:dec:{sid}"
    payload = {
        "sid": sid,
        "symbol": symbol.upper(),
        "bucket": str(bucket).lower(),
        "p_edge": float(p_edge),
        "enforce": int(enforce),
        "ok_rule": int(ok_rule),
        "missing": int(missing),
        "model_ver": str(model_ver),
        "ts_ms": int(_now_ms()),
    }
    try:
        r.set(key, json.dumps(payload, separators=(",", ":")), ex=ttl_sec)
    except Exception:
        # Fail-open: don't break decision flow if cache write fails
        pass


def _stable_hash_u64(s: str) -> int:
    """Generate stable 64-bit hash from string (for deterministic sampling)"""
    h = hashlib.md5(s.encode("utf-8")).digest()[:8]
    return int.from_bytes(h, "big", signed=False)




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


class MLConfirmConfig(BaseModel):
    """
    Pydantic schema for ML gate configuration validation.
    Enforces P0 range constraints for probability thresholds (p_min).
    """
    model_config = ConfigDict(extra="allow")  # Keep extra fields for variant-specific logic

    p_min: float = 0.52
    p_min_by_bucket: dict[str, float] = Field(default_factory=dict)
    util_floors: dict[str, Any] = Field(default_factory=dict)
    edge_floors: dict[str, Any] = Field(default_factory=dict)

    @field_validator("p_min")
    @classmethod
    def validate_p_min(cls, v: float) -> float:
        # Floor lowered to 0.0 to allow shadow/canary configs with p_min<0.5
        # (champion cfg:ml_confirm:champion uses p_min=0.2 in SHADOW mode).
        # In ENFORCE mode the gate remains meaningful because p_min_by_bucket still
        # enforces [0.5, 0.95] per-bucket, and edge_floors has its own [0.5, 0.95] guard.
        if not (0.0 <= v <= 0.95):
            raise ValueError(f"p_min must be in range [0.0, 0.95], got {v}")
        return v

    @field_validator("p_min_by_bucket")
    @classmethod
    def validate_p_min_by_bucket(cls, v: dict[str, float]) -> dict[str, float]:
        for k, val in v.items():
            if not (0.5 <= val <= 0.95):
                raise ValueError(f"p_min_by_bucket[{k}] must be in range [0.5, 0.95], got {val}")
        return v

    @field_validator("edge_floors")
    @classmethod
    def validate_edge_floors_dict(cls, v: dict[str, Any], info) -> dict[str, Any]:
        if not v:
            return v

        field_name = info.field_name
        # Validate global.floor
        g = v.get("global") or {}
        if isinstance(g, dict) and "floor" in g:
            f = float(g["floor"])
            if not (0.5 <= f <= 0.95):
                raise ValueError(f"{field_name}.global.floor must be in range [0.5, 0.95], got {f}")

        # Validate by_bucket.*.floor
        bb = v.get("by_bucket") or {}
        if isinstance(bb, dict):
            for k, bucket_cfg in bb.items():
                if isinstance(bucket_cfg, dict) and "floor" in bucket_cfg:
                    f = float(bucket_cfg["floor"])
                    if not (0.5 <= f <= 0.95):
                        raise ValueError(f"{field_name}.by_bucket[{k}].floor must be in range [0.5, 0.95], got {f}")
        return v


@dataclass
class MLConfirmDecision:
    mode: str = "OFF"          # OFF|SHADOW|ENFORCE|ERR
    kind: str = "none"         # util_mh_v1|...
    allow: bool = True

    # backwards compatible fields used by OFConfirmEngine final_reason (p_edge/p_min)
    p_edge: float = 0.0
    p_min: float = 0.0

    # util_mh fields
    best_h_ms: int = 0
    score: float = 0.0
    floor: float = 0.0
    bucket: str = "other"
    util_pred: dict[str, float] | None = None
    unc: dict[str, float] | None = None
    missing: list[str] | None = None

    model_run_id: str = ""
    model_path: str = ""
    reason: str = ""
    error: str = ""

    # SRE / perf
    latency_us: int = 0

    # SRE / quality (selective prediction)
    abstain: bool = False
    conf: float = 0.0        # 0..1 proxy (see below)
    p_margin: float = 0.0    # p_edge - p_min (works for util_mh too)
    status: str = ""         # ALLOW|BLOCK|ABSTAIN_*|MISSING_*|SHADOW|OFF|ERR

    # calibration fields (for metrics and drift tracking)
    p_edge_raw: float = 0.0   # pre-calibration probability
    p_edge_cal: float = 0.0   # post-calibration probability (effective p_edge)
    calib_type: str = ""      # platt_logit|none

    # expert recommendations & risk fields (P74+)
    exec_risk_ref_bps: float = 0.0
    exec_risk_bps: float = 0.0
    exec_risk_norm: float = 0.0
    exec_pen: float = 0.0
    score_breakdown_small: dict[str, Any] | None = None
    score_breakdown_json: str = ""

    # cfg diagnostics (for metrics/debug)
    cfg_key_used: str = ""
    cfg_source: str = ""        # champion|challenger
    cfg_raw_len: int = 0
    cfg_parse_err: str = ""

    # per-symbol mode resolution (for observability)
    effective_mode: str = ""    # resolved mode after per-symbol overrides
    mode_source: str = ""      # global|cfg_per_symbol|env_per_symbol|canary|cfg_per_symbol_canary

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "kind": self.kind,
            "allow": bool(self.allow),
            "p_edge": float(self.p_edge),
            "p_min": float(self.p_min),
            "best_h_ms": int(self.best_h_ms),
            "score": float(self.score),
            "floor": float(self.floor),
            "bucket": str(self.bucket),
            "util_pred": self.util_pred or {},
            "unc": self.unc or {},
            "missing": self.missing or [],
            "model_run_id": self.model_run_id,
            "model_path": self.model_path,
            "reason": self.reason,
            "error": self.error,
            "latency_us": int(self.latency_us),
            "abstain": int(bool(self.abstain)),
            "conf": float(self.conf),
            "p_margin": float(self.p_margin),
            "status": str(self.status),
            "p_edge_raw": float(self.p_edge_raw),
            "p_edge_cal": float(self.p_edge_cal),
            "calib_type": str(self.calib_type or ""),
            "cfg_key_used": str(self.cfg_key_used or ""),
            "cfg_source": str(self.cfg_source or ""),
            "cfg_raw_len": int(self.cfg_raw_len),
            "cfg_parse_err": str(self.cfg_parse_err or ""),
            "effective_mode": str(self.effective_mode or self.mode),
            "mode_source": str(self.mode_source or "global"),

            # P74+
            "exec_risk_ref_bps": float(self.exec_risk_ref_bps),
            "exec_risk_bps": float(self.exec_risk_bps),
            "exec_risk_norm": float(self.exec_risk_norm),
            "exec_pen": float(self.exec_pen),
            "score_breakdown_small": self.score_breakdown_small or {},
            "score_breakdown_json": str(self.score_breakdown_json or ""),
        }


class MLConfirmGate:
    """
    v10.4 util_mh_v1:
      score_h = pred_util_h - unc_k*unc_h
      best = max_h(score_h)
      allow if best_score >= util_floor(bucket)

    ENV:
      REDIS_URL
      ML_CONFIRM_MODE=OFF|SHADOW|ENFORCE
      ML_CONFIRM_FAIL_POLICY=OPEN|CLOSED
      ML_CFG_CHAMPION_KEY=cfg:ml_confirm:champion
      ML_CFG_CHALLENGER_KEY=cfg:ml_confirm:challenger
      ML_MODEL_CACHE_TTL_MS=60000
      EXEC_RISK_REF_BPS=10   (fallback ref if exec_risk_norm missing)

      # metrics
      ML_CONFIRM_METRICS_STREAM=metrics:ml_confirm
      ML_CONFIRM_METRICS_ENABLE=1
      ML_CONFIRM_METRICS_SAMPLE=1.0

      # selective prediction knobs (default OFF -> behavior unchanged)
      ML_CONFIRM_ABSTAIN_BAND=0.0       # if |p_edge - p_min| <= band -> ABSTAIN (allow=True)
      ML_CONFIRM_CONF_MIN=0.0           # if conf < conf_min -> ABSTAIN (allow=True)
      ML_CONFIRM_ABSTAIN_ON_MISSING=0   # if 1, missing_critical in ENFORCE -> ABSTAIN (allow=True) instead of block
      ML_CONFIRM_P_MIN_HARD_FLOOR=0.0   # p_min = max(cfg_floor, hard_floor)

      # golden replay capture
      ML_REPLAY_CAPTURE_ENABLE=0
      ML_REPLAY_INPUTS_STREAM=stream:ml_confirm:inputs
      ML_REPLAY_INPUTS_SAMPLE=0.01
      ML_REPLAY_INPUTS_MAXLEN=200000
    """

    def __init__(
        self,
        *,
        r: redis.Redis,
        mode: str,
        fail_policy: str,
        champion_key: str,
        challenger_key: str,
        champion_kinds: list[str] | None = None,
        ab_variant: str = "",
    ) -> None:
        self.r = r

        # Parse AB variant
        self.ab_variant = (ab_variant or "").strip().lower()

        # Default mode parsing
        self.mode = (mode or "OFF").upper()

        # --- A/B Variant Overrides ---
        if self.ab_variant in ("shadow", "enforce", "off"):
            self.mode = self.ab_variant.upper()
        elif self.ab_variant == "challenger" and challenger_key:
            # Route all traffic to Challenger instead of Champion
            champion_key = challenger_key

        self.fail_policy = (fail_policy or "OPEN").upper()
        self.champion_key = champion_key
        self.challenger_key = challenger_key

        self._cfg_source = "none"  # champion|challenger|hash_fallback|none
        self._cfg_hash_key = os.getenv("ML_CFG_HASH_KEY", "cfg:ml_confirm")

        self._cache_loaded_ms: int = 0
        self._cache_ttl_ms: int = int(os.getenv("ML_MODEL_CACHE_TTL_MS", "60000"))
        self._cfg: dict[str, Any] = {}
        self._model: Any = None
        self._model_load_error: str = ""  # Detailed error reason when model fails to load
        self._last_error_log_ms: int = 0  # Throttle error logging
        self._check_call_count: int = 0  # Throttle DEBUG check log

        # Multi-kind (Phase 2): per-kind cfg & model isolation
        self._champion_kinds: list[str] = [k.strip().lower() for k in (champion_kinds or []) if k.strip()]
        self._cfgs: dict[str, dict[str, Any]] = {}      # kind → cfg dict
        self._models: dict[str, Any] = {}                 # kind → model
        self._cfg_sources: dict[str, str] = {}            # kind → source (champion|challenger|...)
        self._cfg_keys_used: dict[str, str] = {}          # kind → Redis key used

        # last cfg diagnostics (used when returning ERR_* decisions)
        self._cfg_key_used: str = ""
        self._cfg_source: str = ""
        self._cfg_raw_len: int = 0
        self._cfg_parse_err: str = ""

        # metrics
        self._metrics_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
        self._metrics_enable = int(os.getenv("ML_CONFIRM_METRICS_ENABLE", "1") or 1) == 1
        try:
            self._metrics_sample = float(os.getenv("ML_CONFIRM_METRICS_SAMPLE", "1.0") or 1.0)
        except Exception:
            self._metrics_sample = 1.0

        # selective prediction defaults (OFF unless enabled)
        try:
            self._abstain_band = float(os.getenv("ML_CONFIRM_ABSTAIN_BAND", "0.0") or 0.0)
        except Exception:
            self._abstain_band = 0.0
        try:
            self._conf_min = float(os.getenv("ML_CONFIRM_CONF_MIN", "0.0") or 0.0)
        except Exception:
            self._conf_min = 0.0
        self._abstain_on_missing = int(os.getenv("ML_CONFIRM_ABSTAIN_ON_MISSING", "0") or 0) == 1
        try:
            self._p_min_hard_floor = float(os.getenv("ML_CONFIRM_P_MIN_HARD_FLOOR", "0.0") or 0.0)
        except Exception:
            self._p_min_hard_floor = 0.0

        # golden replay capture
        self._replay_capture = int(os.getenv("ML_REPLAY_CAPTURE_ENABLE", "0") or 0) == 1
        self._replay_stream = os.getenv("ML_REPLAY_INPUTS_STREAM", "stream:ml_confirm:inputs")
        try:
            self._replay_sample = float(os.getenv("ML_REPLAY_INPUTS_SAMPLE", "0.01") or 0.01)
        except Exception:
            self._replay_sample = 0.01
        self._replay_maxlen = int(os.getenv("ML_REPLAY_INPUTS_MAXLEN", "200000") or 200000)

        # calibration layer (optional)
        self._calibrator: PlattLogitCalibrator | None = None
        self._calibrate_enabled = int(os.getenv("ML_CALIBRATION_ENABLE", "1") or 1) == 1
        self._calib_type = "none"

        # per-symbol mode overrides (populated from champion JSON mode_overrides block)
        self._mode_by_symbol: dict[str, str] = {}
        self._enforce_share_by_symbol: dict[str, float] = {}

        # Multi-kind per-symbol overrides: kind → {symbol: mode}
        self._mode_by_symbol_by_kind: dict[str, dict[str, str]] = {}
        self._enforce_share_by_sym_by_kind: dict[str, dict[str, float]] = {}

        # strict feature schema (guards against unbounded feature cardinality at serve time)
        self._strict_feature_cols = (
            int(os.getenv("ML_STRICT_FEATURE_COLS", os.getenv("STRICT_FEATURE_COLS", "0")) or 0) == 1
        )
        _env_forbid = os.getenv("ML_FORBID_SCENARIO_V4_ONEHOT", os.getenv("FORBID_SCENARIO_V4_ONEHOT"))
        if _env_forbid is None:
            # strict mode implies this guard unless explicitly overridden
            self._forbid_scenario_v4_onehot = bool(self._strict_feature_cols)
        else:
            self._forbid_scenario_v4_onehot = int(_env_forbid or "0") == 1

        # Use centralized metrics from registry (fail-open if not available)
        # Note: metrics_registry defines metrics with same names, so we can use them directly
        # We keep local references for backward compatibility and to handle mock metrics
        if METRICS_REGISTRY_AVAILABLE:
            self._metrics_events_total = ml_confirm_events_total
            self._metrics_errors_total = ml_confirm_errors_total
            self._metrics_cfg_present = ml_confirm_cfg_present
            self._metrics_cfg_valid = ml_confirm_cfg_valid
            self._metrics_enforce_share = ml_confirm_enforce_share
            self._metrics_model_loaded = ml_confirm_model_loaded
            self._metrics_model_load_seconds = ml_confirm_model_load_seconds
            self._metrics_latency_seconds = ml_confirm_latency_seconds
            # Additional local metric for last successful load timestamp
            if PROMETHEUS_AVAILABLE:
                try:
                    self._metrics_last_successful_load_ts = Gauge(
                        "ml_confirm_last_successful_load_ts_seconds",
                        "Timestamp of last successful model load",
                        ["kind"]
                    )
                except ValueError:
                    # In tests/multiple instances, might already be registered
                    # Try to retrieve from global registry or just mock it if we can't find it
                    # For simplicity, we can use a mock if it fails here or assume it's already there
                    # Better: use a module-level lock or registry.
                    from prometheus_client import REGISTRY
                    self._metrics_last_successful_load_ts = REGISTRY._names_to_collectors.get("ml_confirm_last_successful_load_ts_seconds")
                    if self._metrics_last_successful_load_ts is None:
                        class _MockMetric:
                            def labels(self, **kwargs): return self
                            def set(self, *args, **kwargs): pass
                        self._metrics_last_successful_load_ts = _MockMetric()
            else:
                class _MockMetric:
                    def labels(self, **kwargs):
                        return self
                    def set(self, *args, **kwargs):
                        pass
                self._metrics_last_successful_load_ts = _MockMetric()
            # cfg_defaulted_total is tracked via ml_missing_critical_total
            self._metrics_cfg_defaulted_total = ml_missing_critical_total
        else:
            # Mock metrics when registry is not available
            class _MockMetric:
                def labels(self, **kwargs):
                    return self
                def inc(self, *args, **kwargs):
                    pass
                def set(self, *args, **kwargs):
                    pass
                def observe(self, *args, **kwargs):
                    pass
            self._metrics_events_total = _MockMetric()
            self._metrics_errors_total = _MockMetric()
            self._metrics_cfg_present = _MockMetric()
            self._metrics_cfg_valid = _MockMetric()
            self._metrics_enforce_share = _MockMetric()
            self._metrics_model_loaded = _MockMetric()
            self._metrics_model_load_seconds = _MockMetric()
            self._metrics_last_successful_load_ts = _MockMetric()
            self._metrics_latency_seconds = _MockMetric()
            self._metrics_cfg_defaulted_total = _MockMetric()

    @staticmethod
    def from_env() -> MLConfirmGate:
        # Support ML_REDIS_URL for separate config Redis, fallback to REDIS_URL
        redis_url = os.getenv("ML_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = redis.Redis.from_url(redis_url, decode_responses=True)

        mode = os.getenv("ML_CONFIRM_MODE", "SHADOW")
        fail_policy = os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN")
        champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
        challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")

        # Multi-kind support: comma-separated list of kinds to load independently
        # e.g. ML_CFG_CHAMPION_KINDS="util_mh_v1,edge_stack_v1,meta_lr"
        kinds_raw = os.getenv("ML_CFG_CHAMPION_KINDS", "")
        ab_variant = os.getenv("ML_CONFIRM_AB_VARIANT", "")
        champion_kinds = [k.strip() for k in kinds_raw.split(",") if k.strip()] if kinds_raw else None

        return MLConfirmGate(
            r=r,
            mode=mode,
            fail_policy=fail_policy,
            champion_key=champion_key,
            challenger_key=challenger_key,
            champion_kinds=champion_kinds,
            ab_variant=ab_variant,
        )

    def _fail_allow(self) -> bool:
        # FAIL_OPEN => allow, FAIL_CLOSED => block
        return self.fail_policy != "CLOSED"

    def _coerce_hash_cfg(self, h: dict[str, str]) -> dict[str, Any]:
        """
        HGETALL returns strings. Keep as strings, parsing happens downstream (float/int) in existing code.
        """
        cfg: dict[str, Any] = {}
        for k, v in h.items():
            cfg[str(k)] = v
        cfg.setdefault("mode", "SHADOW")
        cfg.setdefault("fail_policy", "OPEN")
        cfg.setdefault("enforce_share", 0.05)
        return cfg

    def _load_cfg_and_model(self) -> tuple[dict[str, Any], Any]:
        """
        Load configuration and model from Redis with shared process-level caching.
        
        If ML_CFG_CHAMPION_KINDS is set, loads per-kind configs FIRST, then falls
        back to legacy single-key loading for backward compat.
        """
        import logging

        logger = logging.getLogger("ml_confirm_gate")

        # Phase 2: Multi-kind loading
        if self._champion_kinds:
            self._load_per_kind_configs(logger)

        self._cfg_key_used = self.champion_key
        self._cfg_source = "none"
        self._cfg_raw_len = 0
        self._cfg_parse_err = ""
        self._model_load_error = ""


        raw_payload = None

        # Step 1: Resolve raw payload from Redis
        try:
            # 1a. Try Champion
            raw_p = self.r.get(self.champion_key)
            if raw_p:
               try:
                   p = _safe_loads(raw_p)
                   if isinstance(p, dict) and p:
                       raw_payload = raw_p
                       self._cfg_source = "champion"
                       self._cfg_key_used = self.champion_key
               except Exception:
                   pass

            # 1b. Try Challenger only when ab_variant explicitly == "challenger".
            # NOTE: when ab_variant=="challenger", __init__ already sets
            # self.champion_key = challenger_key, so 1a above already loaded it.
            # This branch handles the edge-case where champion_key was overridden
            # but the key was empty; we retry using the original challenger_key.
            # Do NOT load challenger as a generic SHADOW fallback — that would
            # silently widen challenger traffic beyond the A/B scope.
            if not raw_payload and self.ab_variant == "challenger" and self.challenger_key != self.champion_key:
                raw_p = self.r.get(self.challenger_key)
                if raw_p:
                    try:
                        p = _safe_loads(raw_p)
                        if isinstance(p, dict) and p:
                            raw_payload = raw_p
                            self._cfg_source = "challenger"
                            self._cfg_key_used = self.challenger_key
                    except Exception:
                        pass

            # 1c. Hash fallback
            if not raw_payload:
                h = self.r.hgetall(self._cfg_hash_key)
                if h and isinstance(h, dict) and len(h) > 0:
                    cfg_dict = self._coerce_hash_cfg(h)
                    self._cfg_source = "hash_fallback"
                    self._cfg_key_used = self._cfg_hash_key
                    # Represent as JSON for the cache/metrics
                    raw_payload = json.dumps(cfg_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

                    # Bootstrap if needed (legacy behavior expected by tests)
                    self.r.set(self.champion_key, raw_payload)
        except Exception as e:
            logger.error(f"ML gate: Redis error in _load_cfg_and_model: {e}")
            raw_payload = None

        if not raw_payload:
            self._model_load_error = "no_cfg"
            # If per-kind configs were loaded, use first as primary
            if self._cfgs:
                first_kind = next(iter(self._cfgs))
                return self._cfgs[first_kind].copy(), self._models.get(first_kind)
            return {}, None

        if not raw_payload:
            self._model_load_error = "no_cfg"
            return {}, None

        cfg, model = self._parse_and_load_from_payload(raw_payload, id(self.r), logger, self._cfg_key_used)

        # Register legacy-loaded config in multi-kind registry too
        if cfg:
            kind = (cfg.get("kind", "")).strip().lower()
            if kind and kind not in self._cfgs:
                self._cfgs[kind] = cfg.copy()
                self._models[kind] = model
                self._cfg_sources[kind] = self._cfg_source
                self._cfg_keys_used[kind] = self._cfg_key_used

        return cfg, model

    def _load_per_kind_configs(self, logger: Any) -> None:
        """
        Load per-kind champion configs from Redis.
        
        For each kind in _champion_kinds, tries:
          1. cfg:ml_confirm:champion:{kind}  (per-kind key)
          2. Falls through to legacy key (handled by caller)
        
        Populates _cfgs[kind], _models[kind], and per-kind mode_overrides.
        """
        for kind in self._champion_kinds:
            per_kind_key = f"{self.champion_key}:{kind}"
            try:
                raw_p = self.r.get(per_kind_key)
                if not raw_p:
                    continue

                p = _safe_loads(raw_p)
                if not isinstance(p, dict) or not p:
                    continue

                # Parse and load per-kind config
                cfg, model = self._parse_and_load_from_payload(
                    raw_p, id(self.r), logger, per_kind_key
                )
                if not cfg:
                    continue

                # Verify kind matches
                cfg_kind = (cfg.get("kind", "")).strip().lower()
                if cfg_kind and cfg_kind != kind:
                    logger.warning(
                        f"ML gate: per-kind key {per_kind_key} has kind={cfg_kind}, "
                        f"expected {kind}. Using cfg_kind={cfg_kind}."
                    )

                effective_kind = cfg_kind or kind
                self._cfgs[effective_kind] = cfg.copy()
                self._models[effective_kind] = model
                self._cfg_sources[effective_kind] = "champion_per_kind"
                self._cfg_keys_used[effective_kind] = per_kind_key

                # Parse per-kind mode_overrides
                mo = cfg.get("mode_overrides")
                if isinstance(mo, dict):
                    by_sym = mo.get("by_symbol")
                    if isinstance(by_sym, dict):
                        _allowed = {"OFF", "SHADOW", "CANARY", "ENFORCE"}
                        parsed = {}
                        for sym, m in by_sym.items():
                            m_up = str(m).strip().upper()
                            if m_up in _allowed:
                                parsed[str(sym).strip().upper()] = m_up
                        if parsed:
                            self._mode_by_symbol_by_kind[effective_kind] = parsed

                    es_sym = mo.get("enforce_share_by_symbol")
                    if isinstance(es_sym, dict):
                        parsed_es = {}
                        for sym, share in es_sym.items():
                            try:
                                sv = float(share)
                                if 0.0 <= sv <= 1.0:
                                    parsed_es[str(sym).strip().upper()] = sv
                            except (TypeError, ValueError):
                                pass
                        if parsed_es:
                            self._enforce_share_by_sym_by_kind[effective_kind] = parsed_es

                logger.info(
                    f"ML gate: loaded per-kind config for {effective_kind} "
                    f"from {per_kind_key} (model={cfg.get('model_path', 'N/A')})"
                )
            except Exception as e:
                logger.warning(f"ML gate: failed to load per-kind config for {kind}: {e}")

    def _parse_and_load_from_payload(self, raw_payload: Any, cache_key_id: Any, logger: Any, cfg_key_used: str) -> tuple[dict[str, Any], Any]:
        self._cfg_raw_len = len(raw_payload)

        # Step 2: Check process-level cache for JSON payloads using a unified key across sync/async clients
        cache_key = ("global_ml_gate", cfg_key_used)
        if _SHARED_CONFIG_PAYLOADS.get(cache_key) == raw_payload:
            cached_cfg = _SHARED_CONFIGS.get(cache_key)
            if cached_cfg:
                model_path = cached_cfg.get("model_path")
                kind = (cached_cfg.get("kind", "")).lower()
                # Deep Cache: Payload matches, skip disk stat check unless forced
                model = _load_model_cached(model_path, kind, logger=logger, force_stat_check=False)
                return cached_cfg.copy(), model

        # Step 3: Parse and Validate
        from core.champion_cfg_validator import validate_champion_cfg
        try:
            payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else str(raw_payload)
            cfg = _safe_loads(payload_str)
            if not isinstance(cfg, dict):
                cfg = {}
            try:
                cfg_validated, validation_info = validate_champion_cfg(payload_str)
                # Ensure validated fields are mapped if validation succeeded
                cfg["model_path"] = cfg_validated.model_path
                cfg["kind"] = cfg_validated.kind
                cfg["mode"] = cfg_validated.mode
                cfg["enforce_share"] = cfg_validated.enforce_share
                cfg["run_id"] = cfg_validated.run_id
            except Exception as ve:
                # Lenient mode for top-level infra: Log warning but keep the parsed JSON
                logger.warning(f"ML gate: Basic champion validation failed for {cfg_key_used}, but using as-is (legacy): {ve}")

            # P0 Fix: Strict Pydantic validation for internal ML thresholds (p_min bounds)
            try:
                # This will raise ValidationError if p_min is out of [0.5, 0.95]
                ml_cfg_obj = MLConfirmConfig.model_validate(cfg)
                # Update cfg with validated/defaulted values from Pydantic model
                # to ensure we use the sanitized values downstream.
                validated_dict = ml_cfg_obj.model_dump()
                for k, v in validated_dict.items():
                    cfg[k] = v
            except Exception as ve:
                # Strict mode for thresholds: fail the whole config load if P0 invariants are violated
                logger.error(f"ML gate: Pydantic validation failed for {cfg_key_used} (P0 constraint violated): {ve}")
                raise ValueError(f"p_min_validation_failed: {ve}") from ve

            # Update cache
            _SHARED_CONFIG_PAYLOADS[cache_key] = raw_payload
            _SHARED_CONFIGS[cache_key] = cfg

            # Load model
            model_path = cfg.get("model_path")
            kind = cfg.get("kind")
            model = _load_model_cached(model_path, kind, logger=logger)

            if METRICS_REGISTRY_AVAILABLE:
                k = kind or "unknown"
                self._metrics_cfg_present.labels(kind=k).set(1)
                self._metrics_cfg_valid.labels(kind=k).set(1)
                if model:
                    self._metrics_model_loaded.labels(kind=k).set(1)

            return cfg.copy(), model

        except Exception as e:
            # Match legacy error reporting for tests
            err_msg = str(e)
            self._cfg_parse_err = f"invalid_cfg({err_msg})" if ("mode" in err_msg or "enforce_share" in err_msg) else err_msg
            self._model_load_error = f"parse_error:{type(e).__name__}"
            logger.error(f"ML gate: Config parse/validate failed for {cfg_key_used}: {e}")
            return {}, None

    async def refresh_async(self, redis_async: Any) -> None:
        """
        Async version of _refresh_cache_if_needed to eliminate blocking calls in main loop.
        """
        import json
        import logging
        logger = logging.getLogger("ml_confirm_gate")

        if self.mode == "OFF":
            self._cfg, self._model = {}, None
            return

        now = _now_ms()
        # Use existing TTL
        if self._cache_loaded_ms and (now - self._cache_loaded_ms) < self._cache_ttl_ms:
            return

        # Protect test overrides
        if not self._cache_loaded_ms and self._cfg and self._model:
            self._cache_loaded_ms = now
            return

        # 2. Parse & Load (Run in thread to avoid blocking loop depending on model size)
        loop = asyncio.get_running_loop()
        t_start = time.monotonic()
        t_redis = 0.0
        t_parse = 0.0
        t_per_kind = 0.0
        t_calib = 0.0

        try:
            # Phase 1: Wait for all parallel tasks (timeout protected)
            # Use 10s timeout to ensure we don't stick the main loop forever if Redis hangs
            t0_redis = time.monotonic()
            async with asyncio.timeout(10.0):
                # We start multi-kind loading concurrently with main loading if kinds are known
                tasks = []
                if self._champion_kinds:
                    tasks.append(self._load_per_kind_configs_async(redis_async, logger))

                # Main config load (P1-FIX: 2s timeout per Redis GET to prevent 745ms hangs)
                async def load_main():
                    self._cfg_key_used = self.champion_key
                    self._cfg_source = "none"
                    try:
                        raw_p = await asyncio.wait_for(redis_async.get(self.champion_key), timeout=2.0)
                    except TimeoutError:
                        logger.warning("ML gate: Redis GET timeout (2s) for champion key, keeping cached config")
                        return None
                    if not raw_p and self.ab_variant == "challenger" and self.challenger_key != self.champion_key:
                        try:
                            raw_p = await asyncio.wait_for(redis_async.get(self.challenger_key), timeout=2.0)
                        except TimeoutError:
                            logger.warning("ML gate: Redis GET timeout (2s) for challenger key")
                            return None
                        if raw_p:
                            self._cfg_source = "challenger"
                            self._cfg_key_used = self.challenger_key
                    elif raw_p:
                        self._cfg_source = "champion"
                        self._cfg_key_used = self.champion_key

                    if not raw_p:
                         # Hash Fallback
                         h = await redis_async.hgetall(self._cfg_hash_key)
                         if h and isinstance(h, dict) and len(h) > 0:
                              cfg_dict = self._coerce_hash_cfg(h)
                              self._cfg_source = "hash_fallback"
                              self._cfg_key_used = self._cfg_hash_key
                              return json.dumps(cfg_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    return raw_p

                tasks.append(load_main())
                results = await asyncio.gather(*tasks, return_exceptions=True)
                t_redis = time.monotonic() - t0_redis

                # Filter results
                raw_payload = None
                for res in results:
                    if isinstance(res, Exception):
                        import redis.exceptions as redis_exceptions
                        is_timeout = isinstance(res, (redis_exceptions.ConnectionError, redis_exceptions.TimeoutError, asyncio.TimeoutError, TimeoutError)) or "TimeoutError" in type(res).__name__
                        if is_timeout:
                            logger.warning(f"ML gate: Async refresh part timed out/connection error: {res}")
                        else:
                            logger.error(f"ML gate: Async refresh part failed: {res}")
                        continue
                    if isinstance(res, (bytes, str)):
                        raw_payload = res
                    # Special internal marker for _load_per_kind_configs_async completion time
                    if isinstance(res, float):
                        t_per_kind = res

            if not raw_payload:
                self._model_load_error = "no_cfg"
                if self._cfgs:
                    first_kind = next(iter(self._cfgs))
                    self._cfg = self._cfgs[first_kind].copy()
                    self._model = self._models.get(first_kind)
                    self._cache_loaded_ms = now
                    self._refresh_selective_knobs_from_cfg()
                return

            # Parse & Load main
            t0_parse = time.monotonic()

            # Fast path in event loop to avoid executor queue lag
            has_major_changes = True
            cache_key = ("global_ml_gate", self._cfg_key_used)
            if _SHARED_CONFIG_PAYLOADS.get(cache_key) == raw_payload:
                has_major_changes = False
            else:
                # Fast JSON peek for telemetry drift (same run_id/model)
                try:
                    p_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else str(raw_payload)
                    maybe_cfg = await loop.run_in_executor(None, _safe_loads, p_str)
                    cached_cfg = _SHARED_CONFIGS.get(cache_key)
                    if cached_cfg and isinstance(maybe_cfg, dict):
                        if cached_cfg.get("run_id") == maybe_cfg.get("run_id") and cached_cfg.get("model_path") == maybe_cfg.get("model_path"):
                            has_major_changes = False
                except Exception:
                    pass

            if not has_major_changes and self._cfg and self._model:
                # P-LAG-FIX: Skip redundant re-load/executor call if the model and config are already in memory
                # and haven't changed. This eliminates a potential 3.5s event loop stall.
                cfg, model = self._cfg, self._model
            else:
                cfg, model = await loop.run_in_executor(
                    None,
                    self._parse_and_load_from_payload,
                    raw_payload,
                    "global_ml_gate",
                    logger,
                    self._cfg_key_used
                )
            t_parse = time.monotonic() - t0_parse
            self._cfg = cfg or {}
            self._model = model
            self._cache_loaded_ms = now

            if self._cfg:
                kind = str(self._cfg.get("kind", "")).strip().lower()
                if kind and kind not in self._cfgs:
                    self._cfgs[kind] = self._cfg.copy()
                    self._models[kind] = self._model
                    self._cfg_sources[kind] = self._cfg_source
                    self._cfg_keys_used[kind] = self._cfg_key_used

            self._refresh_selective_knobs_from_cfg()

            if self._calibrate_enabled and has_major_changes:
                 # P0-FIX: Only reload calibrator when config actually changed.
                 # Skipping disk I/O (67-745ms) when has_major_changes=False.
                 # Calibrator remains valid as long as model/cfg are unchanged.
                 t0_calib = time.monotonic()
                 await loop.run_in_executor(None, self._load_calibrator_sync, logger)
                 t_calib = time.monotonic() - t0_calib

            t_total = time.monotonic() - t_start
            if t_total > 0.1: # Log only if not ultra-fast
                logger.info(
                    f"ML gate: Async refresh finished in {t_total*1000:.1f}ms "
                    f"(Redis={t_redis*1000:.1f}ms, MainParse={t_parse*1000:.1f}ms, "
                    f"PerKind={t_per_kind*1000:.1f}ms, Calib={t_calib*1000:.1f}ms)"
                )

        except TimeoutError:
            logger.warning("ML gate: Async refresh timed out (10s)")
        except Exception as e:
            import redis.exceptions as redis_exceptions
            is_timeout = isinstance(e, (redis_exceptions.ConnectionError, redis_exceptions.TimeoutError)) or "TimeoutError" in type(e).__name__
            if is_timeout:
                logger.warning(f"ML gate: Async refresh failed (timeout): {e}")
            else:
                logger.error(f"ML gate: Async refresh failed: {e}")

    async def _load_per_kind_configs_async(self, redis_async: Any, logger: Any) -> None:
        """
        Async version of _load_per_kind_configs with parallel fetching.
        """
        import asyncio
        loop = asyncio.get_running_loop()

        async def _load_one(kind: str):
            per_kind_key = f"{self.champion_key}:{kind}"
            try:
                raw_p = await redis_async.get(per_kind_key)
                if not raw_p:
                    return

                cfg, model = await loop.run_in_executor(
                    None,
                    self._parse_and_load_from_payload,
                    raw_p, id(redis_async), logger, per_kind_key
                )
                if not cfg:
                    return

                cfg_kind = (cfg.get("kind", "")).strip().lower()
                if cfg_kind and cfg_kind != kind:
                    logger.warning(
                        f"ML gate: async per-kind key {per_kind_key} has kind={cfg_kind}, "
                        f"expected {kind}. Using cfg_kind={cfg_kind}."
                    )

                effective_kind = cfg_kind or kind
                self._cfgs[effective_kind] = cfg.copy()
                self._models[effective_kind] = model
                self._cfg_sources[effective_kind] = "champion_per_kind"
                self._cfg_keys_used[effective_kind] = per_kind_key

                # Parse per-kind mode_overrides
                mo = cfg.get("mode_overrides")
                if isinstance(mo, dict):
                    by_sym = mo.get("by_symbol")
                    if isinstance(by_sym, dict):
                        _allowed = {"OFF", "SHADOW", "CANARY", "ENFORCE"}
                        parsed = {}
                        for sym, m in by_sym.items():
                            m_up = str(m).strip().upper()
                            if m_up in _allowed:
                                parsed[str(sym).strip().upper()] = m_up
                        if parsed:
                            self._mode_by_symbol_by_kind[effective_kind] = parsed

                    es_sym = mo.get("enforce_share_by_symbol")
                    if isinstance(es_sym, dict):
                        parsed_es = {}
                        for sym, share in es_sym.items():
                            try:
                                sv = float(share)
                                if 0.0 <= sv <= 1.0:
                                    parsed_es[str(sym).strip().upper()] = sv
                            except (TypeError, ValueError):
                                pass
                        if parsed_es:
                            self._enforce_share_by_sym_by_kind[effective_kind] = parsed_es

                logger.debug(f"ML gate: loaded async per-kind config for {effective_kind} from {per_kind_key}")
            except Exception as e:
                logger.warning(f"ML gate: async failed to load per-kind config for {kind}: {e}")

        if self._champion_kinds:
            t0 = time.monotonic()
            tasks = [_load_one(k) for k in self._champion_kinds]
            await asyncio.gather(*tasks, return_exceptions=True)
            return time.monotonic() - t0
        return 0.0

    def _refresh_selective_knobs_from_cfg(self) -> None:
        try:
            if self._cfg.get("abstain_band") is not None:
                self._abstain_band = float(self._cfg.get("abstain_band"))
                p_min = float(self._cfg.get("p_min", 0.5))
                assert self._abstain_band < (p_min - 0.5), f"abstain_band {self._abstain_band} too wide for p_min {p_min}"
        except AssertionError as ae:
            import logging
            logging.getLogger("ml_confirm_gate").error(str(ae))
            self._abstain_band = 0.0 # disable abstain band to prevent p_min bypass
        except Exception:
            pass
        try:
            if self._cfg.get("conf_min") is not None:
                self._conf_min = float(self._cfg.get("conf_min"))
        except Exception:
            pass
        try:
            if self._cfg.get("abstain_on_missing") is not None:
                self._abstain_on_missing = int(float(self._cfg.get("abstain_on_missing") or 0)) == 1
        except Exception:
            pass
        try:
            if self._cfg.get("p_min_hard_floor") is not None:
                self._p_min_hard_floor = float(self._cfg.get("p_min_hard_floor"))
        except Exception:
            pass

        # Per-symbol mode overrides from champion JSON
        self._mode_by_symbol = {}
        self._enforce_share_by_symbol = {}
        overrides = self._cfg.get("mode_overrides")
        if isinstance(overrides, dict):
            by_sym = overrides.get("by_symbol")
            if isinstance(by_sym, dict):
                _allowed = {"OFF", "SHADOW", "CANARY", "ENFORCE"}
                for sym, m in by_sym.items():
                    m_up = str(m).strip().upper()
                    if m_up in _allowed:
                        self._mode_by_symbol[str(sym).strip().upper()] = m_up
            es_sym = overrides.get("enforce_share_by_symbol")
            if isinstance(es_sym, dict):
                for sym, share in es_sym.items():
                    try:
                        sv = float(share)
                        if 0.0 <= sv <= 1.0:
                            self._enforce_share_by_symbol[str(sym).strip().upper()] = sv
                    except (TypeError, ValueError):
                        pass

    def _load_calibrator_sync(self, logger: Any) -> None:
        # Re-use logic from _refresh_cache_if_needed for calibrator
        self._calibrator = None
        self._calib_type = "none"

        # Priority 1: cfg.calibrator
        cal = self._cfg.get("calibrator", None)
        if isinstance(cal, dict) and (cal.get("type", "") or "") == "platt_logit":
            try:
                self._calibrator = PlattLogitCalibrator.from_dict(cal)
                self._calib_type = "cfg_calibrator"
                logger.info("ML gate: Calibrator loaded from cfg.calibrator (type=platt_logit)")
            except Exception as e:
                self._calibrator = None
                self._calib_type = "none"
                logger.warning(f"ML gate: Failed to load calibrator from cfg.calibrator: {e}")

        # Priority 2: cfg.calibrator_path
        if self._calibrator is None:
            cal_path = self._cfg.get("calibrator_path", None)
            if cal_path and isinstance(cal_path, str) and cal_path.strip():
                try:
                    if os.path.exists(cal_path):
                        try:
                            mtime = os.path.getmtime(cal_path)
                            size = os.path.getsize(cal_path)
                            stats = (mtime, size)
                            if cal_path in _SHARED_CALIBRATORS and _SHARED_CALIBRATOR_STATS.get(cal_path) == stats:
                                cal_obj = _SHARED_CALIBRATORS[cal_path]
                                if isinstance(cal_obj, dict) and (cal_obj.get("type", "") or "") == "platt_logit":
                                    self._calibrator = PlattLogitCalibrator.from_dict(cal_obj)
                                    self._calib_type = "cfg_calibrator_path"
                                    logger.debug(f"ML gate: Calibrator loaded from process-level cache for {cal_path}")
                            else:
                                if cal_path.endswith(".json"):
                                    with open(cal_path, encoding="utf-8") as f:
                                        cal_obj = json.load(f)
                                elif cal_path.endswith(".joblib") and joblib is not None:
                                    cal_obj = joblib.load(cal_path)
                                else:
                                    cal_obj = None

                                if cal_obj is not None:
                                    _SHARED_CALIBRATORS[cal_path] = cal_obj
                                    _SHARED_CALIBRATOR_STATS[cal_path] = stats
                                    if isinstance(cal_obj, dict) and (cal_obj.get("type", "") or "") == "platt_logit":
                                        self._calibrator = PlattLogitCalibrator.from_dict(cal_obj)
                                        self._calib_type = "cfg_calibrator_path"
                                        logger.info(f"ML gate: Calibrator loaded from disk for {cal_path}")
                        except Exception as file_e:
                            logger.warning(f"ML gate: Disk cache fault for calibrator {cal_path}: {file_e}")
                except Exception as e:
                    logger.warning(f"ML gate: Failed to load calibrator from cfg.calibrator_path={cal_path}: {e}")

        # Priority 3: model pack
        if self._calibrator is None and self._model is not None:
            try:
                if isinstance(self._model, dict):
                    cal_dict = self._model.get("calibrator", None)
                    if isinstance(cal_dict, dict) and (cal_dict.get("type", "") or "") == "platt_logit":
                        self._calibrator = PlattLogitCalibrator.from_dict(cal_dict)
                        self._calib_type = "model_pack_calibrator"
                        logger.info("ML gate: Calibrator loaded from model pack")
                elif hasattr(self._model, "calibrator"):
                     cal_obj = getattr(self._model, "calibrator", None)
                     if isinstance(cal_obj, dict) and (cal_obj.get("type", "") or "") == "platt_logit":
                        self._calibrator = PlattLogitCalibrator.from_dict(cal_obj)
                        self._calib_type = "model_pack_calibrator"
                        logger.info("ML gate: Calibrator loaded from model.calibrator attribute")
            except Exception as e:
                logger.warning(f"ML gate: Failed to load calibrator from model: {e}")



        if self._calibrator is None:
             logger.debug("ML gate: No calibrator loaded")

    def _refresh_cache_if_needed(self) -> None:
        import logging
        logger = logging.getLogger("ml_confirm_gate")

        if self.mode == "OFF":
            self._cfg, self._model = {}, None
            return

        now = _now_ms()
        if self._cache_loaded_ms and (now - self._cache_loaded_ms) < self._cache_ttl_ms:
            return

        if not self._cache_loaded_ms and self._cfg and self._model:
            self._cache_loaded_ms = now
            return

        cfg, model = self._load_cfg_and_model()
        if not cfg and self._model_load_error in ("no_cfg", "") and self._cfg:
            # Transient Redis failure: preserve existing config, do NOT advance cache timestamp
            # so the next call retries instead of serving ERR_NO_CFG for the whole TTL window.
            logger.warning(
                f"ML gate: Redis returned no cfg (error={self._model_load_error}), "
                f"keeping existing config from cfg_source={getattr(self, '_cfg_source', 'none')}"
            )
            return
        self._cfg = cfg or {}
        self._model = model
        self._cache_loaded_ms = now

        if model is None and self._model_load_error:
            logger.warning(
                f"ML gate: Model not loaded (mode={self.mode}, cfg_source={getattr(self, '_cfg_source', 'none')}, "
                f"error={self._model_load_error})"
            )

        self._refresh_selective_knobs_from_cfg()
        self._load_calibrator_sync(logger)

    def _ensure_exec_risk_norm(self, indicators: dict[str, Any]) -> None:
        # If exec_risk_norm missing, derive it to avoid fail-closed noise.
        if "exec_risk_norm" in indicators:
            return
        spread = _f(indicators.get("spread_bps"), 0.0)
        slip = _f(indicators.get("expected_slippage_bps"), 0.0)
        exec_bps = max(0.0, spread + slip)
        ref = _f(indicators.get("exec_risk_ref_bps", os.getenv("EXEC_RISK_REF_BPS", "10")), 10.0)
        if ref <= 1e-9:
            ref = 10.0
        indicators["exec_risk_bps"] = exec_bps
        indicators["exec_risk_norm"] = max(0.0, min(1.0, exec_bps / ref))

    def _build_feature_row(
        self,
        *,
        model: Any,
        indicators: dict[str, Any],
        direction: str,
        scenario: str,
        ts_ms: int,
    ) -> tuple[list[float], list[str]]:
        feature_cols: list[str] = list(getattr(model, "feature_cols", []) or [])
        missing: list[str] = []

        # critical inputs (accuracy/safety)
        critical = ["spread_bps", "expected_slippage_bps"]
        for k in critical:
            if k not in indicators:
                missing.append(k)

        # derive exec_risk_norm if possible
        self._ensure_exec_risk_norm(indicators)
        if "exec_risk_norm" not in indicators:
            missing.append("exec_risk_norm")

        d = (direction or "").upper()
        s = _scenario_norm(scenario)

        # optional feature engineering (backward compatible)
        transforms = getattr(model, "feature_transforms", None)
        if not isinstance(transforms, dict):
            transforms = {}

        rs = getattr(model, "robust_scaler", None)
        if isinstance(rs, RobustScalerPack):
            scaler = rs
        elif isinstance(rs, dict):
            scaler = RobustScalerPack(params=rs)
        else:
            scaler = None

        # regime/session/buckets (only used when feature_cols contain соответствующие префиксы)
        session_cfg = getattr(model, "session_cfg", None)
        if not isinstance(session_cfg, dict):
            session_cfg = {}
        session_label = derive_session_label(int(ts_ms or 0), cfg=session_cfg)

        spread_edges = getattr(model, "spread_bucket_edges", None)
        if not isinstance(spread_edges, (list, tuple)) or len(spread_edges) == 0:
            spread_edges = [2.0, 5.0, 10.0, 20.0]
        spread_bps_raw = _f(indicators.get("spread_bps", 0.0), 0.0)
        spread_bucket_idx = bucketize(float(spread_bps_raw), [float(x) for x in spread_edges])

        def _spread_bucket_label(x: float) -> str:
            try:
                edges = [float(e) for e in spread_edges]
            except Exception:
                edges = [2.0, 5.0, 10.0, 20.0]
            if not edges:
                return "b0"
            if x <= edges[0]:
                return f"le{int(edges[0])}"
            for a, b in zip(edges[:-1], edges[1:]):
                if a < x <= b:
                    return f"{int(a)}_{int(b)}"
            return f"gt{int(edges[-1])}"

        spread_bucket_label = _spread_bucket_label(float(spread_bps_raw))

        liq_cfg = getattr(model, "liq_cfg", None)
        if not isinstance(liq_cfg, dict):
            liq_cfg = {}
        liq_label = derive_regime_label(indicators.get("liq_regime"), fallback_score=_f(indicators.get("liq_score"), None), cfg=liq_cfg)
        vol_label = derive_regime_label(indicators.get("vol_regime"), fallback_score=_f(indicators.get("vol_score"), None), cfg=liq_cfg)

        # UTC hour/day-of-week and scenario bucket (Commit 8)
        tm = time.gmtime(float(int(ts_ms or 0)) / 1000.0)
        utc_hour = int(getattr(tm, "tm_hour", 0))
        utc_dow = int(getattr(tm, "tm_wday", 0))
        bucket = _bucket_from_scenario(s) or "other"

        # B1: bucket2 (breakout/reversal/high_var) — additive categorization.
        # Producer (tick_processor) should set indicators['bucket2'].
        # Fail-open fallback: conservative derivation from scenario/id + indicators.
        try:
            bucket2 = (indicators.get("bucket2") or "").strip().lower()
        except Exception:
            bucket2 = ""
        if not bucket2:
            try:
                bucket2 = str(derive_bucket2_label(s, indicators=indicators) or "").strip().lower()
            except Exception:
                bucket2 = ""

        cache: dict[str, float] = {}

        def num(name: str) -> float:
            if name in cache:
                return cache[name]
            x = _f(indicators.get(name, 0.0), 0.0)
            # non-linear transforms (log/clip/winsor) if configured in model
            if transforms and name in transforms:
                x = apply_transform(float(x), transforms.get(name))
            # robust scaling if exported with model
            if scaler is not None:
                x = scaler.scale(name, float(x))
            cache[name] = float(x)
            return cache[name]

        row: list[float] = []
        for col in feature_cols:
            if col.startswith("f_"):
                key = col[2:]
                row.append(num(key))
            elif col.startswith("mul_"):
                # interaction term: mul_a__b -> a*b (after per-feature transform/scale)
                pair = col[4:]
                if "__" in pair:
                    a, b = pair.split("__", 1)
                    row.append(num(a) * num(b))
                else:
                    row.append(0.0)
            elif col.startswith("direction_"):
                val = col[len("direction_"):].upper()
                row.append(1.0 if val == d else 0.0)
            elif col.startswith("scenario_v4_"):
                if bool(getattr(self, "_forbid_scenario_v4_onehot", False)):
                    # Strict mode: scenario_v4_* must not appear in feature_cols at serve time.
                    # Mark as missing so the caller can detect and reject; fill 0.0 to keep shape.
                    missing.append("__forbidden_feature_cols")
                    row.append(0.0)
                    continue
                val = col[len("scenario_v4_"):].lower()
                row.append(1.0 if val == s else 0.0)
            elif col.startswith("bucket:"):
                val = col[len("bucket:"):].lower()
                row.append(1.0 if val == str(bucket).lower() else 0.0)
            elif col.startswith("bucket2:"):
                val = col[len("bucket2:"):].lower()
                row.append(1.0 if bucket2 and val == str(bucket2).lower() else 0.0)
            elif col.startswith("hour:"):
                try:
                    hh = int(col[len("hour:"):])
                except Exception:
                    hh = -1
                row.append(1.0 if hh == int(utc_hour) else 0.0)
            elif col.startswith("dow:"):
                try:
                    dd = int(col[len("dow:"):])
                except Exception:
                    dd = -1
                row.append(1.0 if dd == int(utc_dow) else 0.0)
            elif col.startswith("session_"):
                # NOTE: session one-hots are derived from ts_ms, not from indicators,
                # to keep train==serve deterministic even if upstream did not export them.
                val = col[len("session_"):].lower()
                row.append(1.0 if val == str(session_label) else 0.0)
            elif col.startswith("spread_bucket_"):
                val = col[len("spread_bucket_"):].lower()
                ok = (val == str(spread_bucket_idx)) or (val == f"b{spread_bucket_idx}") or (val == spread_bucket_label)
                row.append(1.0 if ok else 0.0)
            elif col.startswith("liq_regime_"):
                val = col[len("liq_regime_"):].lower()
                row.append(1.0 if val == str(liq_label) else 0.0)
            elif col.startswith("vol_regime_"):
                val = col[len("vol_regime_"):].lower()
                row.append(1.0 if val == str(vol_label) else 0.0)
            else:
                row.append(0.0)

        return row, missing

    @staticmethod
    def _conf_from_margin(p_margin: float) -> float:
        # stable 0..1 proxy, cheap (no dependence on p calibration)
        # grows with |margin|
        try:
            return float(1.0 - math.exp(-abs(float(p_margin))))
        except Exception:
            return 0.0

    def _apply_selective(self, dec: MLConfirmDecision, *, ok_rule: int) -> None:
        """Softening ENFORCE decisions near threshold / low confidence."""
        if self.mode != "ENFORCE" or int(ok_rule) != 1:
            if self.mode == "SHADOW":
                dec.status = dec.status or "SHADOW"
            return
        if dec.error:
            dec.status = dec.status or "ERR"
            return
        if dec.missing:
            # missing handled earlier
            return
        band = float(self._abstain_band or 0.0)
        p_min = float(self._cfg.get("p_min", 0.5)) if self._cfg else 0.5
        assert band < (p_min - 0.5), f"abstain_band {band} too wide for p_min {p_min}"
        if band > 0.0 and abs(float(dec.p_margin)) <= band:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_BAND"
            dec.reason = f"ml_abstain_band(margin={dec.p_margin:.6f},band={band:.6f})"
            return
        cmin = float(self._conf_min or 0.0)
        if cmin > 0.0 and float(dec.conf) < cmin:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_LOWCONF"
            dec.reason = f"ml_abstain_lowconf(conf={dec.conf:.6f},min={cmin:.6f})"

    def _cache_ml_decision(
        self,
        dec: MLConfirmDecision,
        *,
        sid: str,
        symbol: str,
        scenario: str,
        ok_rule: int,
    ) -> None:
        """
        Cache ML decision for outcome emitter join.
        
        Called after _emit_metrics to write ml:dec:{sid} cache.
        """
        if not self.r or not sid:
            return

        # Extract bucket from decision or scenario
        bucket = dec.bucket or _bucket_from_scenario(scenario) or "other"

        # Determine enforce: 1 if ENFORCE mode and decision was allowed, else 0
        enforce = 1 if (self.mode == "ENFORCE" and dec.allow) else 0

        # Determine missing: 1 if critical features were missing, else 0
        missing = 1 if (dec.missing and len(dec.missing) > 0) else 0

        # Extract model version
        model_ver = dec.model_run_id or getattr(self, "_model_run_id", "") or ""
        if not model_ver and self._cfg:
            model_ver = str(self._cfg.get("model_ver", "") or "")

        # Cache decision
        cache_ml_decision(
            self.r,
            sid=sid,
            symbol=symbol,
            bucket=bucket,
            p_edge=float(dec.p_edge or 0.0),
            enforce=enforce,
            ok_rule=ok_rule,
            missing=missing,
            model_ver=model_ver,
        )

    def _emit_metrics(self, dec: MLConfirmDecision, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                     rule_score: float, rule_have: int, rule_need: int, cancel_spike_veto: int, ok_rule: int,
                     sid: str | None = None, indicators: dict[str, Any] | None = None) -> None:
        if not self._metrics_enable:
            return
        redis = self.r
        if redis is None:
            return
        try:
            # Compute canonical sid for cross-stream joins
            raw_sid = (sid or "") if sid else str(indicators.get("sid") or indicators.get("signal_id") or "") if indicators else ""
            sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
            # Deterministic sampling by sid (stable across restarts)
            sample_rate = float(self._metrics_sample)
            if sample_rate < 1.0 and sample_rate > 0.0:
                if not _stable_sample(sid, sample_rate, salt="metrics:ml_confirm"):
                    return

            # Extract bucket and exec_risk_norm from indicators or decision
            bucket = dec.bucket or _bucket_from_scenario(scenario)
            exec_risk_norm = 0.0
            exec_risk_bps = 0.0

            # Extract detailed score breakdown if available
            sb = {}
            if indicators:
                exec_risk_norm = float(indicators.get("exec_risk_norm", 0.0) or 0.0)
                exec_risk_bps = float(indicators.get("exec_risk_bps", 0.0) or 0.0)
                sb = indicators.get("score_breakdown") or {}

            payload: dict[str, Any] = {
                "ts_ms": ts_ms,
                "sid": sid,
                "symbol": symbol,
                "mode": self.mode,
                "effective_mode": str(dec.effective_mode or dec.mode or self.mode),
                "mode_source": str(dec.mode_source or "global"),
                "ab_variant": str(self.ab_variant or ""),
                "kind": dec.kind or "",
                "model_run_id": str(dec.model_run_id or ""),
                "bucket": bucket,
                "cfg_source": getattr(self, "_cfg_source", "none"),
                "direction": str(direction),
                "scenario_v4": str(scenario),
                "rule_score": f"{float(rule_score):.6f}",

                # Extended score breakdown metrics (Step 1)
                "rule_base_score": f"{float(sb.get('base_score', rule_score)):.6f}",
                "rule_score_raw": f"{float(sb.get('final_score_raw', rule_score)):.6f}",
                "rule_exec_pen": f"{float(sb.get('exec_pen', 0.0)):.6f}",
                "score_raw_sum": f"{float(sb.get('raw_sum', 0.0)):.6f}",
                "score_w_sum": f"{float(sb.get('w_sum', 0.0)):.6f}",
                "score_agg": (sb.get('agg', 'unknown')),

                "rule_have": str(int(rule_have)),
                "rule_need": str(int(rule_need)),
                "have_need_ratio": f"{(float(rule_have) / max(1.0, float(rule_need))):.3f}",
                "ok_rule": str(int(ok_rule)),
                "cancel_spike_veto": str(int(cancel_spike_veto)),
                "p_edge": float(dec.p_edge or 0.0),
                "p_edge_cal": float(dec.p_edge_cal or 0.0),
                "p_edge_raw": float(dec.p_edge_raw or 0.0),
                "lat_ms": f"{float(dec.latency_us or 0) / 1000.0:.3f}",
                "latency_us": str(int(dec.latency_us or 0)),

                # P74+ Executive Summary
                "exec_risk_ref_bps": f"{float(dec.exec_risk_ref_bps):.2f}",
                "exec_risk_bps": f"{float(dec.exec_risk_bps):.2f}",
                "exec_risk_norm": f"{float(dec.exec_risk_norm):.4f}",
                "exec_pen": f"{float(dec.exec_pen):.4f}",
                "score_breakdown_small": json.dumps(dec.score_breakdown_small or {}, separators=(",", ":")),
                "latency_ms": f"{float(dec.latency_us or 0) / 1000.0:.3f}",
                "status": str(dec.status or ""),
                "allow": int(bool(dec.allow)),
                "err": str(dec.error or ""),
                "abstain": int(bool(dec.abstain)),
                "conf": f"{float(dec.conf or 0.0):.6f}",
                "missing_n": str(len(dec.missing or [])),
            }
            # Attach rule score breakdown (if present) for drift/debug
            if indicators and isinstance(indicators.get("score_breakdown"), dict):
                sb = indicators.get("score_breakdown") or {}
                try:
                    payload["rule_base_score"] = float(sb.get("base_score", 0.0) or 0.0)
                    payload["rule_exec_pen"] = float(sb.get("exec_pen", 0.0) or 0.0)
                    payload["rule_score_raw"] = float(sb.get("final_score_raw", sb.get("final_score", 0.0)) or 0.0)
                    payload["rule_score_01"] = float(sb.get("final_score_01", sb.get("final_score", 0.0)) or 0.0)
                    payload["score_raw_sum"] = float(sb.get("raw_sum", 0.0) or 0.0)
                    payload["score_w_sum"] = float(sb.get("w_sum", 0.0) or 0.0)
                    payload["score_agg"] = (sb.get("agg", "") or "")
                except Exception:
                    pass
            # Also attach exec risk reference if present
            if indicators and "exec_risk_ref_bps" in indicators:
                with contextlib.suppress(Exception):
                    payload["exec_risk_ref_bps"] = float(indicators.get("exec_risk_ref_bps") or 0.0)

            # Full score breakdown as JSON (P0 requirement)
            if indicators:
                sb = indicators.get("score_breakdown")
                if sb and isinstance(sb, dict):
                    with contextlib.suppress(Exception):
                        payload["score_breakdown_json"] = json.dumps(sb, separators=(",", ":"))

                # Ensure exec_pen is available at top level if needed (aliasing rule_exec_pen)
                # rule_exec_pen is already in payload, but we add exec_pen explicitly if requested
                if sb and "exec_pen" in sb:
                    payload["exec_pen"] = float(sb.get("exec_pen", 0.0) or 0.0)
            # Add exec_risk fields if present (useful for drift analysis)
            if exec_risk_norm > 0.0 or exec_risk_bps > 0.0:
                payload["exec_risk_norm"] = float(exec_risk_norm)
                payload["exec_risk_bps"] = float(exec_risk_bps)

            # Add low-cardinality context fields (useful for slicing metrics)
            if indicators:
                for k in ["spread_bucket", "session", "liq_regime", "vol_regime", "regime_bucket", "regime_group"]:
                    v = indicators.get(k)
                    if v is not None and v != "":
                        payload[k] = str(v)
                for k in ["data_health", "book_health_ok", "tick_time_age_abs_ema_ms", "tick_event_stream_skew_abs_ema_ms"]:
                    v = indicators.get(k)
                    if v is not None:
                        try:
                            payload[k] = f"{float(v):.6f}"
                        except Exception:
                            payload[k] = str(v)
                # exec_risk reference if exported by rule-engine
            # Add exec_risk reference if exported by rule-engine
            if indicators:
                if "exec_risk_ref_bps" in indicators:
                    with contextlib.suppress(Exception):
                        payload["exec_risk_ref_bps"] = float(indicators.get("exec_risk_ref_bps") or 0.0)

            # Rule-gate score breakdown (if provided by OFConfirmEngine enrichment)
            if indicators:
                sb = indicators.get('score_breakdown_small') or indicators.get('score_breakdown')
                if isinstance(sb, dict):
                    try:
                        payload['rule_base_score'] = f"{float(sb.get('base_score', 0.0) or 0.0):.6f}"
                        payload['rule_exec_pen'] = f"{float(sb.get('exec_pen', 0.0) or 0.0):.6f}"
                        payload['rule_score_raw'] = f"{float(sb.get('final_score_raw', sb.get('final_score', 0.0)) or 0.0):.6f}"
                        payload['rule_score_01'] = f"{float(sb.get('final_score_01', payload.get('rule_score', 0.0)) or 0.0):.6f}"
                        payload['score_raw_sum'] = f"{float(sb.get('raw_sum', 0.0) or 0.0):.6f}"
                        payload['score_w_sum'] = f"{float(sb.get('w_sum', 0.0) or 0.0):.6f}"
                        payload['score_agg'] = (sb.get('agg', '') or '')
                    except Exception:
                        pass

            # Add util_* fields if available
            if indicators:
                for h in ["util_h1", "util_h4", "util_h24"]:
                    u = indicators.get(h)
                    if u is not None:
                        # store as string with correct key
                        payload[h] = f"{float(u):.6f}"

            # ------------------------------------------------------------------
            # Phase 2.3D: schema observability fields.
            # schema_name: which ML feature schema the model was served with.
            # vol_ratio_present: was vol_ratio propagated from the ATR profile?
            # hz_gate_mode: current horizon DQ gate mode (shadow/canary/enforce).
            # ------------------------------------------------------------------
            try:
                model_obj = getattr(self, "_model", None)
                sch = ""
                if model_obj is not None:
                    sch = str(getattr(model_obj, "schema_name", "") or "")
                    if not sch and hasattr(model_obj, "feature_cols"):
                        # derive from feature overlap when schema_name attr is absent
                        fc = list(getattr(model_obj, "feature_cols", []) or [])
                        if any("vol_ratio" in c for c in fc):
                            sch = "v5_of"
                        elif len(fc) > 70:
                            sch = "v4_of"
                payload["schema_name"] = sch
                if indicators:
                    payload["vol_ratio_present"] = str(int("vol_ratio" in indicators or "vol_ratio_fast_slow" in indicators))
                    payload["vol_ratio_z_present"] = str(int("vol_ratio_z" in indicators))
                    payload[HzGateKeys.MODE] = (indicators.get(HzGateKeys.MODE, ""))
                    # vol_ratio value for drift tracking (bounded)
                    vr = indicators.get("vol_ratio") or indicators.get("vol_ratio_fast_slow")
                    if vr is not None:
                        with contextlib.suppress(Exception):
                            payload["vol_ratio"] = f"{float(vr):.4f}"
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Phase 7: Horizon-aware SHADOW observability (ml:metrics stream).
            # hz_features_present: count of non-zero v5 horizon features at inference.
            # hz_{key}: per-field values for Grafana group-by / delta tracking.
            # ------------------------------------------------------------------
            try:
                if indicators:
                    _HZ_V5_KEYS = (
                        "atr_tf_ms", "atr_stop_pct", "atr_regime_pct",
                        "hold_target_ms_norm", "alpha_half_life_ms_norm",
                        "vol_ratio_fast_slow", "max_signal_age_ratio",
                    )
                    hz_present = sum(
                        1 for k in _HZ_V5_KEYS
                        if indicators.get(k) is not None and float(indicators.get(k) or 0.0) != 0.0
                    )
                    payload["hz_features_present"] = str(hz_present)
                    payload["hz_features_total"] = str(len(_HZ_V5_KEYS))
                    for _hk in _HZ_V5_KEYS:
                        _hv = indicators.get(_hk)
                        if _hv is not None:
                            with contextlib.suppress(Exception):
                                payload[f"hz_{_hk}"] = f"{float(_hv):.6f}"
            except Exception:
                pass

            self.r.xadd(self._metrics_stream, payload, maxlen=50000, approximate=True)
        except Exception as e:
            # Increment error metric and rate-limited log
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_errors_total.labels(kind=dec.kind or "unknown", reason="emit_metrics").inc()
            # Rate-limited logging (at most once per 30 seconds)
            if not hasattr(self, '_last_emit_metrics_error_log_ts'):
                self._last_emit_metrics_error_log_ts = 0
            now_ms = _now_ms()
            if now_ms - self._last_emit_metrics_error_log_ts > 30000:
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: _emit_metrics error: {type(e).__name__}: {str(e)[:200]}")
                self._last_emit_metrics_error_log_ts = now_ms

    def _capture_replay_input(self, dec: MLConfirmDecision, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                              indicators: dict[str, Any], rule_score: float, rule_have: int, rule_need: int,
                              cancel_spike_veto: int, ok_rule: int) -> None:
        if not self._replay_capture:
            return
        try:
            # Compute canonical sid for cross-stream joins
            raw_sid = str(indicators.get("sid") or indicators.get("signal_id") or "")
            sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
            # Deterministic sampling by sid (stable across restarts)
            do_emit = True
            if float(self._replay_sample) < 0.999:
                do_emit = _stable_sample(sid, float(self._replay_sample), salt=f"ml_replay_inputs_v1|{self._replay_stream}")
            if not do_emit:
                return
            cfg = self._cfg or {}
            cfg_small = {
                "kind": cfg.get("kind", ""),
                "run_id": cfg.get("run_id", ""),
                "model_path": cfg.get("model_path", ""),
                "util_floors": cfg.get("util_floors", {}),
                "abstain_band": cfg.get("abstain_band", None),
                "conf_min": cfg.get("conf_min", None),
                "abstain_on_missing": cfg.get("abstain_on_missing", None),
                "p_min_hard_floor": cfg.get("p_min_hard_floor", None),
            }
            payload = {
                "ts_ms": int(ts_ms),
                "symbol": symbol.upper(),
                "direction": str(direction),
                "scenario_v4": str(scenario),
                "sid": str(sid),  # Added for deterministic replay
                "indicators": _json_safe(indicators),
                "rule_score": float(rule_score),
                "rule_have": int(rule_have),
                "rule_need": int(rule_need),
                "cancel_spike_veto": int(cancel_spike_veto),
                "ok_rule": int(ok_rule),
                "cfg": _json_safe(cfg_small),
            }
            # Add a compact decision summary for offline audits (does not affect feature vector).
            payload["dec_summary"] = _json_safe({
                "kind": str(dec.kind or ""),
                "mode": str(dec.mode or ""),
                "allow": int(bool(dec.allow)),
                "abstain": int(bool(dec.abstain)),
                "status": str(dec.status or ""),
                "reason": str(dec.reason or ""),
                "bucket": str(dec.bucket or ""),
                "p_edge": float(dec.p_edge or 0.0),
                "p_min": float(dec.p_min or 0.0),
                "p_margin": float(dec.p_margin or 0.0),
                "conf": float(dec.conf or 0.0),
                "missing_n": int(len(dec.missing or [])),
            })
            self.r.xadd(self._replay_stream, {
                "ts_ms": str(int(ts_ms)),
                "symbol": symbol.upper(),
                "scenario_v4": str(scenario),
                "sid": str(sid),  # Added for deterministic replay
                "model_run_id": str(dec.model_run_id or ""),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }, maxlen=50000, approximate=True)
        except Exception as e:
            # Increment error metric and rate-limited log
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_errors_total.labels(kind=dec.kind or "unknown", reason="replay_capture").inc()
            # Rate-limited logging (at most once per 30 seconds)
            if not hasattr(self, '_last_replay_capture_error_log_ts'):
                self._last_replay_capture_error_log_ts = 0
            now_ms = _now_ms()
            if now_ms - self._last_replay_capture_error_log_ts > 30000:
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: _capture_replay_input error: {type(e).__name__}: {str(e)[:200]}")
                self._last_replay_capture_error_log_ts = now_ms

    def _decide_ml_scorer(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
        cfg: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> MLConfirmDecision:
        """Decision logic for simple GBDT/LGBM scorers (Scorer V3/V4)."""
        cfg = cfg if cfg is not None else self._cfg
        model = model if model is not None else self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind=(cfg.get("kind", "ml_scorer")), allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.reason = self._model_load_error or "no_model_loaded"
            dec.error = dec.reason
            dec.status = "ERR_NO_MODEL"
            return dec

        # Build features via a dict-pack view so optional transforms/scaler/buckets apply.
        view = _DictPackModelView(model) if isinstance(model, dict) else model
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms,
        )
        dec.missing = missing

        if missing and mode == "ENFORCE" and not self._abstain_on_missing:
            dec.allow = False
            dec.status = "MISSING_CRITICAL_BLOCK"
            dec.reason = f"missing_critical({','.join(missing)})"
            return dec

        import numpy as np
        X = np.array([x_row], dtype=np.float32)

        try:
            m = model.get("model") if isinstance(model, dict) else model
            if hasattr(m, "predict"):
                p_raw = float(m.predict(X)[0])
            elif hasattr(m, "predict_proba"):
                p_raw = float(m.predict_proba(X)[0, 1])
            else:
                raise ValueError("Model has no predict/predict_proba method")
        except Exception as e:
            dec.mode = "ERR"
            dec.error = str(e)
            dec.status = "ERR_PREDICT"
            return dec

        dec.p_edge_raw = float(p_raw)
        dec.p_edge = float(p_raw)
        dec.p_min = float(cfg.get("p_min", 0.5) or 0.5)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.allow = bool(dec.p_edge >= dec.p_min)
        dec.status = "ALLOW" if dec.allow else "DENY"
        dec.reason = "ml_allow" if dec.allow else "ml_deny"
        return dec

    def _decide_util_mh(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
        cfg: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> MLConfirmDecision:
        cfg = cfg if cfg is not None else self._cfg
        model = model if model is not None else self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="util_mh_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            # Use detailed error reason if available, otherwise generic
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            # Explicitly set metrics to 0.0 for clarity and consistency
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []

            # Log the error for diagnostics (but not on every request to avoid spam)
            import logging
            logger = logging.getLogger("ml_confirm_gate")

            # Check if fallback was attempted
            fallback_info = ""
            if cfg.get("model_path_fallback_used"):
                original_path = cfg.get("model_path_original", "unknown")
                fallback_info = f" (fallback from {original_path} attempted but also failed)"

            if hasattr(self, '_last_error_log_ms'):
                now_ms = _now_ms()
                if now_ms - self._last_error_log_ms > 60000:  # Log at most once per minute
                    logger.error(
                        f"ML gate: Model not loaded for decision (symbol={symbol}, "
                        f"error={error_reason}, cfg_source={getattr(self, '_cfg_source', 'none')}, "
                        f"model_path={dec.model_path}{fallback_info})"
                    )
                    self._last_error_log_ms = now_ms
            else:
                logger.error(
                    f"ML gate: Model not loaded for decision (symbol={symbol}, "
                    f"error={error_reason}, cfg_source={getattr(self, '_cfg_source', 'none')}, "
                    f"model_path={dec.model_path}{fallback_info})"
                )
                self._last_error_log_ms = _now_ms()

            return dec

        x_row, missing = self._build_feature_row(model=model, indicators=indicators, direction=direction, scenario=scenario, ts_ms=ts_ms)
        dec.missing = missing

        # ENFORCE: если критические фичи реально отсутствуют -> fail-closed (точнее и безопаснее)
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                # selective: do not hard-block, let rule gate decide
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        import numpy as np
        X = np.array([x_row], dtype=np.float32)

        util_pred = model.predict_util(X)  # dict[int]->ndarray
        unc = model.predict_unc(X)         # dict[int]->ndarray
        horizons: list[int] = list(getattr(model, "horizons", []) or list(util_pred.keys()))

        # Validate model outputs before processing
        if not horizons:
            dec.error = "no_horizons"
            dec.reason = "no_horizons(model_horizons_empty,util_pred_keys_empty)"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.util_pred = {}
            dec.unc = {}
            dec.status = "ERR_NO_HORIZONS"
            return dec

        if not util_pred or not unc:
            dec.error = "empty_predictions"
            dec.reason = f"empty_predictions(util_pred={bool(util_pred)},unc={bool(unc)})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.util_pred = {}
            dec.unc = {}
            dec.status = "ERR_EMPTY_PREDICTIONS"
            return dec

        util_floors = cfg.get("util_floors") if isinstance(cfg.get("util_floors"), dict) else {}
        unc_k = _f(util_floors.get("unc_k", getattr(model, "unc_k", 0.5)), getattr(model, "unc_k", 0.5))

        best_h = 0
        best_score = -1e18
        util_pred_out: dict[str, float] = {}
        unc_out: dict[str, float] = {}
        scores_computed = False

        for h in horizons:
            if h not in util_pred or h not in unc:
                continue
            try:
                u = float(util_pred[h][0])
                un = float(unc[h][0])

                # Validate: check for NaN/Inf values
                if not (math.isfinite(u) and math.isfinite(un)):
                    import logging
                    logger = logging.getLogger("ml_confirm_gate")
                    logger.warning(f"ML gate: Non-finite prediction for horizon {h} (u={u}, unc={un})")
                    continue

                util_pred_out[str(h)] = u
                unc_out[str(h)] = un
                sc = u - unc_k * un

                # Validate computed score
                if not math.isfinite(sc):
                    import logging
                    logger = logging.getLogger("ml_confirm_gate")
                    logger.warning(f"ML gate: Non-finite score for horizon {h} (score={sc})")
                    continue

                if sc > best_score:
                    best_score = sc
                    best_h = int(h)
                    scores_computed = True
            except (IndexError, KeyError, TypeError, ValueError) as e:
                # Skip invalid predictions for this horizon, continue with others
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: Invalid prediction for horizon {h}: {e}")
                continue

        # Check if we actually computed any valid scores
        if not scores_computed or best_score <= -1e17:  # Still at initial value (with small tolerance for float precision)
            dec.error = "no_valid_scores"
            dec.reason = f"no_valid_scores(horizons={len(horizons)},computed={scores_computed},best_score={best_score:.2f})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = float(best_score) if scores_computed else 0.0
            dec.best_h_ms = best_h
            dec.util_pred = util_pred_out
            dec.unc = unc_out
            dec.status = "ERR_NO_VALID_SCORES"
            bucket = _bucket_from_scenario(scenario)
            dec.bucket = bucket
            floor = _get_floor(util_floors, bucket)
            try:
                floor = max(float(floor), float(self._p_min_hard_floor))
            except Exception:
                floor = float(floor)
            dec.floor = float(floor)
            dec.allow = False  # No valid scores -> block
            return dec

        bucket = _bucket_from_scenario(scenario)
        floor = _get_floor(util_floors, bucket)
        # hard floor guardrail
        try:
            floor = max(float(floor), float(self._p_min_hard_floor))
        except Exception:
            floor = float(floor)

        dec.bucket = bucket
        dec.best_h_ms = best_h
        dec.score = float(best_score)
        dec.floor = float(floor)
        dec.util_pred = util_pred_out
        dec.unc = unc_out

        dec.allow = bool(best_score >= floor)

        # p_edge: convert utility score to probability before calibration
        # Utility scores can be negative/zero/positive, but calibrator expects [0,1]
        #
        # Solution: Use adaptive scaling based on the actual range of utility scores.
        # For very negative scores, we need more aggressive scaling to map them to a useful
        # probability range. We use a piecewise scaling approach:
        # - For scores in typical range [-5, 5]: scale by 2.5 (maps to [0.006, 0.994])
        # - For very negative scores (< -5): use more aggressive scaling to prevent all zeros
        # - For very positive scores (> 5): already near 1.0, less scaling needed

        def _sigmoid(x: float) -> float:
            """Stable sigmoid: 1 / (1 + exp(-x))"""
            if x >= 0:
                z = math.exp(-x)
                return 1.0 / (1.0 + z)
            z = math.exp(x)
            return z / (1.0 + z)

        # Adaptive scaling: more aggressive for very negative scores
        base_scale = float(self._cfg.get("p_edge_scale_factor", 2.5) or 2.5)

        if best_score < -5.0:
            # Very negative: use more aggressive scaling to prevent all zeros
            # Scale by 4x for scores < -5 to map them to at least ~0.001 range
            scale_factor = base_scale * 1.6  # 2.5 * 1.6 = 4.0
        elif best_score > 5.0:
            # Very positive: already near 1.0, less scaling needed
            scale_factor = base_scale * 0.8  # 2.5 * 0.8 = 2.0
        else:
            # Typical range [-5, 5]: use base scaling
            scale_factor = base_scale

        scaled_score = float(best_score) * scale_factor
        p_edge_from_score = _sigmoid(scaled_score)

        # Ensure minimum precision: if sigmoid produces a very small value, keep it for accuracy
        # but ensure it's not exactly 0.0 for valid scores (helps with diagnostics)
        if p_edge_from_score == 0.0 and best_score > -1e17:
            # This shouldn't happen with proper scaling, but add safety check
            # For very negative scores, ensure we get at least a tiny non-zero value
            p_edge_from_score = max(1e-6, _sigmoid(scaled_score * 1.1))

        # Store pre-calibration probability (not raw utility score)
        dec.p_edge_raw = float(p_edge_from_score)  # pre-calibration probability
        dec.p_edge_cal = float(p_edge_from_score)  # will be updated by calibrator if enabled
        dec.calib_type = str(self._calib_type or "none")

        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        calibrate = bool(calibrate)

        if calibrate and self._calibrator is not None:
            # Now calibrate the probability (already in [0,1] range)
            dec.p_edge_cal = float(self._calibrator.apply_one(p_edge_from_score))

        # Map floor to probability space identically to p_edge
        scaled_floor = float(floor) * scale_factor
        p_min_from_floor = _sigmoid(scaled_floor)

        p_min_cal = p_min_from_floor
        if calibrate and self._calibrator is not None:
            p_min_cal = float(self._calibrator.apply_one(p_min_from_floor))

        # use calibrated p_edge for downstream thresholds/metrics
        dec.p_edge = float(dec.p_edge_cal)
        dec.p_min = float(p_min_cal)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"util_mh(score={best_score:.4f},floor={floor:.4f},h={best_h},bucket={bucket})"

        return dec

    def _decide_edge_stack_v1(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
        cfg: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> MLConfirmDecision:
        """
        Решение для edge_stack_v1: OOF stacking (LR + GBDT -> meta LR).
        
        Модель: dict-pack с ключами:
          - schema_version: 1
          - kind: "edge_stack_v1"
          - feature_cols: List[str]
          - lr: sklearn Pipeline (scaler + LR)
          - gbdt: CatBoostClassifier или HistGradientBoostingClassifier
          - meta: LogisticRegression
        
        Конфиг поддерживает:
          - p_min: глобальный порог (0..1)
          - p_min_by_bucket: {"trend": 0.55, "range": 0.60, "other": 0.50, "news": 0.65}
          - hard_p_min_floor: минимальный порог (fail-safe guardrail)
        """
        cfg = cfg if cfg is not None else self._cfg
        model = model if model is not None else self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="edge_stack_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        # Проверка структуры модели
        if not isinstance(model, dict):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_format"
            dec.reason = f"bad_model_format(expected_dict,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        if model.get("kind") != "edge_stack_v1":
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_kind"
            dec.reason = f"bad_model_kind(expected=edge_stack_v1,got={model.get('kind')})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        feature_cols = model.get("feature_cols", [])
        if not feature_cols:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "no_feature_cols"
            dec.reason = "no_feature_cols(model_missing_feature_cols)"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Build features via a dict-pack view so optional transforms/scaler/buckets apply.
        view = _DictPackModelView(model)

        # Strict schema guard: fail before inference if model contains forbidden feature_cols.
        # This prevents unbounded cardinality (scenario_v4_*) from silently corrupting predictions.
        if bool(getattr(self, "_forbid_scenario_v4_onehot", False)):
            bad_cols = _find_forbidden_feature_cols(
                feature_cols, forbid_scenario_v4_onehot=True
            )
            if bad_cols:
                dec.mode = "ERR"
                dec.allow = self._fail_allow()
                dec.error = "forbidden_feature_cols"
                dec.reason = (
                    f"forbidden_feature_cols(scenario_v4_onehot,"
                    f"n={len(bad_cols)},ex={bad_cols[0]})"
                )
                dec.status = "ERR_FORBIDDEN_FEATURE_COLS"
                dec.p_edge = 0.0
                dec.p_min = 0.0
                dec.p_margin = 0.0
                dec.conf = 0.0
                dec.missing = ["__forbidden_feature_cols"]
                with contextlib.suppress(Exception):
                    self._metrics_errors_total.labels(
                        kind="edge_stack_v1", reason="forbidden_feature_cols"
                    ).inc()
                return dec
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms,
        )
        dec.missing = missing

        # ENFORCE: если критические фичи отсутствуют -> fail-closed
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        X = np.array([x_row], dtype=np.float32)

        # Получаем base модели
        lr_model = model.get("lr")
        gbdt_model = model.get("gbdt")
        meta_model = model.get("meta")

        if lr_model is None or gbdt_model is None or meta_model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "missing_base_models"
            dec.reason = f"missing_base_models(lr={lr_model is not None},gbdt={gbdt_model is not None},meta={meta_model is not None})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Предсказания base моделей
        try:
            p_lr = lr_model.predict_proba(X)[0, 1]  # вероятность класса 1
            p_gbdt = gbdt_model.predict_proba(X)[0, 1]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "base_prediction_failed"
            dec.reason = f"base_prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Проверка на NaN/Inf
        if not (math.isfinite(p_lr) and math.isfinite(p_gbdt)):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_base_preds"
            dec.reason = f"non_finite_base_preds(lr={p_lr},gbdt={p_gbdt})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Meta предсказание
        meta_degenerate = False
        try:
            # Fallback if meta model is degenerate (zeroed coefficients)
            if hasattr(meta_model, "coef_") and np.all(meta_model.coef_ == 0):
                meta_degenerate = True
                p_edge_raw = p_gbdt
            else:
                Z = np.array([[p_lr, p_gbdt]], dtype=np.float32)
                p_edge_raw = meta_model.predict_proba(Z)[0, 1]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "meta_prediction_failed"
            dec.reason = f"meta_prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        if not math.isfinite(p_edge_raw):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_meta_pred"
            dec.reason = f"non_finite_meta_pred(p={p_edge_raw})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Калибровка (если включена)
        dec.p_edge_raw = float(np.clip(p_edge_raw, 0.0, 1.0))
        dec.p_edge_cal = float(dec.p_edge_raw)
        dec.calib_type = str(self._calib_type or "none")

        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        calibrate = bool(calibrate)

        if calibrate and self._calibrator is not None:
            if meta_degenerate:
                # Bypass calibrator if meta model is degenerate, since calibrator was tuned for meta model
                dec.p_edge_cal = float(dec.p_edge_raw)
                dec.calib_type = "bypassed_degenerate"
            else:
                dec.p_edge_cal = float(self._calibrator.apply_one(dec.p_edge_raw))

        dec.p_edge = float(dec.p_edge_cal)

        # Определение bucket и p_min
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket

        # p_min из конфига: приоритет p_min_by_bucket, затем p_min, затем hard_p_min_floor
        # NOTE: Для edge_stack_v1 используется p_min (только на p_cal).
        # TODO: В будущем можно реализовать edge_floors как score_min (p_cal - unc_k*unc),
        #       чтобы учитывать uncertainty в пороге. Это потребует добавления uncertainty
        #       в модель edge_stack_v1 или использования отдельной uncertainty модели.
        p_min_by_bucket = cfg.get("p_min_by_bucket", {})
        if isinstance(p_min_by_bucket, dict) and bucket in p_min_by_bucket:
            p_min_cfg = float(p_min_by_bucket[bucket])
        else:
            p_min_cfg = float(cfg.get("p_min", 0.55))

        # hard_p_min_floor как guardrail
        hard_p_min_floor = float(cfg.get("hard_p_min_floor", 0.0))
        with contextlib.suppress(Exception):
            hard_p_min_floor = max(float(hard_p_min_floor), float(self._p_min_hard_floor))

        p_min = max(p_min_cfg, hard_p_min_floor)
        p_min = max(0.0, min(1.0, p_min))  # clamp to [0, 1]

        dec.p_min = float(p_min)
        dec.floor = float(p_min)  # для совместимости
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        # Решение
        dec.allow = bool(dec.p_edge >= dec.p_min)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"edge_stack_v1(p_edge={dec.p_edge:.4f},p_min={dec.p_min:.4f},bucket={bucket})"

        return dec

    def _decide_edge_stack_mh(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
        cfg: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> MLConfirmDecision:
        """
        Решение для edge_stack_mh_v1: multi-horizon stacking с uncertainty.
        
        Модель: EdgeStackMHModelV1
          - p_lr[h], p_gbdt[h] -> p_meta[h] -> p_cal[h]
          - unc[h] = |p_lr[h] - p_gbdt[h]|
          - score[h] = p_cal[h] - unc_k * unc[h]
          - best_h = argmax_h(score[h])
          - allow if best_score >= edge_floors[bucket].floor
        """
        cfg = cfg if cfg is not None else self._cfg
        model = model if model is not None else self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="edge_stack_mh_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        # Проверка типа модели
        if not isinstance(model, EdgeStackMHModelV1):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_type"
            dec.reason = f"bad_model_type(expected=EdgeStackMHModelV1,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # P0 fix: для edge_stack_mh_v1 модель - это объект EdgeStackMHModelV1,
        # который уже имеет все нужные атрибуты (feature_cols, feature_transforms, robust_scaler, etc.)
        # поэтому передаём его напрямую (не создаём temp_model)
        x_row, missing = self._build_feature_row(
            model=model,  # НЕ temp_model - используем реальный объект модели
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms,
        )
        dec.missing = missing

        # ENFORCE: если критические фичи отсутствуют -> fail-closed
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        X = np.array([x_row], dtype=np.float32)

        # Предсказания модели
        try:
            p_cal_dict = model.predict_p_cal(X)  # Dict[int, np.ndarray]
            unc_dict = model.predict_unc(X)      # Dict[int, np.ndarray]
            score_dict = model.predict_score(X)  # Dict[int, np.ndarray]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "prediction_failed"
            dec.reason = f"prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_PREDICTION"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        horizons = model.horizons
        if not horizons:
            dec.error = "no_horizons"
            dec.reason = "no_horizons(model_horizons_empty)"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.status = "ERR_NO_HORIZONS"
            return dec

        # Выбираем лучший горизонт по score
        best_h = 0
        best_score = -1e18
        best_p_cal = 0.0
        best_unc = 0.0

        for h in horizons:
            if h not in score_dict or h not in p_cal_dict or h not in unc_dict:
                continue
            try:
                sc = float(score_dict[h][0])
                p_cal = float(p_cal_dict[h][0])
                unc = float(unc_dict[h][0])

                if not (math.isfinite(sc) and math.isfinite(p_cal) and math.isfinite(unc)):
                    continue

                if sc > best_score:
                    best_score = sc
                    best_h = int(h)
                    best_p_cal = p_cal
                    best_unc = unc
            except (IndexError, KeyError, TypeError, ValueError):
                continue

        if best_score <= -1e17:
            dec.error = "no_valid_scores"
            dec.reason = f"no_valid_scores(horizons={len(horizons)})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = best_h
            dec.status = "ERR_NO_VALID_SCORES"
            bucket = _bucket_from_scenario(scenario)
            dec.bucket = bucket
            floor = _get_floor(cfg.get("edge_floors", {}), bucket)
            try:
                floor = max(float(floor), float(self._p_min_hard_floor))
            except Exception:
                floor = float(floor)
            dec.floor = float(floor)
            dec.allow = False
            return dec

        # Определение bucket и floor
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket
        edge_floors = cfg.get("edge_floors", {})
        floor = _get_floor(edge_floors, bucket)
        try:
            floor = max(float(floor), float(self._p_min_hard_floor))
        except Exception:
            floor = float(floor)

        dec.best_h_ms = best_h
        dec.score = float(best_score)
        dec.floor = float(floor)

        # p_edge: используем p_cal лучшего горизонта
        dec.p_edge_raw = float(best_p_cal)
        dec.p_edge_cal = float(best_p_cal)
        dec.calib_type = "platt_logit"  # модель уже калибрована

        # use calibrated p_edge for downstream thresholds/metrics
        dec.p_edge = float(dec.p_edge_cal)
        dec.p_min = float(floor)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        # Решение: allow if best_score >= floor
        dec.allow = bool(best_score >= floor)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"edge_stack_mh(score={best_score:.4f},floor={floor:.4f},h={best_h},bucket={bucket},unc={best_unc:.4f})"

        # Сохраняем uncertainty для метрик
        dec.unc = {str(best_h): float(best_unc)}

        return dec

    def _decide_meta_lr(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
        cfg: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> MLConfirmDecision:
        """Decision logic for simple MetaModelLR (logistic regression)."""
        cfg = cfg if cfg is not None else self._cfg
        model = model if model is not None else self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="meta_lr", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        if not isinstance(model, MetaModelLR):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_type"
            dec.reason = f"bad_model_type(expected=MetaModelLR,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # P0 fix: MetaModelLR использует 'features' вместо 'feature_cols',
        # но имеет transforms и robust_scaler, которые нужно прокинуть в _build_feature_row
        class _MetaModelView:
            def __init__(self, meta_model: MetaModelLR):
                self.feature_cols = meta_model.features  # маппинг features -> feature_cols
                self.feature_transforms = getattr(meta_model, "transforms", {}) or {}
                self.robust_scaler = getattr(meta_model, "robust_scaler", None)
                # для session/spread/liq используем defaults (MetaModelLR обычно не имеет этих cfg)
                self.session_cfg = {}
                self.spread_bucket_edges = None
                self.liq_cfg = {}

        view = _MetaModelView(model)
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms,
        )
        dec.missing = missing

        # ENFORCE missing check
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        # Predict
        # construct feat dict from row? No, predict_proba expects dict?
        # MetaModelLR.predict_proba expects Dict[str, Any]
        # BUT _build_feature_row returns List[float] for feature_cols.
        # This is inefficient: we built list, now need to rebuild dict or unsafe existing methods.
        # Actually MetaModelLR.predict_proba iterates over self.features and does lookups.
        # So we can just pass indicators directly?
        # _build_feature_row handles critical checks and derived features (like spread_bucket).
        # MetaModelLR *might* depend on derived features.
        # Let's inspect MetaModelLR.predict_proba again.
        # It calls _f(feat.get(name, 0.0)).

        # If model.features includes "spread_bucket_..." or "session_...", we need those derived.
        # _build_feature_row logic is complex and handles derivation.
        # Ideally we should refactor, but for now let's construct a feat dict from the row we just built.

        feat_dict = {}
        for i, col in enumerate(model.features):
            feat_dict[col] = x_row[i]

        try:
            p_edge_raw = model.predict_proba(feat_dict)
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "prediction_failed"
            dec.reason = f"prediction_failed({str(e)[:100]})"
            dec.status = "ERR_PRED"
            return dec

        if not math.isfinite(p_edge_raw):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_pred"
            dec.reason = f"non_finite_pred({p_edge_raw})"
            dec.status = "ERR_NON_FINITE"
            return dec

        dec.p_edge_raw = float(p_edge_raw)
        dec.p_edge_cal = float(p_edge_raw)
        dec.calib_type = str(self._calib_type or "none")

        # Optional calibration
        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        if bool(calibrate) and self._calibrator is not None:
             dec.p_edge_cal = float(self._calibrator.apply_one(dec.p_edge_raw))

        dec.p_edge = float(dec.p_edge_cal)

        # Determine p_min
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket

        # p_min from config
        p_min_by_bucket = cfg.get("util_floors", {}).get("by_bucket", {})
        # Flatten structure if needed or just use what we stored in init_ml... (util_floors.by_bucket.{bucket}.floor)
        # Note: init_ml_confirm_on_startup sets structure: util_floors.by_bucket.trend.floor = 0.55
        # So we can traverse that.

        floor = _get_floor(cfg.get("util_floors", {}), bucket)
        if floor == 0.0:  # fallback to top-level p_min
            floor = float(cfg.get("p_min", 0.55))

        # guardrail
        with contextlib.suppress(Exception):
            floor = max(float(floor), float(self._p_min_hard_floor))

        dec.p_min = float(floor)
        dec.floor = float(floor)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        dec.allow = bool(dec.p_edge >= dec.p_min)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"meta_lr(p={dec.p_edge:.4f},thr={dec.p_min:.4f},bucket={bucket})"

        return dec

    def check(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        rule_score: float,
        rule_have: int,
        rule_need: int,
        cancel_spike_veto: int,
        ok_rule: int,
    ) -> MLConfirmDecision:
        self._check_call_count += 1
        t0_ns = time.perf_counter_ns()
        t0_sec = time.time()
        # Ensure we do NOT load synchronously. refresh_async will load in background on startup.

        if self.mode == "OFF":
            # Distinguish: OFF from ab_variant vs OFF from ENV/default
            _ab = self.ab_variant  # e.g. "off" | "shadow" | "enforce" | "challenger" | ""
            _off_source = f"ab_variant:{_ab}" if _ab == "off" else "global"
            _off_reason = f"mode_off(ab_variant={_ab})" if _ab == "off" else "mode_off"
            dec = MLConfirmDecision(mode="OFF", kind="none", allow=True, reason=_off_reason)
            dec.status = "OFF"
            dec.effective_mode = "OFF"
            dec.mode_source = _off_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind="none", outcome="OFF").inc()
                self._metrics_latency_seconds.labels(kind="none").observe(latency_sec)
            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            return dec

        # Resolve kind from indicators or fallback to primary legacy config
        req_kind = (indicators.get("ml_kind", "")).strip().lower()
        if not req_kind and self._cfg:
            req_kind = str(self._cfg.get("kind", "")).strip().lower()

        kind = req_kind or "none"

        # Determine effective model, cfg, and per-symbol overrides for this kind
        cfg = self._cfgs.get(kind) or self._cfg
        model = self._models.get(kind) or self._model
        _mode_by_sym = self._mode_by_symbol_by_kind.get(kind, self._mode_by_symbol)
        _enf_share_by_sym = self._enforce_share_by_sym_by_kind.get(kind, self._enforce_share_by_symbol)
        _cfg_key_used = self._cfg_keys_used.get(kind, self._cfg_key_used)
        _cfg_source = self._cfg_sources.get(kind, self._cfg_source)

        # Per-symbol mode resolution (early phase: steps 1-2 don't need _cfg loaded)
        # Priority chain:
        #   1. mode_overrides.by_symbol[SYMBOL] from champion JSON (hot-reloadable via _mode_by_sym)
        #   2. ML_CONFIRM_MODE__SYMBOL from ENV (static, restart required)
        #   3. Global canary/rollout logic (needs cfg, applied later)
        #   4. self.mode (global ENV ML_CONFIRM_MODE)
        symbol_up = symbol.upper()
        effective_mode = self.mode  # fallback: global mode from ENV
        _mode_source = "global"

        if _mode_source == "global" and cfg:
            try:
                cfg_mode = (cfg.get("mode", "")).upper().strip()
                if cfg_mode in ("OFF", "SHADOW", "CANARY", "ENFORCE"):
                    effective_mode = cfg_mode
                    _mode_source = "cfg"
            except Exception:
                pass

        # 2. Check per-symbol ENV fallback
        _env_sym_mode = os.getenv(f"ML_CONFIRM_MODE__{symbol_up}", "").upper().strip()
        if _env_sym_mode in ("OFF", "SHADOW", "CANARY", "ENFORCE"):
            effective_mode = _env_sym_mode
            _mode_source = "env_per_symbol"

        # 1. Check per-symbol config override (highest priority, from champion JSON)
        _cfg_sym_mode = _mode_by_sym.get(symbol_up, "")
        if _cfg_sym_mode:
            effective_mode = _cfg_sym_mode
            _mode_source = "cfg_per_symbol"

        # Handle per-symbol OFF: short-circuit before config/model loading
        if effective_mode == "OFF":
            dec = MLConfirmDecision(mode="OFF", kind="none", allow=True,
                                   reason=f"mode_off(source={_mode_source},symbol={symbol_up})")
            dec.status = "OFF"
            dec.effective_mode = "OFF"
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind="none", outcome="OFF").inc()
                self._metrics_latency_seconds.labels(kind="none").observe(latency_sec)
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            return dec

        if not cfg:
            allow = self._fail_allow()
            # Distinguish missing key vs bad/empty cfg
            err = self._model_load_error or "no_cfg"
            if err == "parse_error:CfgError":
                err = "bad_cfg"

            rsn = "no_cfg" if err == "no_cfg" else f"bad_cfg({self._cfg_parse_err})"
            dec = MLConfirmDecision(mode="ERR", kind="none", allow=allow, reason=rsn, error=err)
            dec.status = "ERR_NO_CFG" if err == "no_cfg" else "ERR_BAD_CFG"
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec
            kind_for_metrics = "unknown"
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome="ERR").inc()
                self._metrics_errors_total.labels(kind=kind_for_metrics, reason=err).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)
            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            return dec

        # 3. Canary / Rollout logic (only applies when effective_mode is still SHADOW, requires cfg)
        if effective_mode == "SHADOW":
            try:
                # Resolve enforce_share: per-symbol override > cfg global > ENV
                per_sym_share = _enf_share_by_sym.get(symbol_up)
                if per_sym_share is not None:
                    enforce_share = float(per_sym_share)
                    _mode_source = "cfg_per_symbol_canary"
                else:
                    env_share = float(os.getenv("ML_CONFIRM_ENFORCE_SHARE", "0.0") or 0.0)
                    enforce_share = float(cfg.get("enforce_share", env_share) or 0.0)

                if enforce_share > 0.0:
                    # CANARY: deterministic routing by sid.
                    # A signal is enforced iff stable_u01 < enforce_share.
                    raw_sid = str(indicators.get("sid") or indicators.get("signal_id") or "") if indicators else ""
                    sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
                    run_id = (cfg.get("run_id", "unknown"))
                    salt = f"{run_id}|{kind}"
                    if _stable_u01(f"canary|{sid}", salt=salt) < float(enforce_share):
                        effective_mode = "ENFORCE"
                        if _mode_source == "global":
                            _mode_source = "canary"
            except Exception:
                pass

        if kind.lower().startswith("util_mh"):
            dec = self._decide_util_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode, cfg=cfg, model=model)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            return dec

        # P1 guard: ENFORCE with p_min < 0.5 and no per-bucket overrides lets ~80% of signals
        # pass the gate unchecked. Warn loudly; operator must set p_min_by_bucket or raise p_min.
        if effective_mode == "ENFORCE":
            try:
                _p_min_val = float(cfg.get("p_min", 0.5) or 0.5)
                _p_min_buckets = cfg.get("p_min_by_bucket") or {}
                if _p_min_val < 0.5 and not _p_min_buckets:
                    logger.warning(
                        "⚠️ ML gate ENFORCE p_min=%.2f < 0.5 with empty p_min_by_bucket "
                        "for %s/%s — gate is nearly open. Set p_min≥0.5 or add p_min_by_bucket "
                        "in cfg:ml_confirm:champion. (cfg_key=%s)",
                        _p_min_val, symbol, kind, _cfg_key_used,
                    )
            except Exception:
                pass

        if kind.lower() == "edge_stack_v1":
            dec = self._decide_edge_stack_v1(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode, cfg=cfg, model=model)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            return dec

        if kind == "meta_lr":
            dec = self._decide_meta_lr(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode, cfg=cfg, model=model)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            return dec

        if kind.lower().startswith("edge_stack_mh"):
            dec = self._decide_edge_stack_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode, cfg=cfg, model=model)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            # Update Prometheus metrics
            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                # Determine outcome for metrics
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            return dec

        if kind.lower().startswith("ml_scorer"):
            dec = self._decide_ml_scorer(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode, cfg=cfg, model=model)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = _cfg_key_used
            dec.cfg_source = _cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
            dec.effective_mode = effective_mode
            dec.mode_source = _mode_source
            dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
            latency_sec = time.time() - t0_sec

            if METRICS_REGISTRY_AVAILABLE:
                kind_for_metrics = kind or "unknown"
                if dec.error:
                    outcome = "ERR"
                    self._metrics_errors_total.labels(kind=kind_for_metrics, reason=dec.error or "unknown").inc()
                elif dec.status == "SHADOW":
                    outcome = "SHADOW"
                elif dec.allow:
                    outcome = "ALLOW"
                else:
                    outcome = "DENY"
                self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome=outcome).inc()
                self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)

            sid = _canonical_sid(indicators, symbol, ts_ms)
            self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                               rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                               cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
            self._cache_ml_decision(dec, sid=sid, symbol=symbol, scenario=scenario, ok_rule=ok_rule)
            self._capture_replay_input(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                                       indicators=indicators, rule_score=rule_score, rule_have=rule_have,
                                       rule_need=rule_need, cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule)
            return dec

        # если когда-то будут другие kind — можно расширить, но для v10.4 достаточно util_mh
        allow = self._fail_allow()
        dec = MLConfirmDecision(mode="ERR", kind=kind or "unknown", allow=allow, reason="unsupported_kind", error="unsupported_kind")
        dec.status = "ERR_UNSUPPORTED_KIND"
        dec.cfg_key_used = _cfg_key_used
        dec.cfg_source = _cfg_source
        dec.cfg_raw_len = int(self._cfg_raw_len)
        dec.cfg_parse_err = self._cfg_parse_err
        dec.effective_mode = effective_mode
        dec.mode_source = _mode_source
        dec.latency_us = int((time.perf_counter_ns() - t0_ns) / 1000)
        latency_sec = time.time() - t0_sec
        kind_for_metrics = kind or "unknown"
        if METRICS_REGISTRY_AVAILABLE:
            self._metrics_events_total.labels(ab_variant=str(self.ab_variant or ""), kind=kind_for_metrics, outcome="ERR").inc()
            self._metrics_errors_total.labels(kind=kind_for_metrics, reason="unsupported_kind").inc()
            self._metrics_latency_seconds.labels(kind=kind_for_metrics).observe(latency_sec)
        # Extract sid from indicators or generate in format crypto-of:{symbol}:{ts_ms}
        sid = _canonical_sid(indicators, symbol, ts_ms)
        self._emit_metrics(dec, symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario,
                           rule_score=rule_score, rule_have=rule_have, rule_need=rule_need,
                           cancel_spike_veto=cancel_spike_veto, ok_rule=ok_rule, sid=sid, indicators=indicators)
        return dec

    async def check_async(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        rule_score: float,
        rule_have: int,
        rule_need: int,
        cancel_spike_veto: int,
        ok_rule: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> MLConfirmDecision:
        """Async wrapper: runs check() in a thread pool so LightGBM predict()
        never blocks the asyncio event loop.

        LightGBM releases the GIL during predict() → true thread-level
        parallelism even across multiple concurrent signal evaluations.

        Usage (drop-in replacement for check()):
            ml_dec = await self._ml_gate.check_async(symbol=..., ts_ms=..., ...)

        Thread count controlled by ML_CONFIRM_THREADS env (default 2).
        """
        _loop = loop or asyncio.get_event_loop()
        return await _loop.run_in_executor(
            _get_ml_executor(),
            lambda: self.check(
                symbol=symbol,
                ts_ms=ts_ms,
                direction=direction,
                scenario=scenario,
                indicators=indicators,
                rule_score=rule_score,
                rule_have=rule_have,
                rule_need=rule_need,
                cancel_spike_veto=cancel_spike_veto,
                ok_rule=ok_rule,
            ),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Process-level shared ThreadPoolExecutor for ML inference.
# LightGBM's predict() releases the GIL → true parallelism across threads.
# Max workers default=2: handles burst of 2 simultaneous signal evals;
# configure via ML_CONFIRM_THREADS without rebuild.
# B2 CONFIRMED: _ML_INFER_EXECUTOR is a module-level singleton (global + lazy init).
# Created once per process, never recreated → no thread pool leak.
# ──────────────────────────────────────────────────────────────────────────────
_ML_INFER_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
_OF_BUILD_SEMAPHORE: asyncio.Semaphore | None = None


def _get_of_build_slots() -> int:
    raw = os.getenv("OF_BUILD_MAX_INFLIGHT", os.getenv("ML_CONFIRM_THREADS", "2"))
    try:
        return max(1, int(raw or "2"))
    except Exception:
        return 2


def _get_of_build_semaphore() -> asyncio.Semaphore:
    global _OF_BUILD_SEMAPHORE
    if _OF_BUILD_SEMAPHORE is None:
        _OF_BUILD_SEMAPHORE = asyncio.Semaphore(_get_of_build_slots())
    return _OF_BUILD_SEMAPHORE


def _get_ml_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _ML_INFER_EXECUTOR
    if _ML_INFER_EXECUTOR is None:
        n = int(os.getenv("ML_CONFIRM_THREADS", "2") or "2")
        _ML_INFER_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, n),
            thread_name_prefix="ml-infer",
        )
    return _ML_INFER_EXECUTOR


async def run_bounded_of_build(fn, *, timeout_s: float, acquire_timeout_s: float | None = None):
    """Run OF build in the shared executor without allowing unbounded backlog."""
    from services.orderflow.metrics import (
        of_confirm_build_inflight,
        of_confirm_build_rejected_total,
        of_confirm_build_timeout_total,
    )

    symbol = "unknown"
    tf = "1s"
    try:
        symbol = str(getattr(fn, "_of_build_symbol", "unknown"))
        tf = str(getattr(fn, "_of_build_tf", "1s"))
    except Exception:
        pass

    semaphore = _get_of_build_semaphore()
    acquire_timeout = acquire_timeout_s
    if acquire_timeout is None:
        acquire_timeout = float(os.getenv("OF_BUILD_ACQUIRE_TIMEOUT_S", "0.01") or 0.01)
    acquire_timeout = max(0.001, float(acquire_timeout))

    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=acquire_timeout)
    except TimeoutError:
        with contextlib.suppress(Exception):
            of_confirm_build_rejected_total.labels(symbol=symbol, tf=tf, reason="executor_busy").inc()
        return None, "executor_busy"

    with contextlib.suppress(Exception):
        of_confirm_build_inflight.set(float(_get_of_build_slots() - semaphore._value))

    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(_get_ml_executor(), fn)
    released = False

    def _release_slot(_f=None) -> None:
        nonlocal released
        if released:
            return
        released = True
        with contextlib.suppress(Exception):
            semaphore.release()
        with contextlib.suppress(Exception):
            of_confirm_build_inflight.set(float(_get_of_build_slots() - semaphore._value))

    future.add_done_callback(_release_slot)

    try:
        result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout_s)
        _release_slot()
        return result, None
    except TimeoutError:
        with contextlib.suppress(Exception):
            of_confirm_build_timeout_total.labels(symbol=symbol, tf=tf).inc()
        return None, "timeout"


def _shutdown_ml_executor() -> None:
    """Gracefully shutdown the ML inference executor on process exit.
    Prevents thread pool leak on hot-reload or container restart.
    Called automatically via atexit.
    """
    global _ML_INFER_EXECUTOR
    if _ML_INFER_EXECUTOR is not None:
        with contextlib.suppress(Exception):
            _ML_INFER_EXECUTOR.shutdown(wait=False)
        _ML_INFER_EXECUTOR = None


# B4: Register graceful shutdown to prevent thread leak on process exit
import atexit as _atexit

_atexit.register(_shutdown_ml_executor)


def is_of_sync_build() -> bool:
    """B4 kill-switch: if OF_SYNC_BUILD=1, of_engine.build() runs synchronously
    in the event loop (blocks it) instead of using the thread pool.
    Use only for emergency rollback or debugging — sets OF_SYNC_BUILD=1 env.
    """
    return os.getenv("OF_SYNC_BUILD", "0").strip() == "1"
