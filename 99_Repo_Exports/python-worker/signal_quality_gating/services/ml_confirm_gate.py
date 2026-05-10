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
from core.redis_keys import RedisStreams as RS

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
_SHARED_MODELS: dict[str, Any] = {}
_SHARED_CONFIGS: dict[str, Any] = {}
_SHARED_CONFIG_PAYLOADS: dict[str, bytes] = {}  # key -> last raw payload
_SHARED_MODEL_STATS: dict[str, tuple[float, int]] = {} # path -> (mtime, size)


def _load_model_cached(model_path: str, kind: str, logger: Any = None) -> Any | None:
    """Load model from disk or return from process-level cache if unchanged."""
    if not model_path or not os.path.exists(model_path):
        print(f"DEBUG: Model path does not exist: {model_path}", flush=True)
        return None

    try:
        mtime = os.path.getmtime(model_path)
        size = os.path.getsize(model_path)
    except Exception as e:
        print(f"DEBUG: Failed to get stats for {model_path}: {e}", flush=True)
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
    if "range" in s0 or "meanrev" in s0 or "chop" in s0:
        return "range"
    if "trend" in s0 or "continuation" in s0 or "reversal" in s0:
        return "trend"
    return "other"


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
    r: Any,
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
        r: Redis client (decode_responses=True), can be sync or aioredis
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
    payload_str = json.dumps(payload, separators=(",", ":"))
    try:
        import asyncio
        is_async = "aioredis" in type(r).__module__ or "asyncio" in type(r).__module__ or (hasattr(r.set, "__call__") and asyncio.iscoroutinefunction(r.set))
        if is_async:
            try:
                from utils.task_manager import safe_create_task
                safe_create_task(r.set(key, payload_str, ex=ttl_sec))
            except ImportError:
                asyncio.create_task(r.set(key, payload_str, ex=ttl_sec))
        else:
            r.set(key, payload_str, ex=ttl_sec)
    except Exception:
        # Fail-open: don't break decision flow if cache write fails
        pass


def _stable_hash_u64(s: str) -> int:
    """Generate stable 64-bit hash from string (for deterministic sampling)"""
    h = hashlib.md5(s.encode("utf-8")).digest()[:8]
    return int.from_bytes(h, "big", signed=False)


def _stable_u01(s: str) -> float:
    """Generate stable uniform [0,1) value from string"""
    return _stable_hash_u64(s) / float(2**64 - 1)


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

    # cfg diagnostics (for metrics/debug)
    cfg_key_used: str = ""
    cfg_source: str = ""        # champion|challenger
    cfg_raw_len: int = 0
    cfg_parse_err: str = ""

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
    ) -> None:
        self.r = r
        self.mode = (mode or "OFF").upper()
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

        # last cfg diagnostics (used when returning ERR_* decisions)
        self._cfg_key_used: str = ""
        self._cfg_source: str = ""
        self._cfg_raw_len: int = 0
        self._cfg_parse_err: str = ""

        # metrics
        self._metrics_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)
        self._metrics_enable = int(os.getenv("ML_CONFIRM_METRICS_ENABLE", "1") or 1) == 1
        # P1 fix: было hardcode 50000 → теперь через ENV (default 200000)
        self._metrics_maxlen = int(os.getenv("ML_CONFIRM_METRICS_MAXLEN", "200000") or 200000)
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
        self._replay_stream = os.getenv("ML_REPLAY_INPUTS_STREAM", RS.ML_CONFIRM_INPUTS)
        try:
            self._replay_sample = float(os.getenv("ML_REPLAY_INPUTS_SAMPLE", "0.01") or 0.01)
        except Exception:
            self._replay_sample = 0.01
        self._replay_maxlen = int(os.getenv("ML_REPLAY_INPUTS_MAXLEN", "200000") or 200000)

        # calibration layer (optional)
        self._calibrator: PlattLogitCalibrator | None = None
        self._calibrate_enabled = int(os.getenv("ML_CALIBRATION_ENABLE", "1") or 1) == 1
        self._calib_type = "none"

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
    def from_env(redis_client: Any | None = None) -> MLConfirmGate:
        if redis_client is not None:
            r = redis_client
        else:
            # Support ML_REDIS_URL for separate config Redis, fallback to REDIS_URL
            redis_url = os.getenv("ML_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            r = redis.Redis.from_url(redis_url, decode_responses=True)

        mode = os.getenv("ML_CONFIRM_MODE", "SHADOW")
        fail_policy = os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN")
        champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
        challenger_key = os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger")

        return MLConfirmGate(
            r=r,
            mode=mode,
            fail_policy=fail_policy,
            champion_key=champion_key,
            challenger_key=challenger_key,
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
        """
        import logging
        logger = logging.getLogger("ml_confirm_gate")

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
                    p = json.loads(raw_p)
                    if isinstance(p, dict) and p:
                        raw_payload = raw_p
                        self._cfg_source = "champion"
                        self._cfg_key_used = self.champion_key
                except Exception:
                    pass

            # 1b. Try Challenger (only if SHADOW and no successful Champion)
            if not raw_payload and self.mode == "SHADOW":
                raw_p = self.r.get(self.challenger_key)
                if raw_p:
                    try:
                        p = json.loads(raw_p)
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
            return {}, None

        if not raw_payload:
            self._model_load_error = "no_cfg"
            return {}, None

        return self._parse_and_load_from_payload(raw_payload, id(self.r), logger)

    def _parse_and_load_from_payload(self, raw_payload: Any, cache_key_id: int, logger: Any) -> tuple[dict[str, Any], Any]:
        self._cfg_raw_len = len(raw_payload)

        # Step 2: Check process-level cache for JSON payloads (Isolated by Redis instance ID)
        cache_key = (cache_key_id, self._cfg_key_used)
        if _SHARED_CONFIG_PAYLOADS.get(cache_key) == raw_payload:
            cached_cfg = _SHARED_CONFIGS.get(cache_key)
            if cached_cfg:
                model_path = cached_cfg.get("model_path")
                kind = (cached_cfg.get("kind", "")).lower()
                model = _load_model_cached(model_path, kind, logger=logger)
                return cached_cfg.copy(), model

        # Step 3: Parse and Validate
        try:
            payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else str(raw_payload)
            cfg = json.loads(payload_str)

            try:
                cfg_validated, validation_info = validate_champion_cfg(payload_str)
                # Ensure validated fields are mapped if validation succeeded
                cfg["model_path"] = cfg_validated.model_path
                cfg["kind"] = cfg_validated.kind
                cfg["mode"] = cfg_validated.mode
                cfg["enforce_share"] = cfg_validated.enforce_share
                cfg["run_id"] = cfg_validated.run_id
            except Exception as ve:
                # Lenient mode: Log warning but keep the parsed JSON
                logger.warning(f"ML gate: Config validation failed for {self._cfg_key_used}, but using as-is (legacy): {ve}")

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
            logger.error(f"ML gate: Config parse/validate failed for {self._cfg_key_used}: {e}")
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

        # 1. Fetch from Redis (Async)
        self._cfg_key_used = self.champion_key
        self._cfg_source = "none"
        raw_payload = None

        try:
            # 1a. Try Champion
            raw_p = await redis_async.get(self.champion_key)
            if raw_p:
               try:
                   p = json.loads(raw_p)
                   if isinstance(p, dict) and p:
                       raw_payload = raw_p
                       self._cfg_source = "champion"
                       self._cfg_key_used = self.champion_key
               except Exception:
                   pass

            # 1b. Challenger
            if not raw_payload and self.mode == "SHADOW":
                raw_p = await redis_async.get(self.challenger_key)
                if raw_p:
                    try:
                        p = json.loads(raw_p)
                        if isinstance(p, dict) and p:
                            raw_payload = raw_p
                            self._cfg_source = "challenger"
                            self._cfg_key_used = self.challenger_key
                    except Exception:
                        pass

            # 1c. Hash Fallback
            if not raw_payload:
                h = await redis_async.hgetall(self._cfg_hash_key)
                if h and isinstance(h, dict) and len(h) > 0:
                     cfg_dict = self._coerce_hash_cfg(h)
                     self._cfg_source = "hash_fallback"
                     self._cfg_key_used = self._cfg_hash_key
                     raw_payload = json.dumps(cfg_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except Exception as e:
            logger.error(f"ML gate: Async Redis error: {e}")
            # Don't return, allow retry next loop
            return

        if not raw_payload:
            self._model_load_error = "no_cfg"
            # Do not clear existing config on momentary Redis failure, just return
            return

        # 2. Parse & Load (Run in thread to avoid blocking loop depending on model size)
        loop = asyncio.get_running_loop()
        try:
            # Use id(redis_async) for cache isolation
            cfg, model = await loop.run_in_executor(
                None,
                self._parse_and_load_from_payload,
                raw_payload,
                id(redis_async),
                logger
            )
            self._cfg = cfg or {}
            self._model = model
            self._cache_loaded_ms = now

            # Refresh selective knobs logic (duplicated from sync path for now)
            self._refresh_selective_knobs_from_cfg()

            # Load calibrator logic
            if self._calibrate_enabled:
                 await loop.run_in_executor(None, self._load_calibrator_sync, logger)

        except Exception as e:
            logger.error(f"ML gate: Async parse failed: {e}")

    def _refresh_selective_knobs_from_cfg(self) -> None:
        try:
            if self._cfg.get("abstain_band") is not None:
                self._abstain_band = float(self._cfg.get("abstain_band"))
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
                        if cal_path.endswith(".json"):
                            with open(cal_path, encoding="utf-8") as f:
                                cal_dict = json.load(f)
                            if isinstance(cal_dict, dict) and (cal_dict.get("type", "") or "") == "platt_logit":
                                self._calibrator = PlattLogitCalibrator.from_dict(cal_dict)
                                self._calib_type = "cfg_calibrator_path"
                                logger.info(f"ML gate: Calibrator loaded from cfg.calibrator_path={cal_path}")
                        elif cal_path.endswith(".joblib") and joblib is not None:
                            cal_obj = joblib.load(cal_path)
                            if isinstance(cal_obj, dict) and (cal_obj.get("type", "") or "") == "platt_logit":
                                self._calibrator = PlattLogitCalibrator.from_dict(cal_obj)
                                self._calib_type = "cfg_calibrator_path"
                                logger.info(f"ML gate: Calibrator loaded from cfg.calibrator_path={cal_path}")
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

        # UTC hour/day-of-week and scenario bucket (legacy bucket:)
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
                val = col[len("scenario_v4_"):].lower()
                row.append(1.0 if val == s else 0.0)
            elif col.startswith("bucket:"):
                val = col[len("bucket:"):].lower()
                row.append(1.0 if val == str(bucket).lower() else 0.0)
            elif col.startswith("bucket2:"):
                val = col[len("bucket2:"):].lower()
                row.append(1.0 if bucket2 and val == str(bucket2).lower() else 0.0)
            elif col.startswith("session_"):
                val = col[len("session_"):].lower()
                row.append(1.0 if val == str(session_label) else 0.0)
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
                if not _stable_sample(sid, sample_rate, salt=RS.ML_CONFIRM_METRICS):
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
                "latency_ms": f"{float(dec.latency_us or 0) / 1000.0:.3f}",
                "status": str(dec.status or ""),
                "allow": str(int(bool(dec.allow))),
                "err": str(dec.error or ""),
                "abstain": str(int(bool(dec.abstain))),
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

            import asyncio
            is_async = "aioredis" in type(redis).__module__ or "asyncio" in type(redis).__module__ or (hasattr(redis.xadd, "__call__") and asyncio.iscoroutinefunction(redis.xadd))
            if is_async:
                try:
                    from utils.task_manager import safe_create_task
                    safe_create_task(redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True))
                except ImportError:
                    asyncio.create_task(redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True))
            else:
                redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True)
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
            }, maxlen=self._replay_maxlen, approximate=True)
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

    def _decide_util_mh(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
    ) -> MLConfirmDecision:
        cfg = self._cfg
        model = self._model

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

        # use calibrated p_edge for downstream thresholds/metrics
        dec.p_edge = float(dec.p_edge_cal)
        dec.p_min = float(floor)
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
        cfg = self._cfg
        model = self._model

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
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms
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

        import numpy as np
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
        try:
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
        cfg = self._cfg
        model = self._model

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
            ts_ms=ts_ms
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

        import numpy as np
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
    ) -> MLConfirmDecision:
        """Decision logic for simple MetaModelLR (logistic regression)."""
        cfg = self._cfg
        model = self._model

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
            ts_ms=ts_ms
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

        floor = 0.55 # default
        try:
            uf = cfg.get("util_floors", {})
            if isinstance(uf, dict):
                bb = uf.get("by_bucket", {})
                if isinstance(bb, dict) and bucket in bb:
                    floor = float(bb[bucket].get("floor", 0.55))
                else:
                    g = uf.get("global", {})
                    floor = float(g.get("floor", 0.55))
        except Exception:
            pass

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
            dec = MLConfirmDecision(mode="OFF", kind="none", allow=True, reason="mode_off")
            dec.status = "OFF"
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

        if not self._cfg:
            allow = self._fail_allow()
            # Distinguish missing key vs bad/empty cfg
            err = self._model_load_error or "no_cfg"
            if err == "parse_error:CfgError":
                err = "bad_cfg"

            rsn = "no_cfg" if err == "no_cfg" else f"bad_cfg({self._cfg_parse_err})"
            dec = MLConfirmDecision(mode="ERR", kind="none", allow=allow, reason=rsn, error=err)
            dec.status = "ERR_NO_CFG" if err == "no_cfg" else "ERR_BAD_CFG"
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
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

        kind = str(self._cfg.get("kind", "") or "")

        # Canary / Rollout logic (effective mode override)
        effective_mode = self.mode
        if self.mode == "SHADOW":
            # Check for ENFORCE promotion via canary bucket
            try:
                # Priority: 1. Redis Config, 2. Env Var, 3. Default 0.0
                env_share = float(os.getenv("ML_CONFIRM_ENFORCE_SHARE", "0.0") or 0.0)
                enforce_share = float(self._cfg.get("enforce_share", env_share) or 0.0)

                if enforce_share > 0.0:
                    # CANARY: deterministic routing by sid.
                    # A signal is enforced iff stable_u01 < enforce_share.
                    raw_sid = str(indicators.get("sid") or indicators.get("signal_id") or "") if indicators else ""
                    sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
                    run_id = str(self._cfg.get("run_id", "unknown"))
                    salt = f"{run_id}|{kind}"
                    if _stable_u01(f"canary|{sid}", salt=salt) < float(enforce_share):
                        effective_mode = "ENFORCE"
            except Exception:
                pass

        if kind.lower().startswith("util_mh"):
            dec = self._decide_util_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
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

        if kind.lower() == "edge_stack_v1":
            dec = self._decide_edge_stack_v1(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
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
            dec = self._decide_meta_lr(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
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
            dec = self._decide_edge_stack_mh(symbol=symbol, ts_ms=ts_ms, direction=direction, scenario=scenario, indicators=indicators, effective_mode=effective_mode)
            # apply selective prediction (only matters in ENFORCE + ok_rule)
            self._apply_selective(dec, ok_rule=ok_rule)
            dec.cfg_key_used = self._cfg_key_used
            dec.cfg_source = self._cfg_source
            dec.cfg_raw_len = int(self._cfg_raw_len)
            dec.cfg_parse_err = self._cfg_parse_err
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

        # если когда-то будут другие kind — можно расширить, но для v10.4 достаточно util_mh
        allow = self._fail_allow()
        dec = MLConfirmDecision(mode="ERR", kind=kind or "unknown", allow=allow, reason="unsupported_kind", error="unsupported_kind")
        dec.status = "ERR_UNSUPPORTED_KIND"
        dec.cfg_key_used = self._cfg_key_used
        dec.cfg_source = self._cfg_source
        dec.cfg_raw_len = int(self._cfg_raw_len)
        dec.cfg_parse_err = self._cfg_parse_err
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
