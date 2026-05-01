# -*- coding: utf-8 -*-
from __future__ import annotations
"""
confidence_conformal_v1.py

Split Conformal Prediction (binary) on top of calibrated confidence.

Goal
- Provide *coverage-guaranteed* prediction sets for the binary success label
  using split-conformal on nonconformity scores computed from calibrated p.

Model
- Given p = P(Y=1) (ideally after calibration: confidence_cal),
  nonconformity score is:
    s = 1 - p   if y=1
    s = p       if y=0
- For miscoverage alpha, compute qhat = quantile_{ceil((n+1)(1-alpha))/n}(s)
- For a new p, prediction set is:
    include label 1 if (1 - p) <= qhat  <=> p >= 1 - qhat
    include label 0 if p <= qhat
  If both included => abstain (set size 2).

Notes
- Fail-open: if model is missing/stale, we emit cp_* fields but do not block signals.
- Bucketization: optional per (symbol, kind). Falls back to symbol-only, then global.

Env
- CONF_CONFORMAL_PATH: JSON produced by train_confidence_conformal_v1.py
- CONF_CONFORMAL_RELOAD_TTL_SEC: reload interval
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def now_ms() -> int:
    return get_ny_time_millis()


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _bkey(symbol: str, kind: str) -> str:
    s = (symbol or "unknown").strip().upper()
    k = (kind or "unknown").strip().lower()
    return f"{s}|{k}"


@dataclass
class ConformalModel:
    schema_version: str
    alpha: float
    global_qhat: float
    buckets: Dict[str, float]
    trained_ts_ms: int
    source_path: str

    def qhat_for(self, symbol: str, kind: str) -> Tuple[float, str]:
        """Return (qhat, bucket_used)."""
        if self.buckets:
            bk = _bkey(symbol, kind)
            if bk in self.buckets:
                return float(self.buckets[bk]), bk
            # fallback: symbol-only bucket
            bs = f"{(symbol or 'unknown').strip().upper()}|*"
            if bs in self.buckets:
                return float(self.buckets[bs]), bs
        return float(self.global_qhat), "*"

    def predict_set(self, p: float, qhat: float) -> Dict[str, Any]:
        """
        Returns dict:
          cp_set_size: 1 or 2
          cp_abstain:  1 if set size 2
          cp_in_set0 / cp_in_set1: 0/1
          cp_thresh_p0_max: qhat
          cp_thresh_p1_min: 1-qhat
        """
        p = _clamp01(float(p))
        q = _clamp01(float(qhat))
        in0 = 1 if (p <= q) else 0
        in1 = 1 if ((1.0 - p) <= q) else 0
        size = int(in0 + in1)
        if size <= 0:
            # Shouldn't happen, but fail-open -> include both.
            in0, in1, size = 1, 1, 2
        return {
            "cp_set_size": int(size),
            "cp_abstain": int(1 if size == 2 else 0),
            "cp_in_set0": int(in0),
            "cp_in_set1": int(in1),
            "cp_thresh_p0_max": float(q),
            "cp_thresh_p1_min": float(1.0 - q),
        }


_CACHE: Dict[str, Any] = {
    "model": None,
    "loaded_ts_ms": 0,
    "mtime": 0.0,
}


def _load_model(path: str) -> Optional[ConformalModel]:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        schema_version = str(obj.get("schema_version") or "conf_conformal_v1")
        alpha = float(obj.get("alpha") or 0.10)
        global_qhat = float(obj.get("global_qhat") or 0.50)
        buckets = obj.get("buckets") or {}
        trained_ts_ms = int(obj.get("trained_ts_ms") or 0)
        # Normalize bucket keys to str->float
        b2: Dict[str, float] = {}
        if isinstance(buckets, dict):
            for k, v in buckets.items():
                try:
                    b2[str(k)] = float(v)
                except Exception:
                    continue
        return ConformalModel(
            schema_version=schema_version,
            alpha=alpha,
            global_qhat=global_qhat,
            buckets=b2,
            trained_ts_ms=trained_ts_ms,
            source_path=path,
        )
    except Exception:
        return None


def get_model() -> Optional[ConformalModel]:
    path = os.getenv("CONF_CONFORMAL_PATH", "").strip()
    ttl_sec = int(os.getenv("CONF_CONFORMAL_RELOAD_TTL_SEC", "60"))
    now = now_ms()

    # No path configured -> CP disabled (fail-open)
    if not path:
        return None

    # Reload if TTL exceeded or file changed
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        mtime = 0.0

    m: Optional[ConformalModel] = _CACHE.get("model")
    loaded_ts = int(_CACHE.get("loaded_ts_ms") or 0)
    last_mtime = float(_CACHE.get("mtime") or 0.0)

    need = (m is None) or ((now - loaded_ts) > ttl_sec * 1000) or (mtime and mtime != last_mtime)
    if need:
        nm = _load_model(path)
        _CACHE["model"] = nm
        _CACHE["loaded_ts_ms"] = now
        _CACHE["mtime"] = mtime
        return nm
    return m


def apply_conformal_binary(*, p: Optional[float], symbol: str, kind: str) -> Dict[str, Any]:
    """
    Apply CP to a calibrated probability p.

    Returns a dict that is safe to merge into indicators:
      cp_enabled, cp_alpha, cp_qhat, cp_bucket, cp_set_size, cp_abstain,
      cp_in_set0, cp_in_set1, cp_thresh_p0_max, cp_thresh_p1_min.
    """
    m = get_model()
    if p is None:
        return {"cp_enabled": 0}

    # Fail-open if missing model: do not abstain, but emit thresholds as unknown.
    if m is None:
        return {
            "cp_enabled": 0,
            "cp_alpha": float(os.getenv("CONF_CONFORMAL_ALPHA", "0.10")),
            "cp_qhat": 0.50,
            "cp_bucket": "missing_model",
            "cp_set_size": 2,
            "cp_abstain": 1,
            "cp_in_set0": 1,
            "cp_in_set1": 1,
            "cp_thresh_p0_max": 0.50,
            "cp_thresh_p1_min": 0.50,
        }

    qhat, bucket = m.qhat_for(symbol, kind)
    out = m.predict_set(float(p), float(qhat))
    out.update({
        "cp_enabled": 1,
        "cp_alpha": float(m.alpha),
        "cp_qhat": float(qhat),
        "cp_bucket": str(bucket),
    })
    return out
