"""Confidence calibration (production-safe).

Purpose
  - Convert a raw confidence (0..1) into a calibrated probability proxy.
  - Keep inference O(1), deterministic, and dependency-free (numpy-free).

Supported calibrators (operate on logit(p)):
  - temp_logit:  p' = sigmoid(logit(p) / T)
  - platt_logit: p' = sigmoid(a * logit(p) + b)

Notes
  - These are standard post-hoc calibration methods.
  - Training is done offline (see ml_analysis/tools/train_confidence_calibrator.py).
"""

from __future__ import annotations

import os
import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


def _clamp01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return x


def _sigmoid(z: float) -> float:
    # numerically stable sigmoid
    if z >= 0:
        ez = math.exp(-z)
        return 1.0 / (1.0 + ez)
    ez = math.exp(z)
    return ez / (1.0 + ez)


def _logit(p: float, eps: float) -> float:
    p = max(eps, min(1.0 - eps, p))
    return math.log(p / (1.0 - p))


@dataclass(frozen=True)
class ConfidenceCalibrator:
    type: str  # temp_logit | platt_logit | identity
    eps: float = 1e-6
    # temp
    t: float = 1.0
    # platt
    a: float = 1.0
    b: float = 0.0

    def apply(self, p_raw: float) -> float:
        try:
            p = float(p_raw)
            if not math.isfinite(p):
                return float('nan')
            p = _clamp01(p)

            if self.type == "identity":
                return p

            z = _logit(p, self.eps)

            if self.type == "temp_logit":
                T = float(self.t)
                if not math.isfinite(T) or T <= 1e-6:
                    return p
                return _sigmoid(z / T)

            if self.type == "platt_logit":
                a = float(self.a)
                b = float(self.b)
                if not (math.isfinite(a) and math.isfinite(b)):
                    return p
                return _sigmoid(a * z + b)

            # fallback
            return p
        except Exception:
            return float('nan')


def load_calibrator_from_dict(d: Dict[str, Any]) -> ConfidenceCalibrator:
    typ = str(d.get("type") or "identity").lower()
    eps = float(d.get("eps", 1e-6) or 1e-6)
    if eps <= 0:
        eps = 1e-6

    if typ == "temp_logit":
        t = float(d.get("t", 1.0) or 1.0)
        return ConfidenceCalibrator(type="temp_logit", eps=eps, t=t)

    if typ == "platt_logit":
        a = float(d.get("a", 1.0) or 1.0)
        b = float(d.get("b", 0.0) or 0.0)
        return ConfidenceCalibrator(type="platt_logit", eps=eps, a=a, b=b)

    return ConfidenceCalibrator(type="identity", eps=eps)


try:
    from orderflow_services.confidence_cal_metrics import emit_file_state, emit_train_report, inc_reload
except Exception:  # pragma: no cover
    emit_file_state = None  # type: ignore
    emit_train_report = None  # type: ignore
    inc_reload = None  # type: ignore


def _symbol_from_runtime(runtime: Any) -> str:
    try:
        return str(getattr(runtime, "symbol", None) or getattr(runtime, "symbol_name", None) or getattr(runtime, "instrument", None) or "unknown")
    except Exception:
        return "unknown"


def load_calibrator_payload(path: str) -> Optional[Dict[str, Any]]:
    """Load and schema-guard a calibrator JSON payload."""
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        sv = int(d.get("schema_version", 1) or 1)
        if sv != 1:
            return None
        return d
    except Exception:
        return None


def load_calibrator_from_dict(d: Dict[str, Any]) -> ConfidenceCalibrator:
    typ = str(d.get("type") or "identity").lower()
    eps = float(d.get("eps", 1e-6) or 1e-6)
    if eps <= 0:
        eps = 1e-6

    if typ == "temp_logit":
        t = float(d.get("t", 1.0) or 1.0)
        return ConfidenceCalibrator(type="temp_logit", eps=eps, t=t)

    if typ == "platt_logit":
        a = float(d.get("a", 1.0) or 1.0)
        b = float(d.get("b", 0.0) or 0.0)
        return ConfidenceCalibrator(type="platt_logit", eps=eps, a=a, b=b)

    return ConfidenceCalibrator(type="identity", eps=eps)


def load_calibrator_from_path(path: str) -> Optional[ConfidenceCalibrator]:
    if not path:
        return None
    try:
        d = load_calibrator_payload(path)
        if not d:
            return None
        return load_calibrator_from_dict(d)
    except Exception:
        return None


def get_cached_calibrator(
    runtime: Any,
    path: str,
    *,
    check_every_ms: int = 5000,
    max_age_ms: int = 0,
    disable_if_stale: int = 0,
) -> Optional[ConfidenceCalibrator]:
    """
    Fast path for production: keep calibrator cached in runtime and only
    re-check file mtime periodically.

    Why:
      - loading JSON from disk on every signal is expensive and adds jitter
      - this makes calibration effectively O(1) and reloads are bounded

    Cache key on runtime:
      runtime._confidence_cal_cache = {
        "path": str,
        "mtime_ns": int,
        "last_check_ms": int,
        "cal": ConfidenceCalibrator|None,
        "meta": dict|None,
      }
    """
    try:
        p = str(path or "").strip()
        if not p:
            return None

        now_ms = int(time.time() * 1000)
        sym = _symbol_from_runtime(runtime)
        cache = getattr(runtime, "_confidence_cal_cache", None)
        if not isinstance(cache, dict):
            cache = {"path": "", "mtime_ns": 0, "last_check_ms": 0, "cal": None, "meta": None}

        # Throttle filesystem checks
        if cache.get("path") == p:
            last = int(cache.get("last_check_ms", 0) or 0)
            if (now_ms - last) < int(check_every_ms):
                return cache.get("cal")

        cache["last_check_ms"] = now_ms
        cache["path"] = p

        try:
            st = os.stat(p)
            mtime_ns = int(getattr(st, "st_mtime_ns", 0) or 0)
            age_ms = int(max(0, now_ms - int(mtime_ns // 1_000_000)))
        except Exception:
            # If file is missing/unreadable, fail-open: disable calibration.
            cache["mtime_ns"] = 0
            cache["cal"] = None
            cache["meta"] = None
            setattr(runtime, "_confidence_cal_cache", cache)
            if emit_file_state is not None:
                emit_file_state(sym, present=0, age_ms=0, stale=0)
            return None

        stale = 1 if (int(max_age_ms or 0) > 0 and age_ms > int(max_age_ms)) else 0
        if emit_file_state is not None:
            emit_file_state(sym, present=1, age_ms=age_ms, stale=stale)
        if stale and int(disable_if_stale or 0) == 1:
            # Operationally safer: if stale, prefer disabling rather than applying an old mapping.
            cache["mtime_ns"] = mtime_ns
            cache["cal"] = None
            cache["meta"] = None
            setattr(runtime, "_confidence_cal_cache", cache)
            return None

        if int(cache.get("mtime_ns", 0) or 0) != mtime_ns or cache.get("cal") is None:
            payload = load_calibrator_payload(p)
            cal = load_calibrator_from_dict(payload) if payload else None
            cache["mtime_ns"] = mtime_ns
            cache["cal"] = cal
            cache["meta"] = payload
            setattr(runtime, "_confidence_cal_cache", cache)
            if inc_reload is not None:
                inc_reload(sym, "ok" if cal is not None else "fail")
            # Emit train-time metrics if present
            if payload and emit_train_report is not None:
                try:
                    rep = payload.get("train_report") or {}
                    raw = rep.get("raw") or {}
                    cc = rep.get("cal") or {}
                    emit_train_report(
                        sym,
                        cal_type=str(payload.get("type") or "unknown"),
                        schema_version=int(payload.get("schema_version", 1) or 1),
                        raw_ece=float(raw.get("ece", 0.0) or 0.0),
                        cal_ece=float(cc.get("ece", 0.0) or 0.0),
                        raw_brier=float(raw.get("brier", 0.0) or 0.0),
                        cal_brier=float(cc.get("brier", 0.0) or 0.0),
                    )
                except Exception:
                    pass
            return cal

        setattr(runtime, "_confidence_cal_cache", cache)
        return cache.get("cal")
    except Exception:
        return None
