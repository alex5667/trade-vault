"""LGBM long-gate reader (P2.A, 2026-05-27).

Reads `cfg:lgbm_long_gate` from Redis and exposes `predict_long_win_proba(ctx)`
for EntryPolicyGate. SHADOW mode → only compute + emit metric; ENFORCE mode →
veto when prob < p_min.

Lazy model load (joblib). TTL cache for the snapshot config (10s).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_CFG_KEY_DEFAULT = "cfg:lgbm_long_gate"
_SNAPSHOT_TTL_MS = 10_000


_CFG_LOCK = threading.Lock()
_LAST_FETCH_MS: int = 0
_SNAPSHOT: dict[str, Any] = {}
_MODEL_BUNDLE: dict[str, Any] | None = None
_MODEL_PATH_LOADED: str = ""


def _redis_url() -> str:
    return (
        os.environ.get("LGBM_LONG_GATE_READER_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )


_RC: Any = None
_RC_LOCK = threading.Lock()


def _get_redis() -> Any:
    global _RC
    if _RC is not None:
        return _RC
    with _RC_LOCK:
        if _RC is None:
            try:
                import redis  # type: ignore
                _RC = redis.from_url(_redis_url(), decode_responses=True, socket_timeout=0.5)
            except Exception as e:
                logger.debug("lgbm_reader: redis init fail (fail-open): %s", e)
                _RC = None
        return _RC


def _now_ms() -> int:
    return int(time.time() * 1000)


def _refresh_snapshot() -> None:
    global _LAST_FETCH_MS, _SNAPSHOT
    now_ms = _now_ms()
    if (now_ms - _LAST_FETCH_MS) < _SNAPSHOT_TTL_MS:
        return
    with _CFG_LOCK:
        if (now_ms - _LAST_FETCH_MS) < _SNAPSHOT_TTL_MS:
            return
        _LAST_FETCH_MS = now_ms
        rc = _get_redis()
        if rc is None:
            return
        try:
            cfg_key = os.environ.get("LGBM_LONG_GATE_CFG_KEY", _CFG_KEY_DEFAULT)
            raw = rc.get(cfg_key)
            if raw:
                _SNAPSHOT = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else {}
        except Exception as e:
            logger.debug("lgbm_reader: snapshot refresh fail: %s", e)


def _load_model_bundle() -> dict[str, Any] | None:
    global _MODEL_BUNDLE, _MODEL_PATH_LOADED
    _refresh_snapshot()
    path = (_SNAPSHOT.get("model_path") or "").strip()
    if not path:
        return None
    if _MODEL_BUNDLE is not None and _MODEL_PATH_LOADED == path:
        return _MODEL_BUNDLE
    with _CFG_LOCK:
        if _MODEL_BUNDLE is not None and _MODEL_PATH_LOADED == path:
            return _MODEL_BUNDLE
        try:
            import joblib  # type: ignore
            bundle = joblib.load(path)
            if not isinstance(bundle, dict) or "model" not in bundle:
                return None
            _MODEL_BUNDLE = bundle
            _MODEL_PATH_LOADED = path
            logger.info("lgbm_reader: model loaded: %s run_id=%s", path, bundle.get("run_id"))
            return _MODEL_BUNDLE
        except Exception as e:
            logger.debug("lgbm_reader: model load fail %s: %s", path, e)
            return None


def _build_feature_row(ctx: Any, kind: str) -> dict[str, float] | None:
    """Map ctx.indicators + kind/regime → feature dict matching trainer schema."""
    ind = getattr(ctx, "indicators", None) or {}
    if not isinstance(ind, dict):
        return None

    row: dict[str, float] = {}
    base_keys = [
        "ema21_slope_15m", "higher_low_30m", "vwap_z_15m",
        "market_breadth_ret_5m", "cg_rel_strength_btc_1h",
        "symbol_rel_strength_vs_btc_1m",
        "btc_ret_5m", "btc_ret_1m", "btc_ret_1h",
        "spread_bps", "lob_obi_5",
        "delta_z", "confidence_pct",
        "vol_regime_code",
    ]
    for k in base_keys:
        v = ind.get(k)
        try:
            row[k] = float(v) if v is not None else float("nan")
        except Exception:
            row[k] = float("nan")

    # opened_hour_utc — best-effort
    try:
        import time as _t
        row["opened_hour_utc"] = float(_t.gmtime().tm_hour)
    except Exception:
        row["opened_hour_utc"] = 0.0

    kinds = ["iceberg", "delta_spike", "absorption", "of", "ok"]
    klc = (kind or "").strip().lower()
    for k in kinds:
        row[f"kind_{k}"] = 1.0 if klc == k else 0.0
    regimes = ["trending_bull", "trending_bear", "range", "squeeze", "expansion", "mixed", "na"]
    reg = str(ind.get("regime") or "").strip().lower() or "na"
    for r in regimes:
        row[f"regime_{r}"] = 1.0 if reg == r else 0.0
    return row


def predict_long_win_proba(ctx: Any, *, kind: str = "") -> float | None:
    """Return calibrated P(win | features) for a LONG signal, or None if unavailable.

    Fail-open: any exception → None (gate then bypasses LGBM check).
    """
    bundle = _load_model_bundle()
    if bundle is None:
        return None
    row = _build_feature_row(ctx, kind=kind)
    if row is None:
        return None
    try:
        import pandas as pd  # type: ignore
        feat_names = bundle.get("feature_names") or list(row.keys())
        X = pd.DataFrame([{k: row.get(k, float("nan")) for k in feat_names}])
        model = bundle["model"]
        iso = bundle.get("isotonic")
        raw_proba = float(model.predict_proba(X)[:, 1][0])
        if iso is not None:
            cal_proba = float(iso.transform([raw_proba])[0])
        else:
            cal_proba = raw_proba
        return cal_proba
    except Exception as e:
        logger.debug("lgbm_reader: predict fail: %s", e)
        return None


def get_p_min() -> float:
    _refresh_snapshot()
    try:
        v = float(_SNAPSHOT.get("p_min") or 0.5)
        return max(0.0, min(1.0, v))
    except Exception:
        return 0.5


def get_mode() -> str:
    _refresh_snapshot()
    return str(_SNAPSHOT.get("mode") or "SHADOW").upper().strip()
