import time
from typing import Any

from core.feature_engineering import (
    RobustScalerPack,
    apply_transform,
    bucketize,
    derive_regime_label,
    derive_session_label,
)


def _f(x: Any, default: float) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default

def _scenario_norm(s: str) -> str:
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


def build_feature_row(
    model: Any,
    indicators: dict[str, Any],
    direction: str,
    scenario: str,
    ts_ms: int,
    forbid_scenario_v4_onehot: bool = False
) -> tuple[list[float], list[str]]:
    """Build feature row for model inference."""
    feature_cols: list[str] = list(getattr(model, "feature_cols", []) or [])
    missing: list[str] = []

    # critical inputs (accuracy/safety)
    critical = ["spread_bps", "expected_slippage_bps"]
    for k in critical:
        if k not in indicators:
            missing.append(k)

    if "exec_risk_norm" not in indicators:
        missing.append("exec_risk_norm")

    d = (direction or "").upper()
    s = _scenario_norm(scenario)

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

    tm = time.gmtime(float(int(ts_ms or 0)) / 1000.0)
    utc_hour = int(getattr(tm, "tm_hour", 0))
    utc_dow = int(getattr(tm, "tm_wday", 0))
    bucket = _bucket_from_scenario(s) or "other"

    try:
        bucket2 = (indicators.get("bucket2") or "").strip().lower()
    except Exception:
        bucket2 = ""
    if not bucket2:
        try:
            from core.bucket2_v1 import derive_bucket2_label
            bucket2 = str(derive_bucket2_label(s, indicators=indicators) or "").strip().lower()
        except Exception:
            bucket2 = ""

    cache: dict[str, float] = {}

    def num(name: str) -> float:
        if name in cache:
            return cache[name]
        x = _f(indicators.get(name, 0.0), 0.0)
        if transforms and name in transforms:
            x = apply_transform(float(x), transforms.get(name))
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
            if forbid_scenario_v4_onehot:
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
