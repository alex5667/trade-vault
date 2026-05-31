
from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


import datetime as _dt
import math
from dataclasses import dataclass
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i01(x: Any) -> int:
    try:
        if isinstance(x, bool):
            return 1 if x else 0
        return 1 if int(float(x)) != 0 else 0
    except Exception:
        return 0


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    default: float = 0.0


SCENARIOS = [
    "reversal",
    "continuation",
    "range_meanrev",
    "vol_shock_news_proxy",
    "saw_chop_spoof_proxy",
    "none",
]

# NOTE: Keep ordering stable. This list is the contract between training and inference.
FEATURES: list[FeatureSpec] = [
    # Direction / scenario one-hots
    FeatureSpec("dir_long", 0.0),

    FeatureSpec("sc_reversal", 0.0),
    FeatureSpec("sc_continuation", 0.0),
    FeatureSpec("sc_range_meanrev", 0.0),
    FeatureSpec("sc_vol_shock_news_proxy", 0.0),
    FeatureSpec("sc_saw_chop_spoof_proxy", 0.0),
    FeatureSpec("sc_none", 0.0),
    FeatureSpec("sc_other", 0.0),

    # Core OF / microstructure (numeric)
    FeatureSpec("delta_z", 0.0),
    FeatureSpec("ofi_z", 0.0),
    FeatureSpec("ofi", 0.0),
    FeatureSpec("ofi_stability_score", 0.0),
    FeatureSpec("exec_risk_norm", 0.0),
    FeatureSpec("spread_bps", 0.0),
    FeatureSpec("expected_slippage_bps", 0.0),
    FeatureSpec("liq_score", 0.0),

    # Hawkes-like burst features
    FeatureSpec("hawkes_taker_lam", 0.0),
    FeatureSpec("hawkes_cancel_lam", 0.0),
    FeatureSpec("hawkes_churn_lam", 0.0),

    # Rule gate summary (can help calibrate against the heuristic)
    FeatureSpec("rule_score", 0.0),
    FeatureSpec("rule_have", 0.0),
    FeatureSpec("rule_need", 0.0),

    # Binary flags (evidence legs)
    FeatureSpec("sweep_recent", 0.0),
    FeatureSpec("reclaim_recent", 0.0),
    FeatureSpec("obi_stable", 0.0),
    FeatureSpec("iceberg_strict", 0.0),
    FeatureSpec("abs_lvl_ok", 0.0),
    FeatureSpec("weak_progress", 0.0),
    FeatureSpec("fp_edge_absorb", 0.0),

    FeatureSpec("ofi_stable", 0.0),
    FeatureSpec("ofi_dir_ok", 0.0),

    FeatureSpec("cancel_spike_veto", 0.0),

    # Time features (UTC)
    FeatureSpec("hour_sin", 0.0),
    FeatureSpec("hour_cos", 0.0),
    FeatureSpec("dow_sin", 0.0),
    FeatureSpec("dow_cos", 0.0),
]


def feature_names() -> list[str]:
    return [f.name for f in FEATURES]


def build_feature_vector(
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
) -> tuple[list[float], list[str]]:
    """
    Deterministic feature builder. Must be stable across versions.

    Inputs:
      indicators: dict from strategy/of_confirm_engine (may contain many extra keys).
      rule_*: values from heuristic gate.

    Returns:
      (x, missing_features)
    """
    missing: list[str] = []
    out: dict[str, float] = {}

    # one-hots
    out["dir_long"] = 1.0 if str(direction).upper() == "LONG" else 0.0

    sc = str(scenario).lower()
    if sc in SCENARIOS:
        out[f"sc_{sc}"] = 1.0
        out["sc_other"] = 0.0
    else:
        out["sc_other"] = 1.0

    # numeric from indicators
    def need_num(k: str, default: float = 0.0) -> float:
        v = indicators.get(k)
        if v is None:
            missing.append(k)
            return default
        return _f(v, default)

    out["delta_z"] = need_num("delta_z", 0.0)
    out["ofi_z"] = need_num("ofi_z", 0.0)
    out["ofi"] = need_num("ofi", 0.0)
    out["ofi_stability_score"] = need_num("ofi_stability_score", 0.0)
    out["exec_risk_norm"] = need_num("exec_risk_norm", 0.0)
    out["spread_bps"] = need_num("spread_bps", 0.0)
    out["expected_slippage_bps"] = need_num("expected_slippage_bps", 0.0)
    out["liq_score"] = need_num("liq_score", 0.0)

    out["hawkes_taker_lam"] = need_num("hawkes_taker_lam", 0.0)
    out["hawkes_cancel_lam"] = need_num("hawkes_cancel_lam", 0.0)
    out["hawkes_churn_lam"] = need_num("hawkes_churn_lam", 0.0)

    # heuristic gate summary
    out["rule_score"] = float(rule_score)
    out["rule_have"] = float(rule_have)
    out["rule_need"] = float(rule_need)

    # binary flags: take from indicators if present, else missing->0
    for k in ["sweep_recent","reclaim_recent","obi_stable","iceberg_strict","abs_lvl_ok","weak_progress","fp_edge_absorb",
              "ofi_stable","ofi_dir_ok"]:
        v = indicators.get(k)
        if v is None:
            missing.append(k)
            out[k] = 0.0
        else:
            out[k] = float(_i01(v))

    out["cancel_spike_veto"] = float(int(cancel_spike_veto))

    # UTC time features
    try:
        dt = _dt.datetime.fromtimestamp(int(ts_ms)//1000, tz=_dt.timezone.utc)
        hour = dt.hour + dt.minute/60.0
        dow = dt.weekday()  # 0..6
        out["hour_sin"] = math.sin(2.0*math.pi*hour/24.0)
        out["hour_cos"] = math.cos(2.0*math.pi*hour/24.0)
        out["dow_sin"] = math.sin(2.0*math.pi*dow/7.0)
        out["dow_cos"] = math.cos(2.0*math.pi*dow/7.0)
    except Exception:
        missing.extend(["hour_sin","hour_cos","dow_sin","dow_cos"])

    # finalize in schema order
    vec: list[float] = []
    for fs in FEATURES:
        vec.append(float(out.get(fs.name, fs.default)))
    return vec, missing


# Additional function for nightly pipeline compatibility
def build_features(payload: dict[str, Any]) -> Any:
    """
    payload: dict from signals:of:inputs
    - numeric features from top-level and/or indicators
    - binary legs from indicators/confirmations
    Compatible with nightly pipeline FeatureRow format.
    """
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class FeatureRow:
        x: list[float]
        feature_names: list[str]

    ind = payload.get("indicators") or {}
    conf = payload.get("confirmations") or {}

    # Use existing build_feature_vector for consistency
    vec, missing = build_feature_vector(
        symbol=(payload.get("symbol", "")),
        ts_ms=int(payload.get("ts_ms", 0) or 0),
        direction=(payload.get("direction", "")),
        scenario=(payload.get("scenario", payload.get("scenario_v4", "none")) or "none"),
        indicators=dict(ind, **conf),
        rule_score=_f(payload.get("rule_score", ind.get("of_score", 0.0)), 0.0),
        rule_have=int(payload.get("rule_have", ind.get("strong_gate_have", 0)) or 0),
        rule_need=int(payload.get("rule_need", ind.get("strong_gate_need", 0)) or 0),
        cancel_spike_veto=int(payload.get("cancel_spike_veto", ind.get("cancel_spike_veto", 0)) or 0),
    )

    return FeatureRow(x=vec, feature_names=feature_names())

