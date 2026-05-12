from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


import datetime as _dt
import math
import os
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

def _schema_ver_from_env() -> int:
    """Feature schema version (train==serve contract).

    Default is 1 for backward compatibility with existing deployed models.
    V3 adds continuation-context quality features.
    """
    try:
        v = int((os.getenv("ML_FEATURE_SCHEMA_VERSION", "1") or "1").strip())
    except Exception:
        v = 1
    if v >= 3:
        return 3
    return 2 if v == 2 else 1


# NOTE: Keep ordering stable within a schema version.
# V2 appends confirmation features at the end (train==serve upgrade path).
FEATURES_V1: list[FeatureSpec] = [
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

# V2 adds first-class confirmations (Stage 4).
FEATURES_V2: list[FeatureSpec] = FEATURES_V1 + [
    FeatureSpec("rsi_agree", 0.0),
    FeatureSpec("div_match", 0.0),
    FeatureSpec("sweep_any", 0.0),
    FeatureSpec("sweep_eqh", 0.0),
    FeatureSpec("sweep_eql", 0.0),
]

# V3 adds continuation-context quality features.
# IMPORTANT: cont_ctx_recent is intentionally EXCLUDED because it depends on
# the calibrated cont_ctx_valid_ms parameter → train≠serve drift risk.
# cont_ctx_age_ms is the raw numeric value that lets the model learn the
# optimal threshold automatically.
FEATURES_V3: list[FeatureSpec] = FEATURES_V2 + [
    # Continuation context quality (non-zero only for scenario=continuation)
    FeatureSpec("cont_ctx_age_ms", 0.0),
    FeatureSpec("hidden_ctx_recent", 0.0),
    # Trend direction source one-hots (how trend_dir was determined)
    FeatureSpec("trend_src_hidden_div", 0.0),
    FeatureSpec("trend_src_regime", 0.0),
    FeatureSpec("trend_src_dz_bypass", 0.0),
]


def _feats_for_ver(sv: int) -> list[FeatureSpec]:
    if sv >= 3:
        return FEATURES_V3
    if sv == 2:
        return FEATURES_V2
    return FEATURES_V1


def feature_names(schema_ver: int | None = None) -> list[str]:
    sv = _schema_ver_from_env() if schema_ver is None else max(1, min(3, int(schema_ver)))
    return [f.name for f in _feats_for_ver(sv)]


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
    schema_ver: int | None = None,
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

    if schema_ver in ("v5", "v5_of"):
        from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF
        schema = MLFeatureSchemaV5OF()
        vec = schema.vectorize(
            ts_ms=ts_ms, direction=direction, scenario=scenario,
            indicators=indicators, cancel_spike_veto=bool(cancel_spike_veto)
        )
        return vec, []
    elif schema_ver in ("v5_stable", "v5_of_stable"):
        from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OFStable
        schema = MLFeatureSchemaV5OFStable()
        vec = schema.vectorize(
            ts_ms=ts_ms, direction=direction, scenario=scenario,
            indicators=indicators, cancel_spike_veto=bool(cancel_spike_veto)
        )
        return vec, []
    elif schema_ver in ("v4", "v4_of"):
        from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
        schema = MLFeatureSchemaV4OF()
        vec = schema.vectorize(
            ts_ms=ts_ms, direction=direction, scenario=scenario,
            indicators=indicators, cancel_spike_veto=bool(cancel_spike_veto)
        )
        return vec, []

    # -----------------------------------------------------------------
    # V2 confirmations (Stage 4): first-class binary signals for ML.
    # NOTE: these are appended to the end of the schema to preserve
    # V1 compatibility for already-deployed models.
    # -----------------------------------------------------------------
    try:
        sv_int = int(schema_ver) if schema_ver is not None else _schema_ver_from_env()
    except ValueError:
        sv_int = 3
    sv = max(1, min(3, sv_int))

    def _need_bin(k: str) -> float:
        v = indicators.get(k)
        if v is None:
            missing.append(k)
            return 0.0
        return float(_i01(v))

    if sv >= 2:
        # rsi/div confirmations
        out["rsi_agree"] = _need_bin("rsi_agree")
        out["div_match"] = _need_bin("div_match")

        # sweep_any: OR across all sweep sources (any explicit flag wins)
        v_any = indicators.get("sweep_any")
        v_recent = indicators.get("sweep_recent")
        v_eqh = indicators.get("sweep_eqh")
        v_eql = indicators.get("sweep_eql")

        if (v_any is None) and (v_recent is None) and (v_eqh is None) and (v_eql is None):
            missing.append("sweep_any")
            out["sweep_any"] = 0.0
        else:
            out["sweep_any"] = float(1 if (_i01(v_any) or _i01(v_recent) or _i01(v_eqh) or _i01(v_eql)) else 0)

        # sweep_eqh/eql
        out["sweep_eqh"] = _need_bin("sweep_eqh")
        out["sweep_eql"] = _need_bin("sweep_eql")

    # -----------------------------------------------------------------
    # V3: Continuation-context quality features.
    # cont_ctx_age_ms: raw numeric (model learns optimal threshold).
    # hidden_ctx_recent: stable binary (not dependent on calibrated params).
    # trend_src_*: one-hots for how trend_dir was determined.
    # IMPORTANT: cont_ctx_recent is intentionally excluded (train≠serve drift).
    # -----------------------------------------------------------------
    if sv >= 3:
        out["cont_ctx_age_ms"] = need_num("cont_ctx_age_ms", 0.0)
        out["hidden_ctx_recent"] = _need_bin("hidden_ctx_recent")

        # trend_dir_source one-hots
        tds = (indicators.get("trend_dir_source", "none") or "none").lower()
        out["trend_src_hidden_div"] = 1.0 if tds == "hidden_div" else 0.0
        out["trend_src_regime"] = 1.0 if tds == "regime" else 0.0
        out["trend_src_dz_bypass"] = 1.0 if int(indicators.get("scenario_dz_bypass", 0) or 0) == 1 else 0.0

    # finalize in schema order
    vec: list[float] = []
    feats = _feats_for_ver(sv)
    for fs in feats:
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

    def _parse_confirmations_obj(obj: Any) -> dict[str, Any]:
        """Accept dict OR legacy list[str] like ["rsi_agree=1", ...]."""
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, (list, tuple)):
            out2: dict[str, Any] = {}
            for it in obj:
                if it is None:
                    continue
                s = str(it).strip()
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                kk = str(k).strip().lower().replace("-", "_").replace(" ", "_")
                vv = str(v).strip()
                # best-effort bool/numeric
                if vv.lower() in ("true", "false"):
                    out2[kk] = 1 if vv.lower() == "true" else 0
                    continue
                try:
                    out2[kk] = float(vv) if ("." in vv or "e" in vv.lower()) else int(vv)
                except Exception:
                    # keep raw string; downstream _i01 handles it (or treats missing)
                    out2[kk] = vv
            return out2
        return {}

    ind = payload.get("indicators") or {}
    conf = _parse_confirmations_obj(payload.get("confirmations"))

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

