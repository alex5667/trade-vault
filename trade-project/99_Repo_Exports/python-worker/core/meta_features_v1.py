from __future__ import annotations

import hashlib
import math
from typing import Any

META_FEAT_V1_NAME = "meta_feat_v1"
META_FEAT_V1_VERSION = 1

# Fixed list of columns for Train == Serve consistency (canonical inventory).
META_FEAT_V1_COLS = [
    "have",
    "need",
    "ok_soft",
    "rule_score",
    "exec_risk_norm",
    "exec_risk_bps",
    "age_ms",
    "is_weekend",
    "is_eu_hours",
    "is_us_hours",
    "is_asia_hours",
    "lag1_pnl_15m",
    "lag1_win_15m",
    "lag1_pnl_1h",
    "lag1_win_1h",
    "lag1_pnl_4h",
    "lag1_win_4h",
    "lag1_pnl_24h",
    "lag1_win_24h",
    "lag2_pnl_15m",
    "lag2_win_15m",
    "lag2_pnl_1h",
    "lag2_win_1h",
    "lag2_pnl_4h",
    "lag2_win_4h",
    "lag2_pnl_24h",
    "lag2_win_24h",
    "spread_bps",
    "volatility_15m_bps",
    "ofi_15m",
    "book_churn_15m",
    "liq_imbal_15m",
    "trade_imbal_15m",
    "volatility_1h_bps",
    "ofi_1h",
    "book_churn_1h",
    "liq_imbal_1h",
    "trade_imbal_1h",
    "volatility_4h_bps",
    "ofi_4h",
    "book_churn_4h",
    "liq_imbal_4h",
    "trade_imbal_4h",
    "volatility_24h_bps",
    "ofi_24h",
    "book_churn_24h",
    "liq_imbal_24h",
    "trade_imbal_24h",
    "delta_z",
    "obi_z",
    "ofi_z",
    "scn_is_trend",
    "scn_is_range",
]

META_FEAT_V1_HASH = hashlib.sha256(
    (",".join(META_FEAT_V1_COLS)).encode("utf-8")
).hexdigest()[:16]

# Default transforms for meta_feat_v1.
# Stored in the model JSON and applied at runtime by MetaModelLR.
META_FEAT_V1_TRANSFORMS: dict[str, dict[str, Any]] = {
    # Ages / staleness (ms): log1p(max(0, x))
    "age_ms": {"type": "log1p"},

    # z-scores: clip
    "delta_z": {"type": "clip", "lo": -8.0, "hi": 8.0},
    "obi_z": {"type": "clip", "lo": -8.0, "hi": 8.0},
    "ofi_z": {"type": "clip", "lo": -8.0, "hi": 8.0},

    # bps-like: clip
    "spread_bps": {"type": "clip", "lo": 0.0, "hi": 500.0},
    "exec_risk_bps": {"type": "clip", "lo": 0.0, "hi": 2000.0},
}

def build_meta_features_v1(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    indicators_with_v4: dict[str, Any] | None = None,
    legs: dict[str, Any] | None = None,
    have: int = 0,
    need: int = 0,
    ok_soft: int = 0,
    rule_score: float = 0.0,
    exec_risk_norm: float = 0.0,
    exec_risk_bps: float = 0.0,
    ml_scenario: str = "",
) -> tuple[dict[str, float], list[str]]:
    """
    Builds a flat feature dictionary for MetaModelLR.
    Standardized across training/serving for Train == Serve parity.
    """
    feat: dict[str, float] = {}
    missing: list[str] = []

    def _get(src: dict[str, Any] | None, k: str) -> tuple[bool, Any]:
        if not src: return False, None
        return k in src, src.get(k)

    def _to_float(v: Any, default: float = 0.0) -> float:
        try:
            val = float(v) if v is not None else default
            return val if math.isfinite(val) else default
        except (ValueError, TypeError):
            return default

    def _is_finite(v: float) -> bool:
        return math.isfinite(v)

    def _age(src: dict[str, Any] | None, k: str, miss_name: str) -> float:
        present, val = _get(src, k)
        v = _to_float(val, 0.0)
        bad = (not present) or (val is None) or (not _is_finite(v)) or (v < 0.0)
        if bad:
            missing.append(miss_name)
            return 0.0
        return float(v)

    # Simple scalar fields
    feat["have"] = float(have)
    feat["need"] = float(need)
    feat["ok_soft"] = float(ok_soft)
    feat["rule_score"] = float(rule_score)
    feat["exec_risk_norm"] = float(exec_risk_norm)
    feat["exec_risk_bps"] = float(exec_risk_bps)

    # Scenario (Unified)
    scn = (ml_scenario or "").lower()
    if not scn and indicators_with_v4:
        scn = (indicators_with_v4.get("scenario_v4", "")).lower()

    feat["scn_is_trend"] = 1.0 if "trend" in scn else 0.0
    feat["scn_is_range"] = 1.0 if "range" in scn else 0.0

    # Age / Context
    meta_ctx = evidence.get("meta_context", {}) if isinstance(evidence, dict) else {}
    feat["age_ms"] = _age(evidence, "age_ms", "age_ms") if "age_ms" in evidence else _age(meta_ctx, "age_ms", "age_ms")

    feat["is_weekend"] = _to_float(meta_ctx.get("is_weekend", 0.0))
    feat["is_eu_hours"] = _to_float(meta_ctx.get("is_eu_hours", 0.0))
    feat["is_us_hours"] = _to_float(meta_ctx.get("is_us_hours", 0.0))
    feat["is_asia_hours"] = _to_float(meta_ctx.get("is_asia_hours", 0.0))

    # Lags
    for lag in [1, 2]:
        for win in ["15m", "1h", "4h", "24h"]:
            for suffix in ["pnl", "win"]:
                k = f"lag{lag}_{suffix}_{win}"
                present, val = _get(indicators, k)
                feat[k] = _to_float(val)
                if not present:
                    missing.append(k)

    # Market State
    feat["spread_bps"] = _to_float(indicators.get("spread_bps"))
    if "spread_bps" not in indicators: missing.append("spread_bps")

    for w in ["15m", "1h", "4h", "24h"]:
        k_vol = f"volatility_{w}_bps"
        feat[k_vol] = _to_float(indicators.get(k_vol))
        if k_vol not in indicators: missing.append(k_vol)

        for prefix in ["ofi", "book_churn", "liq_imbal", "trade_imbal"]:
            k = f"{prefix}_{w}"
            feat[k] = _to_float(indicators.get(k))
            if k not in indicators: missing.append(k)

    # Specialized (legs/z-scores)
    for k in ["delta_z", "obi_z", "ofi_z"]:
        # Try indicators_with_v4 first, then legs, then evidence
        found = False
        for src in [indicators_with_v4, legs, evidence]:
            present, val = _get(src, k)
            if present:
                feat[k] = _to_float(val)
                found = True
                break
        if not found:
            feat[k] = 0.0
            missing.append(k)

    return feat, missing
