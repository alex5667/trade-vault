from __future__ import annotations
import hashlib
from typing import Any, Dict, List, Tuple

from core.meta_features_v6 import (
    META_FEAT_V6_COLS,
    META_FEAT_V6_TRANSFORMS,
    build_meta_features_v6,
)

META_FEAT_V7_NAME = "meta_feat_v7"
META_FEAT_V7_VERSION = 7

META_FEAT_V7_NEW_COLS: List[str] = [
    "conf_rsi_agree",
    "conf_div_match",
    "conf_div_match_fallback",
    "conf_sweep_eqh",
    "conf_sweep_eql",
    "conf_sweep_any",
    "conf_iceberg_strict",
    "conf_obi_stable",
    "conf_reclaim",
    "conf_weak_progress",
]

META_FEAT_V7_COLS: List[str] = list(META_FEAT_V6_COLS) + META_FEAT_V7_NEW_COLS
META_FEAT_V7_HASH: str = hashlib.sha1(",".join(META_FEAT_V7_COLS).encode("utf-8")).hexdigest()

META_FEAT_V7_TRANSFORMS = dict(META_FEAT_V6_TRANSFORMS)
for k in META_FEAT_V7_NEW_COLS:
    META_FEAT_V7_TRANSFORMS.setdefault(k, {"type": "clip", "lo": 0.0, "hi": 1.0})


def build_meta_features_v7(
    evidence: Dict[str, Any],
    indicators: Dict[str, Any],
    **kwargs,
) -> Tuple[Dict[str, float], List[str]]:
    feat, missing = build_meta_features_v6(evidence=evidence, indicators=indicators, **kwargs)

    for col in META_FEAT_V7_NEW_COLS:
        v = 0.0
        if isinstance(indicators, dict) and col in indicators:
            val = indicators.get(col)
            if val is not None: v = max(v, float(val))
            
        if isinstance(evidence, dict) and col in evidence:
            val = evidence.get(col)
            if val is not None: v = max(v, float(val))
            
        if isinstance(evidence, dict) and "confirmations" in evidence:
            # Fallback to string parsing for conf_ features
            conf_str = col.replace("conf_", "") + "=1"
            if conf_str in evidence.get("confirmations", []) or f"{col.replace('conf_', '')}=1.0" in evidence.get("confirmations", []):
                v = max(v, 1.0)

        if v == 0.0 and col not in indicators and (not isinstance(evidence, dict) or col not in evidence):
            feat[col] = 0.0
            missing.append(col)
        else:
            try:
                feat[col] = float(v)
            except Exception:
                feat[col] = 0.0
                missing.append(col)

    return feat, missing
