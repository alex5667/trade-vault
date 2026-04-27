
import warnings
import logging
logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

SCHEMA_HASH = "81dec493efe5"



def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _get(indicators: Dict[str, Any], row: Dict[str, Any], key: str, default: Any = None) -> Any:
    if key in indicators:
        return indicators.get(key, default)
    return row.get(key, default)


@dataclass(frozen=True)
class MLFeatureSchemaV4:
    """
    Stable feature schema v4.

    Adds first-class confirmation-derived binary features:
      - rsi_agree
      - div_match
      - sweep_eqh / sweep_eql

    (v3 remains unchanged for backward compatibility with existing models.)
    """

    # ---- feature groups ----
    float_keys: Tuple[str, ...] = (
        "price_z",
        "cvd_z",
        "delta_z",
        "of_imbalance",
        "obi",
        "obi_slope",
        "iceberg_score",
        "abs_strength",
        "hidden_ctx_score",
        "cont_ctx_score",
        "regime_score",
        "macro_bias_conf",
        "micro_structure_conf",
        "fp_edge_absorb_strength",
    )

    int_keys: Tuple[str, ...] = (
        "sweep_age_ms",
        "reclaim_age_ms",
        "abs_age_ms",
        "hidden_ctx_age_ms",
        "cont_ctx_age_ms",
        "fp_edge_age_ms",
    )

    bool_keys: Tuple[str, ...] = (
        "ofi_stable",
        "ofi_dir_ok",
        "obi_stable",
        "iceberg_strict",
        "fp_edge_absorb",
        "abs_lvl_ok",
        "reclaim_recent",
        "sweep_recent",
        "sweep_eqh",
        "sweep_eql",
        "rsi_agree",
        "div_match",
        "cancel_spike_veto",
    )

    cat_keys: Tuple[str, ...] = (
        "direction",
        "regime_group",
        "macro_bias",
        "micro_structure",
    )

    def feature_names(self) -> List[str]:
        out: List[str] = []
        out += list(self.float_keys)
        out += list(self.int_keys)
        out += [f"b:{k}" for k in self.bool_keys]
        out += [f"c:{k}" for k in self.cat_keys]
        return out

    def vectorize(self, row: Dict[str, Any]) -> List[float]:
        indicators = dict(row.get("indicators") or {})

        confirmations = row.get("confirmations")
        if confirmations:
            try:
                from core.confirmations_schema_v1 import parse_confirmations_v1, _CANON_MAP
                conf_map = parse_confirmations_v1(confirmations, indicators)
                for k, v in conf_map.items():
                    orig_k = _CANON_MAP.get(k)
                    if orig_k:
                        indicators[orig_k] = v
                        if orig_k == "sweep_any":
                            indicators["sweep_recent"] = v
            except Exception:
                logger.debug("Exception in scenario loop")

        feats: List[float] = []
        for k in self.float_keys:
            feats.append(_f(_get(indicators, row, k, 0.0), 0.0))
        for k in self.int_keys:
            feats.append(float(_i(_get(indicators, row, k, -1), -1)))
        for k in self.bool_keys:
            feats.append(float(1 if _i(_get(indicators, row, k, 0), 0) else 0))
        # Categorical keys are encoded later (one-hot / hashing) in downstream pipeline.
        for k in self.cat_keys:
            _ = _get(indicators, row, k, "")
            feats.append(0.0)
        return feats
