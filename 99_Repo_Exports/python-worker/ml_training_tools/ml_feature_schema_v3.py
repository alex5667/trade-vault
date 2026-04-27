from __future__ import annotations

import warnings
import logging
logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

from core.bucket_utils import bucket_from_scenario

SCHEMA_HASH = "6a18f2c3fe97"



def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _b(x: Any) -> float:
    try:
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        if isinstance(x, (int, float)):
            return 1.0 if float(x) != 0.0 else 0.0
        s = str(x).strip().lower()
        return 1.0 if s in ("1", "true", "yes", "y", "ok") else 0.0
    except Exception:
        return 0.0


def _utc_hour_dow(ts_ms: int) -> Tuple[int, int]:
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.hour, dt.weekday()
    except Exception:
        return 0, 0


@dataclass(frozen=True)
class MLFeatureSchemaV3:
    """Stable schema v3 (extends v2).

    Adds session features (UTC hour + day-of-week one-hot) and optional outcome/hawkes fields.
    Missing fields -> 0.
    """

    num_keys: Tuple[str, ...] = (
        "delta_z",
        "ofi_z",
        "ofi_stability_score",
        "obi",
        "obi_z",
        "spread_bps",
        "expected_slippage_bps",
        "exec_risk_norm",
        "liq_score",
        "book_staleness_ms",
        "pressure",
        "triggers_per_min",
        "rule_score",
        "rule_have",
        "rule_need",
        "mae_r",
        "mfe_r",
        "adverse_proxy",
        "lambda_taker",
        "lambda_cancel",
        "lambda_spread_widen",
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
        "cancel_spike_veto",
    )

    def feature_names(self) -> List[str]:
        names: List[str] = []
        names.extend([f"n:{k}" for k in self.num_keys])
        names.extend([f"b:{k}" for k in self.bool_keys])
        names.extend(["dir:LONG", "dir:SHORT"])
        names.extend(["bucket:trend", "bucket:range", "bucket:other"])
        names.extend([f"hour:{h}" for h in range(24)])
        names.extend([f"dow:{d}" for d in range(7)])
        return names

    def _get(self, indicators: Dict[str, Any], root: Dict[str, Any], k: str) -> Any:
        if k in indicators:
            return indicators.get(k)
        return root.get(k)

    def vectorize(self, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                  indicators: Dict[str, Any], rule_score: float, rule_have: int,
                  rule_need: int, cancel_spike_veto: int) -> List[float]:
        root: Dict[str, Any] = {
            "symbol": symbol,
            "ts_ms": ts_ms,
            "direction": direction,
            "scenario_v4": scenario,
            "rule_score": rule_score,
            "rule_have": rule_have,
            "rule_need": rule_need,
            "cancel_spike_veto": cancel_spike_veto,
        }

        x: List[float] = []
        for k in self.num_keys:
            x.append(_f(self._get(indicators, root, k), 0.0))
        for k in self.bool_keys:
            x.append(_b(self._get(indicators, root, k)))

        d = (direction or "").upper()
        x.append(1.0 if d == "LONG" else 0.0)
        x.append(1.0 if d == "SHORT" else 0.0)

        bkt = bucket_from_scenario(scenario)
        x.append(1.0 if bkt == "trend" else 0.0)
        x.append(1.0 if bkt == "range" else 0.0)
        x.append(1.0 if bkt not in ("trend", "range") else 0.0)

        hour, dow = _utc_hour_dow(int(ts_ms))
        for h in range(24):
            x.append(1.0 if h == hour else 0.0)
        for dd in range(7):
            x.append(1.0 if dd == dow else 0.0)

        return x

    def vectorize_row(self, row: Dict[str, Any]) -> List[float]:
        indicators = row.get("indicators") or {}
        if not isinstance(indicators, dict):
            indicators = {}
        return self.vectorize(
            symbol=str(row.get("symbol", "") or ""),
            ts_ms=int(_f(row.get("ts_ms", 0), 0)),
            direction=str(row.get("direction", "") or ""),
            scenario=str(row.get("scenario_v4", row.get("scenario", "")) or ""),
            indicators=indicators,
            rule_score=_f(row.get("rule_score", indicators.get("rule_score", 0.0)), 0.0),
            rule_have=int(_f(row.get("rule_have", indicators.get("rule_have", 0)), 0)),
            rule_need=int(_f(row.get("rule_need", indicators.get("rule_need", 0)), 0)),
            cancel_spike_veto=int(_f(row.get("cancel_spike_veto", indicators.get("cancel_spike_veto", 0)), 0)),
        )

