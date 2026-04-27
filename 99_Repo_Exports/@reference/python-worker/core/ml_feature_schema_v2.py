from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from core.bucket_utils import bucket_from_scenario


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _b(x: Any) -> float:
    # bool-ish to 0/1
    try:
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        if isinstance(x, (int, float)):
            return 1.0 if float(x) != 0.0 else 0.0
        s = str(x).strip().lower()
        if s in ("1", "true", "yes", "y", "ok"):
            return 1.0
        return 0.0
    except Exception:
        return 0.0


@dataclass(frozen=True)
class MLFeatureSchemaV2:
    """Explicit stable feature schema.

    Design goals:
    - Deterministic feature ordering.
    - Works with OFInputsV1-like records + indicators dict.
    - Safe defaults (missing -> 0).
    - Minimal transforms only; model learns interactions.
    """

    num_keys: Tuple[str, ...] = (
        # flow / microstructure
        "delta_z",
        "ofi_z",
        "ofi_stability_score",
        "obi",
        "obi_z",
        # execution risk
        "spread_bps",
        "expected_slippage_bps",
        "exec_risk_norm",
        "liq_score",
        "book_staleness_ms",
        # safety/pressure
        "pressure",
        "triggers_per_min",
        # rule gate summary (lets ML learn when rule gate is weak)
        "rule_score",
        "rule_have",
        "rule_need",
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
        names = []
        names.extend([f"n:{k}" for k in self.num_keys])
        names.extend([f"b:{k}" for k in self.bool_keys])
        names.extend(["dir:LONG", "dir:SHORT"])
        names.extend(["bucket:trend", "bucket:range", "bucket:other"])
        return names

    def _get(self, indicators: Dict[str, Any], root: Dict[str, Any], k: str) -> Any:
        if k in indicators:
            return indicators.get(k)
        return root.get(k)

    def vectorize(self, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                  indicators: Dict[str, Any], rule_score: float, rule_have: int,
                  rule_need: int, cancel_spike_veto: int) -> List[float]:
        # Build a "root" context to make training and inference symmetric.
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

        # direction one-hot
        d = (direction or "").upper()
        x.append(1.0 if d == "LONG" else 0.0)
        x.append(1.0 if d == "SHORT" else 0.0)

        # bucket one-hot from scenario
        bkt = bucket_from_scenario(scenario)
        x.append(1.0 if bkt == "trend" else 0.0)
        x.append(1.0 if bkt == "range" else 0.0)
        x.append(1.0 if bkt not in ("trend", "range") else 0.0)

        return x

    def vectorize_row(self, row: Dict[str, Any]) -> List[float]:
        # row must contain: symbol, ts_ms, direction, scenario_v4, indicators (dict), plus rule_* if present
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
