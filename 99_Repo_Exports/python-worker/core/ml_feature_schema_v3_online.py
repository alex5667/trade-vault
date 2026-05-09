from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


"""ML Feature Schema V3 Online — serving-safe, no outcome leakage.

Motivation:
  MLFeatureSchemaV3 contains mae_r / mfe_r which are outcome fields
  (set only after trade close) — they must NOT be used in online serving.
  This schema removes them and keeps only forward-looking features.

Total features: v2 numeric (15) + adverse/lambda (4) + session (24+7) + dir (2) + bucket (3) = 55

Activate via: ML_FEATURE_SCHEMA_VER=v3_online  (or v3o)
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.bucket_utils import bucket_from_scenario

SCHEMA_HASH = "bfd87957a8d2"



def _f(x: Any, d: float = 0.0) -> float:
    """Safe float cast; returns default on any error."""
    try:
        return float(x)
    except Exception:
        return d


def _b(x: Any) -> float:
    """Safe binary cast (0.0 or 1.0); returns 0.0 on any error."""
    try:
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        if isinstance(x, (int, float)):
            return 1.0 if float(x) != 0.0 else 0.0
        s = str(x).strip().lower()
        return 1.0 if s in ("1", "true", "yes", "y", "ok") else 0.0
    except Exception:
        return 0.0


def _utc_hour_dow(ts_ms: int) -> tuple[int, int]:
    """Return (hour_utc 0-23, weekday 0=Mon…6=Sun) from epoch-ms."""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
        return dt.hour, dt.weekday()
    except Exception:
        return 0, 0


@dataclass(frozen=True)
class MLFeatureSchemaV3Online:
    """Serving-safe schema: v2 + adverse/lambda + session (hour/dow).

    No outcome leakage — mae_r / mfe_r deliberately excluded.
    Compare to MLFeatureSchemaV3 which includes those training-only fields.
    """

    num_keys: tuple[str, ...] = (
        # v2 numeric (15) — ordered to match v3 positions for partial compat
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
        # v3_online extra adverse / hawkes (4)
        "adverse_proxy",
        "lambda_taker",
        "lambda_cancel",
        "lambda_spread_widen",
    )

    bool_keys: tuple[str, ...] = (
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

    def feature_names(self) -> list[str]:
        """Return stable ordered feature name list (training-time registry)."""
        names: list[str] = []
        names.extend([f"n:{k}" for k in self.num_keys])
        names.extend([f"b:{k}" for k in self.bool_keys])
        names.extend(["dir:LONG", "dir:SHORT"])
        names.extend(["bucket:trend", "bucket:range", "bucket:other"])
        names.extend([f"hour:{h}" for h in range(24)])
        names.extend([f"dow:{d}" for d in range(7)])
        return names

    def _get(self, indicators: dict[str, Any], root: dict[str, Any], k: str) -> Any:
        """Look up key from indicators first, then root (rule_score etc.)."""
        if k in indicators:
            return indicators.get(k)
        return root.get(k)

    def vectorize(
        self,
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
    ) -> list[float]:
        """Vectorize one observation into a float list aligned with feature_names()."""
        root: dict[str, Any] = {
            "symbol": symbol,
            "ts_ms": ts_ms,
            "direction": direction,
            "scenario_v4": scenario,
            "rule_score": rule_score,
            "rule_have": rule_have,
            "rule_need": rule_need,
            "cancel_spike_veto": cancel_spike_veto,
        }

        x: list[float] = []
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

    def vectorize_row(self, row: dict[str, Any]) -> list[float]:
        """Vectorize from a flat dict (offline dataset row / replay)."""
        indicators = row.get("indicators") or {}
        if not isinstance(indicators, dict):
            indicators = {}
        return self.vectorize(
            symbol=(row.get("symbol", "") or ""),
            ts_ms=int(_f(row.get("ts_ms", 0), 0)),
            direction=(row.get("direction", "") or ""),
            scenario=(row.get("scenario_v4", row.get("scenario", "")) or ""),
            indicators=indicators,
            rule_score=_f(row.get("rule_score", indicators.get("rule_score", 0.0)), 0.0),
            rule_have=int(_f(row.get("rule_have", indicators.get("rule_have", 0)), 0)),
            rule_need=int(_f(row.get("rule_need", indicators.get("rule_need", 0)), 0)),
            cancel_spike_veto=int(
                _f(row.get("cancel_spike_veto", indicators.get("cancel_spike_veto", 0)), 0)
            ),
        )
