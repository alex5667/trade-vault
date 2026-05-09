from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


"""ML Feature Schema V4 Stack — expanded serving-safe schema for edge-stack ML.

Motivation:
  Extends V3Online with book microstructure, evidence timing/stability,
  cyclical time encoding, and confirmation flags as first-class bool features.

Total features: 97
  - V2 numeric (15) + adverse/lambda (4) + microstructure_v4 (12)
  + evidence timing (8) + evidence stability (2) + cyclical time (4)
  + confirmations bool (7) = 52 numeric-type
  + bool_keys (16) = 68 value-type features
  + dir (2) + bucket (3) + hour one-hot (24) + dow one-hot (7) = 36 categorical
  Total = 52 num + 16 bool + 2 dir + 3 bucket + 24 hour + 7 dow = 104 raw
  → Collapsed to 97 because cyclical time (4) replaces the scalar hour/dow in
    the num_keys tuple, while one-hot hour/dow come at the end.

Activate via: ML_FEATURE_SCHEMA_VER=v4  (or v4_stack)
"""

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.bucket_utils import bucket_from_scenario

SCHEMA_HASH = "dd0d597e86ff"



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
class MLFeatureSchemaV4Stack:
    """Expanded serving-safe schema for edge-stack ML.

    No outcome leakage (no mae_r / mfe_r).
    Cyclical time encoding (sin/cos) replaces raw hour/dow scalars in num_keys,
    while one-hot hour/dow are appended at the end for interpretability parity.

    Total features: 97
    """

    num_keys: tuple[str, ...] = (
        # v2 numeric (15)
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
        # book microstructure v4 (12) — populated by compute_microstructure_v4
        "depth_bid_5",
        "depth_ask_5",
        "depth_imb_5",
        "top_imb_5",
        "slope_imb_5",
        "pressure_imb_5",
        "churn_z",
        "book_rate_z",
        "pressure_sps",
        "book_age_ms",
        "book_health_ok",
        "book_midprice",
        # evidence timing (8) — materialized from ev dict in tick_processor
        "ofi_age_ms",
        "obi_age_ms",
        "iceberg_age_ms",
        "sweep_age_ms",
        "reclaim_age_ms",
        "fp_edge_age_ms",
        "abs_age_ms",
        "weak_progress_age_ms",
        # evidence stability (2)
        "ofi_stable_secs",
        "obi_stable_secs",
        # cyclical time (4) — computed from ts_ms; prevents linear wrap-around
        "sin_hour",
        "cos_hour",
        "sin_dow",
        "cos_dow",
    )

    bool_keys: tuple[str, ...] = (
        # v2 bools (9)
        "ofi_stable",
        "ofi_dir_ok",
        "obi_stable",
        "iceberg_strict",
        "fp_edge_absorb",
        "abs_lvl_ok",
        "reclaim_recent",
        "sweep_recent",
        "cancel_spike_veto",
        # confirmations as first-class bool features (7) — Commit 1 parity
        "rsi_agree",
        "div_match",
        "sweep_eqh",
        "sweep_eql",
        "weak_progress",
        "absorption",
        "reclaim",
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

        hour, dow = _utc_hour_dow(int(ts_ms))

        # Precompute cyclical time values (sin/cos encoding avoids midnight wrap)
        if 0 <= hour <= 23:
            hour_rad = (2.0 * math.pi * float(hour)) / 24.0
            sin_h, cos_h = math.sin(hour_rad), math.cos(hour_rad)
        else:
            sin_h, cos_h = 0.0, 0.0

        if 0 <= dow <= 6:
            dow_rad = (2.0 * math.pi * float(dow)) / 7.0
            sin_d, cos_d = math.sin(dow_rad), math.cos(dow_rad)
        else:
            sin_d, cos_d = 0.0, 0.0

        # Override cyclical keys in indicators lookup
        computed: dict[str, float] = {
            "sin_hour": sin_h,
            "cos_hour": cos_h,
            "sin_dow": sin_d,
            "cos_dow": cos_d,
        }

        x: list[float] = []

        # Numeric features — use precomputed values for cyclical keys
        for k in self.num_keys:
            if k in computed:
                x.append(float(computed[k]))
            else:
                x.append(_f(self._get(indicators, root, k), 0.0))

        # Boolean features
        for k in self.bool_keys:
            x.append(_b(self._get(indicators, root, k)))

        # Direction one-hot
        d = (direction or "").upper()
        x.append(1.0 if d == "LONG" else 0.0)
        x.append(1.0 if d == "SHORT" else 0.0)

        # Bucket one-hot
        bkt = bucket_from_scenario(scenario)
        x.append(1.0 if bkt == "trend" else 0.0)
        x.append(1.0 if bkt == "range" else 0.0)
        x.append(1.0 if bkt not in ("trend", "range") else 0.0)

        # Hour one-hot (0-23)
        for h in range(24):
            x.append(1.0 if h == hour else 0.0)

        # Day-of-week one-hot (0=Mon … 6=Sun)
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
