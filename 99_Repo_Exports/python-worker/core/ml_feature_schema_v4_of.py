from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)
msg = "This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)


from dataclasses import dataclass
from typing import Any

SCHEMA_HASH = "a548bfe6a931"



def _get_num(indicators: dict[str, Any], key: str) -> float:
    v = indicators.get(key, 0.0)
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _get_bool(indicators: dict[str, Any], key: str) -> float:
    v = indicators.get(key, 0.0)
    try:
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if v is None:
            return 0.0
        return 1.0 if float(v) != 0.0 else 0.0
    except Exception:
        return 0.0


@dataclass
class MLFeatureSchemaV4OF:
    """Online-only schema: extends v3 with microstructure/health/confirmations.

    NOTE:
      mae_r/mfe_r are included for backward-compat with v3 and are expected
      to come from *previous* closed-trade calibration (not the current trade
      outcome). If your data source cannot guarantee that, set them to 0.
    """

    # numeric keys (read from indicators without the `n:` prefix)
    num_keys: list[str] = None  # type: ignore
    # boolean keys (read from indicators without the `b:` prefix)
    bool_keys: list[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.num_keys is None:
            self.num_keys = [
                # v2 core (15)
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
                # v3 backward-compat: mae_r/mfe_r from *previous* closed trades only
                "mae_r",
                "mfe_r",
                # v3 online-friendly extras (keep if present)
                "adverse_proxy",
                "lambda_taker",
                "lambda_cancel",
                "lambda_spread_widen",
                # new: book microstructure v4 (computed from LOB top-5)
                "mp_mid_bps",
                "mp_shift_bps",
                "depth_bid_5",
                "depth_ask_5",
                "book_slope_bid",
                "book_slope_ask",
                "book_convex_bid",
                "book_convex_ask",
                "obi_dw",
                # new: book dynamics / data quality
                "book_rate_hz",
                "book_rate_z",
                "book_churn_score",
                "cancel_spike_score",
                "data_health",
                # new: regime / risk / resilience (cheap, already exported by tick_processor)
                "vol_fast_bps",
                "vol_slow_bps",
                "res_curr_ratio",
                "res_recovery_ms",
                "res_speed_per_s",
                "atr_bps",
                "atr_age_ms",
                # new: microbar & oscillators
                "rsi_price",
                "rsi_cvd",
                "div_strength",
                "sweep_div_match",
                "microbar_range_bps",
                "microbar_body_bps",
                "microbar_vwap_mid_bps",
                "microbar_close_mid_bps",
            ]
        if self.bool_keys is None:
            self.bool_keys = [
                # v2 core (9)
                "ofi_stable",
                "ofi_dir_ok",
                "obi_stable",
                "iceberg_strict",
                "fp_edge_absorb",
                "abs_lvl_ok",
                "reclaim_recent",
                "sweep_recent",
                "cancel_spike_veto",
                # new: health & guards
                "book_health_ok",
                "atr_bad",
                "cvd_quarantine_active",
                # new: standardized confirmations (produced via parse_confirmations_v1)
                "conf_rsi_agree",
                "conf_div_match",
                "conf_sweep_eqh",
                "conf_sweep_eql",
                "conf_sweep",
                "conf_sweep_recent",
                "conf_abs_lvl_ok",
                "conf_fp_edge_absorb",
                "conf_iceberg_strict",
                "conf_weak_progress",
            ]

    @property
    def n_features(self) -> int:
        # + dir(2) + bucket(3) + hour(24) + dow(7)
        return len(self.num_keys) + len(self.bool_keys) + 2 + 3 + 24 + 7

    def feature_names(self) -> list[str]:
        names: list[str] = []
        names += [f"n:{k}" for k in self.num_keys]
        names += [f"b:{k}" for k in self.bool_keys]
        names += ["dir:LONG", "dir:SHORT"]
        names += ["bucket:trend", "bucket:range", "bucket:other"]
        names += [f"hour:{h}" for h in range(24)]
        names += [f"dow:{d}" for d in range(7)]
        return names

    def vectorize(
        self,
        *,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        cancel_spike_veto: bool,
    ) -> list[float]:
        x: list[float] = []
        for k in self.num_keys:
            x.append(_get_num(indicators, k))
        for k in self.bool_keys:
            if k == "cancel_spike_veto":
                x.append(1.0 if cancel_spike_veto else 0.0)
            else:
                x.append(_get_bool(indicators, k))

        # direction
        d = (direction or "").upper()
        x.append(1.0 if d == "LONG" else 0.0)
        x.append(1.0 if d == "SHORT" else 0.0)

        # bucket
        b = (scenario or "").lower()
        x.append(1.0 if b == "trend" else 0.0)
        x.append(1.0 if b == "range" else 0.0)
        x.append(1.0 if b not in ("trend", "range") else 0.0)

        # hour/dow (UTC)
        try:
            import datetime as _dt

            dt = _dt.datetime.fromtimestamp(float(ts_ms) / 1000.0, _dt.UTC)
            hour = int(dt.hour)
            dow = int(dt.weekday())  # 0=Mon
        except Exception:
            hour, dow = 0, 0
        for h in range(24):
            x.append(1.0 if h == hour else 0.0)
        for d_i in range(7):
            x.append(1.0 if d_i == dow else 0.0)
        return x
