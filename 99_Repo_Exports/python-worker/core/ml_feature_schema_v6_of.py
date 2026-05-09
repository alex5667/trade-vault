from __future__ import annotations

"""ML feature schema v6 (OrderFlow).

v6_of = v5_of + world-practice flow + realized adverse drift.
"""


from dataclasses import dataclass

from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF

SCHEMA_HASH = "0e31856c055b"



@dataclass
class MLFeatureSchemaV6OF(MLFeatureSchemaV5OF):
    """v6_of = v5_of + flow + realized adverse drift."""

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        extra_num: list[str] = [
            "taker_buy_rate_ema",
            "taker_sell_rate_ema",
            "taker_net_rate_ema",
            "taker_flow_imb",
            "taker_flow_imb_z",
            "cancel_to_trade_bid",
            "cancel_to_trade_ask",
            "adverse_realized_drift_bps",
            "adverse_realized_drift_z",
            "adverse_realized_drift_ema_bps",
            "adverse_realized_drift_slope_bps_per_min",
            "adverse_realized_drift_cnt",
        ]

        for k in extra_num:
            if k not in self.num_keys:
                self.num_keys.append(k)
