"""ML feature schema v5 (OrderFlow).

This schema is a strict superset of MLFeatureSchemaV4OF.

Goal:
  - keep v4_of stable for deployed models
  - introduce v5_of for training/online gating with extra low-latency microstructure features

Design constraints:
  - deterministic order of features
  - low-latency (only features that already exist in indicators, or are computed in cheap book_microstructure_v4)
  - backward compatibility: v4_of unchanged
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF


@dataclass
class MLFeatureSchemaV5OF(MLFeatureSchemaV4OF):
    """v5_of = v4_of + extra microstructure/regime/fill features.

    Notes:
      - extras are appended to preserve v4 feature order
      - do not remove/reorder existing keys without bumping schema version
    """

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        extra_num: List[str] = [
            # Vol regime (more informative than raw fast/slow)
            "vol_ratio",
            "vol_ratio_z",

            # Execution/fill proxy
            "fill_prob_proxy",
            "eta_fill_sec",
            "fill_prob_p_base",
            "fill_prob_p_wait",
            "exec_fill_pen",
            "max_expected_slippage_bps_eff",

            # LOB pressure (already produced under lob_* keys)
            "lob_qi_mean",
            "lob_qi_max_abs",
            "lob_qi_slope",
            "lob_micro_mid_div_bps",
            "lob_micro_shift_bps",
            "lob_depth_slope_imb",
            "lob_depth_convexity_imb",
            "lob_dw_obi_z",
            "lob_dw_obi_stability_score",
            "lob_dw_obi_stable_secs",

            # Cheap multilevel depth/imbalance/OFI (added to book_microstructure_v4)
            "depth_total_5",
            "depth_imbalance_5",
            "depth_top5_sum",
            "qimb_wmean",
            "qimb_l1",
            "qimb_l5",
            "qimb_slope",
            "ofi_ml_norm",
            "ofi_ml_wsum",
        ]

        extra_bool: List[str] = [
            "res_recovered",
            "lob_dw_obi_stable",
        ]

        # Append extras without duplicates (stable deterministic order).
        for k in extra_num:
            if k not in self.num_keys:
                self.num_keys.append(k)
        for k in extra_bool:
            if k not in self.bool_keys:
                self.bool_keys.append(k)
