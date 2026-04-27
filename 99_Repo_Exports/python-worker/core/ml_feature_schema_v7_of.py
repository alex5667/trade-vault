"""ML feature schema v7 (OrderFlow).

v7_of = v6_of + execution/LOB extras already present + *new* Hawkes-like intensity splits and VPIN-like toxicity.

Rationale
---------
- Split intensities (taker buy vs sell, cancel bid vs ask, limit-add) add regime/context with minimal compute.
- VPIN-like toxicity is a cheap proxy for adverse selection / one-sided flow.

All features must already be present in `indicators` at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OF

SCHEMA_HASH = "3c555456910d"


_A5_BOOL_KEYS = [
    # A5: runtime flags (encoded as 0/1)
    "flag_high_vol",
    "flag_low_liquidity",
    "flag_large_trade",
    "flag_mean_reversion_mode",
    "flag_session_open",
    "flag_session_close",
    "flag_macro_event",
]

# B2: calendar flags (UTC deterministic)
_B2_BOOL_KEYS = [
    "cal_eom_utc",
    "cal_eoq_utc",
]

# Optional numeric encodings (still low-cardinality)
_B2_NUM_KEYS = [
    "cal_dom_utc",
    "cal_doq_utc",
]


@dataclass
class MLFeatureSchemaV7OF(MLFeatureSchemaV6OF):
    """v7_of = v6_of + hawkes split + vpin toxicity + add-rate + A5 flags."""

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        extra_num: List[str] = [
            # L3 additions rates
            "added_bid_rate_ema",
            "added_ask_rate_ema",
            "added_total_rate_ema",

            # VPIN-like toxicity
            "vpin_tox_ema",
            "vpin_tox_z",

            # Hawkes-like intensities (split)
            "hawkes_dt_s",
            "hawkes_taker_buy_lam",
            "hawkes_taker_sell_lam",
            "hawkes_cancel_bid_lam",
            "hawkes_cancel_ask_lam",
            "hawkes_limit_add_lam",

            # keep legacy aggregates too (already in indicators via hawkes_snapshot)
            "hawkes_taker_lam",
            "hawkes_cancel_lam",
            "hawkes_churn_lam",

            # raw states (debuggable + often informative for linear models)
            "hawkes_S_taker_buy",
            "hawkes_S_taker_sell",
            "hawkes_S_cancel_bid",
            "hawkes_S_cancel_ask",
            "hawkes_S_limit_add",
        ]

        for k in extra_num:
            if k not in self.num_keys:
                self.num_keys.append(k)

        # B2: optional numeric calendar encodings (low-cardinality)
        for k in _B2_NUM_KEYS:
            if k not in self.num_keys:
                self.num_keys.append(k)

        # keep order stable and de-dup
        bk = list(self.bool_keys or [])
        for k in _A5_BOOL_KEYS:
            if k not in bk:
                bk.append(k)
        for k in _B2_BOOL_KEYS:
            if k not in bk:
                bk.append(k)
        self.bool_keys = bk


@dataclass
class MLFeatureSchemaV7OFStable(MLFeatureSchemaV7OF):
    """v7_of_stable = v7_of minus denylisted keys (ML_FEATURE_DENYLIST_PATH).

    Denylist is applied symmetrically to num_keys and bool_keys.
    Keys are raw indicator keys (without n:/b:).

    Fail-open: missing/invalid denylist keeps schema identical to v7_of.
    """

    def __post_init__(self) -> None:  # noqa: D401
        from core.feature_denylist_v1 import denylist_flat
        super().__post_init__()
        deny = set(denylist_flat())
        if not deny:
            return
        self.num_keys = [k for k in (self.num_keys or []) if k not in deny]
        self.bool_keys = [k for k in (self.bool_keys or []) if k not in deny]
