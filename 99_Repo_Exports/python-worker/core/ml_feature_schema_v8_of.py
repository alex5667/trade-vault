from __future__ import annotations

"""ML feature schema v8 (OrderFlow).

v8_of = v7_of + strict data-quality (DQ) metrics + LiqMap compact features.

Why v8
------
We want a single train==serve contract that includes:
  - strict DQ indicators (gap p95, missing-seq EMAs)
  - LiqMap state (snapshot age + proximity/intensity of peaks)
  - optional execution overlay outputs (TP1/SL anchoring adjustments)

Constraints
-----------
  - deterministic order (append-only vs v7)
  - low-latency: only consume keys that already exist in runtime `indicators`
  - fail-open: missing keys are vectorized as 0.0 by the base schema logic

Key naming note (LiqMap)
-----------------------
The current runtime feature layer (`core/liqmap_features_v1.py`) exposes the
following stable keys per window:
  - liqmap_<w>_total_usd, liqmap_<w>_near_total_usd, liqmap_<w>_near_imb
  - liqmap_<w>_dist_up_bps, liqmap_<w>_dist_dn_bps
  - liqmap_<w>_peak_up1_usd, liqmap_<w>_peak_dn1_usd
  - liqmap_<w>_age_ms

Some design docs refer to a slightly different naming (peak_up_usd, peak_up_dist_bps).
To stay robust during refactors we include both variants in v8. If a key is not
present at runtime it simply vectorizes to 0.0 (safe).
"""


from dataclasses import dataclass

from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF

SCHEMA_HASH = "c1ec144d4624"



_DQ_NUM_KEYS_V8: list[str] = [
    # Strict DQ (computed in TickProcessor._update_strict_dq_trackers)
    "tick_gap_p95_ms",
    "tick_missing_seq_ema",
    "book_missing_seq_ema",
]


_LIQMAP_NUM_KEYS_V8: list[str] = [
    # LiqMap compact features (start with a single stable window: 1h)
    "liqmap_1h_total_usd",
    "liqmap_1h_near_total_usd",
    "liqmap_1h_near_imb",
    # current v1 naming (distance + top-1 peak USD)
    "liqmap_1h_dist_dn_bps",
    "liqmap_1h_peak_up1_usd",
    "liqmap_1h_peak_dn1_usd",
    "liqmap_1h_age_ms",
    # forward/alt naming (kept for compatibility with design docs)
]


_LIQMAP_LEVELS_NUM_KEYS_V8: list[str] = [
    # Optional TP1/SL anchoring overlay outputs (D1/D2)
    "liqmap_tp1_adj_bps",
    "liqmap_sl_adj_bps",
]


_LIQMAP_LEVELS_BOOL_KEYS_V8: list[str] = [
    "liqmap_levels_applied",
]


@dataclass
class MLFeatureSchemaV8OF(MLFeatureSchemaV7OF):
    """v8_of = v7_of + DQ + LiqMap (+ optional levels overlay outputs)."""

    def __post_init__(self) -> None:  # noqa: D401
        super().__post_init__()

        extra_num: list[str] = []
        extra_num += list(_DQ_NUM_KEYS_V8)
        extra_num += list(_LIQMAP_NUM_KEYS_V8)
        extra_num += list(_LIQMAP_LEVELS_NUM_KEYS_V8)

        for k in extra_num:
            if k not in (self.num_keys or []):
                self.num_keys.append(k)

        bk = list(self.bool_keys or [])
        for k in _LIQMAP_LEVELS_BOOL_KEYS_V8:
            if k not in bk:
                bk.append(k)
        self.bool_keys = bk


@dataclass
class MLFeatureSchemaV8OFStable(MLFeatureSchemaV8OF):
    """v8_of_stable = v8_of minus denylisted keys (ML_FEATURE_DENYLIST_PATH).

    Denylist is applied symmetrically to num_keys and bool_keys.
    Keys are raw indicator keys (without n:/b:).

    Fail-open: missing/invalid denylist keeps schema identical to v8_of.
    """

    def __post_init__(self) -> None:  # noqa: D401
        from core.feature_denylist_v1 import denylist_flat

        super().__post_init__()
        deny = set(denylist_flat())
        if not deny:
            return
        self.num_keys = [k for k in (self.num_keys or []) if k not in deny]
        self.bool_keys = [k for k in (self.bool_keys or []) if k not in deny]
