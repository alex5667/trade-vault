from __future__ import annotations

"""Meta-features schema v9.

v9 = v8 + liquidation-map (liqmap) compact scalars + liqmap gate scalars.

Intent:
  - keep v8 stable as champion
  - v9 is additive challenger for training/inference

Notes:
  - liqmap features are injected into indicators by TickProcessor before OFConfirmEngine.build()
  - liqmap gate scalars are computed inside OFConfirmEngine and exported
"""


import hashlib
from typing import Any

from core.meta_features_v8 import (
    META_FEAT_V8_COLS,
    META_FEAT_V8_TRANSFORMS,
    _try_get_float,
    build_meta_features_v8,
)

LIQMAP_WINDOWS_V9 = ("5m", "1h")

META_FEAT_V9_NAME = "meta_feat_v9"
META_FEAT_V9_VERSION = 9


def _liq_cols(window: str) -> list[str]:
    w = str(window)
    p = f"liqmap_{w}_"
    return [
        f"{p}age_ms",
        f"{p}levels_n",
        f"{p}total_usd",
        f"{p}near_total_usd",
        f"{p}near_long_usd",
        f"{p}near_short_usd",
        f"{p}near_imb",
        f"{p}dist_up_bps",
        f"{p}dist_dn_bps",
        f"{p}peak_up1_usd",
        f"{p}peak_dn1_usd",
        f"{p}peak_up1_share",
        f"{p}peak_dn1_share",
        f"{p}peaks_up",
        f"{p}peaks_dn",
    ]


META_FEAT_V9_NEW_COLS: list[str] = []
for _w in LIQMAP_WINDOWS_V9:
    META_FEAT_V9_NEW_COLS.extend(_liq_cols(_w))

META_FEAT_V9_NEW_COLS.extend(
    [
        "liqmap_gate_shadow_veto",
        "liqmap_gate_veto",
        "liqmap_gate_rr",
        "liqmap_gate_risk_bps",
        "liqmap_gate_reward_bps",
        "liqmap_gate_adverse_peak_usd",
        "liqmap_gate_favorable_peak_usd",
    ]
)

META_FEAT_V9_COLS: list[str] = list(META_FEAT_V8_COLS) + list(META_FEAT_V9_NEW_COLS)
META_FEAT_V9_HASH: str = hashlib.sha1(",".join(META_FEAT_V9_COLS).encode("utf-8")).hexdigest()


META_FEAT_V9_TRANSFORMS: dict[str, str] = dict(META_FEAT_V8_TRANSFORMS)
META_FEAT_V9_DESCRIPTIONS: dict[str, str] = {}

for _w in LIQMAP_WINDOWS_V9:
    p = f"liqmap_{_w}_"
    META_FEAT_V9_TRANSFORMS.update(
        {
            f"{p}age_ms": "log1p",
            f"{p}levels_n": "log1p",
            f"{p}total_usd": "log1p",
            f"{p}near_total_usd": "log1p",
            f"{p}near_long_usd": "log1p",
            f"{p}near_short_usd": "log1p",
            f"{p}near_imb": "clip(-1,1)",
            f"{p}dist_up_bps": "log1p",
            f"{p}dist_dn_bps": "log1p",
            f"{p}peak_up1_usd": "log1p",
            f"{p}peak_dn1_usd": "log1p",
            f"{p}peak_up1_share": "clip(0,1)",
            f"{p}peak_dn1_share": "clip(0,1)",
            f"{p}peaks_up": "log1p",
            f"{p}peaks_dn": "log1p",
        }
    )
    META_FEAT_V9_DESCRIPTIONS.update(
        {
            f"{p}age_ms": f"LiqMap snapshot age (ms) for window={_w}",
            f"{p}levels_n": f"Number of liqmap levels for window={_w}",
            f"{p}total_usd": f"Total liquidation USD mass in snapshot for window={_w}",
            f"{p}near_total_usd": f"Near-band liquidation USD around price for window={_w}",
            f"{p}near_long_usd": f"Near-band long liquidation USD for window={_w}",
            f"{p}near_short_usd": f"Near-band short liquidation USD for window={_w}",
            f"{p}near_imb": f"Near-band imbalance (long-short)/total for window={_w}",
            f"{p}dist_up_bps": f"Distance to nearest peak above price (bps) for window={_w}",
            f"{p}dist_dn_bps": f"Distance to nearest peak below price (bps) for window={_w}",
            f"{p}peak_up1_usd": f"USD size of nearest above-price peak for window={_w}",
            f"{p}peak_dn1_usd": f"USD size of nearest below-price peak for window={_w}",
            f"{p}peak_up1_share": f"Share of total mass for nearest above peak for window={_w}",
            f"{p}peak_dn1_share": f"Share of total mass for nearest below peak for window={_w}",
            f"{p}peaks_up": f"Count of peaks above price for window={_w}",
            f"{p}peaks_dn": f"Count of peaks below price for window={_w}",
        }
    )

META_FEAT_V9_TRANSFORMS.update(
    {
        "liqmap_gate_shadow_veto": "identity",
        "liqmap_gate_veto": "identity",
        "liqmap_gate_rr": "log1p",
        "liqmap_gate_risk_bps": "log1p",
        "liqmap_gate_reward_bps": "log1p",
        "liqmap_gate_adverse_peak_usd": "log1p",
        "liqmap_gate_favorable_peak_usd": "log1p",
    }
)
META_FEAT_V9_DESCRIPTIONS.update(
    {
        "liqmap_gate_shadow_veto": "LiqMap gate would veto (shadow mode)",
        "liqmap_gate_veto": "LiqMap gate hard veto (enforce mode)",
        "liqmap_gate_rr": "Reward/Risk ratio computed from nearest peaks (gate window)",
        "liqmap_gate_risk_bps": "Risk distance to nearest adverse peak (bps)",
        "liqmap_gate_reward_bps": "Reward distance to nearest favorable peak (bps)",
        "liqmap_gate_adverse_peak_usd": "USD size of nearest adverse peak used for gate",
        "liqmap_gate_favorable_peak_usd": "USD size of nearest favorable peak used for gate",
    }
)


def build_meta_features_v9(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    **kwargs,
) -> tuple[dict[str, float], list[str]]:
    """Build meta_feat_v9 (v8 base + liqmap scalars + liqmap gate scalars)."""

    feat, missing = build_meta_features_v8(evidence=evidence, indicators=indicators, **kwargs)

    nested_ind = evidence.get("indicators") if isinstance(evidence, dict) else None
    if not isinstance(nested_ind, dict):
        nested_ind = {}

    for k in META_FEAT_V9_NEW_COLS:
        v = None
        if isinstance(evidence, dict) and k in evidence:
            v = _try_get_float(evidence.get(k))
        elif isinstance(indicators, dict) and k in indicators:
            v = _try_get_float(indicators.get(k))
        elif k in nested_ind:
            v = _try_get_float(nested_ind.get(k))

        if v is None:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)
        else:
            feat[k] = float(v)
            while k in missing:
                missing.remove(k)

    for k in META_FEAT_V9_COLS:
        if k not in feat:
            feat[k] = 0.0
            if k not in missing:
                missing.append(k)

    return feat, missing
