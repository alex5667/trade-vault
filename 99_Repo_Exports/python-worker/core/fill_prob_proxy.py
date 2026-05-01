# tick_flow_full/core/fill_prob_proxy.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Fill probability / ETA proxy based on L3-lite stats.

We use:
- cancel_to_trade_side (>=0)
- eta_fill_side_sec (>0)
to compute a simple proxy:
  p_base = 1 / (1 + cancel_to_trade)
  p_wait = min(1, max_wait_s / eta_fill_s)
  p_fill = clamp01(p_base * p_wait)
"""


from typing import Dict


def compute_fill_prob_proxy(
    *,
    direction: str,
    cancel_to_trade_bid: float = 0.0,
    cancel_to_trade_ask: float = 0.0,
    # eta_fill_*_sec: optional — when absent (or 0) the ETA term is 1.0 (no penalty)
    eta_fill_bid_sec: float = 0.0,
    eta_fill_ask_sec: float = 0.0,
    max_wait_s: float = 2.0,
    eps: float = 1e-9,
) -> Dict[str, float]:
    d = str(direction or "").upper()
    if d == "LONG":
        c2t = float(cancel_to_trade_bid or 0.0)
        eta = float(eta_fill_bid_sec or 0.0)
    else:
        c2t = float(cancel_to_trade_ask or 0.0)
        eta = float(eta_fill_ask_sec or 0.0)

    if c2t < 0.0:
        c2t = 0.0

    p_base = 1.0 / (1.0 + c2t)

    if eta <= eps:
        # No ETA data → no penalty (conservative: assume fill is possible)
        p_wait = 1.0
    else:
        p_wait = min(1.0, float(max_wait_s) / max(eta, eps))

    p = p_base * p_wait
    if p < 0.0:
        p = 0.0
    if p > 1.0:
        p = 1.0

    return {
        "fill_prob_proxy": float(p),
        # Alias: world-practice tests look up "fill_prob" or "p_fill" in the output
        "fill_prob": float(p),
        "p_fill": float(p),
        "eta_fill_sec": float(eta),
        "cancel_to_trade_side": float(c2t),
        "p_base": float(p_base),
        "p_wait": float(p_wait),
    }

