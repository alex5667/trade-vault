from __future__ import annotations

"""
Centralized aggressor-side signing utilities.

Goal:
  - Prevent hidden BUY/SELL bias when side is missing/invalid.
  - Provide a single canonical mapping from tick -> sign in {-1, 0, +1}.

Conventions:
  - sign = +1  => BUY aggressor (lifted ask)
  - sign = -1  => SELL aggressor (hit bid)
  - sign = 0   => UNKNOWN / cannot infer safely

Preferred inference order:
  1) is_buyer_maker (if present):
        is_buyer_maker == True  => SELL aggressor (sign=-1)
        is_buyer_maker == False => BUY aggressor  (sign=+1)
     This matches Binance semantics: "buyer is maker" => seller is taker/aggressor.
  2) explicit side in {"BUY","SELL"}
  3) UNKNOWN => sign=0
"""

from collections.abc import Mapping
from typing import Any


def side_sign_from_tick(tick: Mapping[str, Any]) -> tuple[int, str]:
    """
    Returns (sign, reason).

    reason is one of:
      - maker_buy, maker_sell
      - side_buy, side_sell
      - unknown
    """
    try:
        ibm = tick.get("is_buyer_maker", None)
        if isinstance(ibm, bool):
            # buyer is maker => SELL aggressor (taker) ; buyer not maker => BUY aggressor
            return (-1, "maker_sell") if ibm else (1, "maker_buy")

        side = (tick.get("side") or "").strip().upper()
        if side == "BUY":
            return (1, "side_buy")
        if side == "SELL":
            return (-1, "side_sell")
        return (0, "unknown")
    except Exception:
        return (0, "unknown")


def signed_qty(qty: float, sign: int) -> float:
    """
    Apply sign to qty.
    If sign is not -1/0/+1, returns 0.0 (fail-safe).
    """
    try:
        s = int(sign)
        if s == 1:
            return float(qty)
        if s == -1:
            return -float(qty)
        return 0.0
    except Exception:
        return 0.0

