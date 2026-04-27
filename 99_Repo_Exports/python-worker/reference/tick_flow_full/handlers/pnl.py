# python-worker/handlers/pnl.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SymbolSpec:
    # Для Binance-USDTM обычно qty*price_diff (quote)
    # Для MT5 инструментов можно задать multiplier (например, XAUUSD ~ 100 oz за 1 lot)
    contract_multiplier: float = 1.0


def pnl_money(entry: float, exit: float, qty: float, side: str, spec: SymbolSpec) -> float:
    sign = 1.0 if side.upper() in ("LONG", "BUY") else -1.0
    return (exit - entry) * sign * qty * spec.contract_multiplier


def pnl_pct(entry: float, exit: float, side: str) -> float:
    if entry <= 0:
        return 0.0
    sign = 1.0 if side.upper() in ("LONG", "BUY") else -1.0
    return ((exit - entry) * sign) / entry * 100.0

