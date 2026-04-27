# python-worker/handlers/levels.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional


def crossed_tp(side: str, last_price: float, tp: float) -> bool:
    if side.upper() in ("LONG", "BUY"):
        return last_price >= tp
    return last_price <= tp


def crossed_sl(side: str, last_price: float, sl: float) -> bool:
    if side.upper() in ("LONG", "BUY"):
        return last_price <= sl
    return last_price >= sl

