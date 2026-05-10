"""binance_filters.py — Symbol exchange filter cache.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- SymbolFilters dataclass: LOT_SIZE / PRICE_FILTER / MIN_NOTIONAL parameters
- FiltersCache: lazy per-session cache fetched via exchange info API
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient


# ---------------------------------------------------------------------------
# Primitive helpers (used internally and re-exported for mapper)
# ---------------------------------------------------------------------------

def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# SymbolFilters — Binance exchange-filter parameters per symbol
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SymbolFilters:
    """Exchange filter parameters for a single Binance USDT-M Futures symbol.

    Attributes:
        tick_size:      PRICE_FILTER tickSize — price quantisation step
        step_size:      LOT_SIZE stepSize    — quantity quantisation step
        min_qty:        LOT_SIZE minQty      — minimum order quantity
        min_notional:   MIN_NOTIONAL notional — minimum order value in USDT
    """
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float

    def quantize_price(self, price: float) -> float:
        """Round price to nearest tick_size multiple (always round-up for safety)."""
        if self.tick_size <= 0:
            return price
        return math.ceil(price / self.tick_size) * self.tick_size

    def quantize_qty_down(self, qty: float) -> float:
        """Round qty DOWN to nearest step_size multiple (Binance LOT_SIZE rule)."""
        if self.step_size <= 0:
            return qty
        return math.floor(qty / self.step_size) * self.step_size

    def is_viable(self, qty: float, price: float) -> bool:
        """Basic viability check: qty >= minQty and notional >= min_notional."""
        if qty < self.min_qty:
            return False
        if self.min_notional > 0 and qty * price < self.min_notional:
            return False
        return True


# ---------------------------------------------------------------------------
# FiltersCache — lazy in-process cache keyed by symbol
# ---------------------------------------------------------------------------

class FiltersCache:
    """Lazy cache of symbol exchange filters (fetched once per symbol per session).

    Fetches via BinanceFuturesClient.get_exchange_info() on first access and
    stores in an in-process dict. The cache is NOT shared across threads, but
    dict reads are GIL-safe on CPython for simple get/set operations.
    """

    def __init__(self, client: "BinanceFuturesClient") -> None:
        self.client = client
        self._cache: dict[str, SymbolFilters] = {}

    def get(self, symbol: str) -> SymbolFilters:
        """Return SymbolFilters for symbol, fetching from exchange if not cached."""
        s = symbol.upper()
        if s in self._cache:
            return self._cache[s]

        info = self.client.get_exchange_info()
        sym_list = info.get("symbols") or []
        by_symbol = {
            (x.get("symbol") or "").upper(): x
            for x in sym_list
            if x.get("symbol")
        }
        if s not in by_symbol:
            raise RuntimeError(f"Unknown Binance symbol: {s}")

        filters = by_symbol[s].get("filters") or []
        tick = 0.0
        step = 0.0
        min_qty = 0.0
        min_notional = 0.0
        for f in filters:
            t = f.get("filterType") or ""
            if t == "PRICE_FILTER":
                tick = _f(f.get("tickSize"), tick)
            elif t == "LOT_SIZE":
                step = _f(f.get("stepSize"), step)
                min_qty = _f(f.get("minQty"), min_qty)
            elif t == "MIN_NOTIONAL":
                min_notional = _f(f.get("notional"), min_notional)

        sf = SymbolFilters(
            tick_size=tick or 0.0,
            step_size=step or 0.0,
            min_qty=min_qty or 0.0,
            min_notional=min_notional or 0.0,
        )
        self._cache[s] = sf
        return sf

    def invalidate(self, symbol: str) -> None:
        """Remove cached entry so it will be re-fetched on next access."""
        self._cache.pop(symbol.upper(), None)

    def invalidate_all(self) -> None:
        """Clear the entire cache."""
        self._cache.clear()
