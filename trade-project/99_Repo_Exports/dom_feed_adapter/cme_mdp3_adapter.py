# -*- coding: utf-8 -*-
"""
CME_MDP3_Adapter — скелет локальной книги.
"""

from bisect import insort
from typing import List, Dict


class LocalBook:
    def __init__(self, max_depth: int = 100):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.bid_px: List[float] = []
        self.ask_px: List[float] = []
        self.max_depth = max_depth

    def _insert(self, side: str, price: float, qty: float):
        book = self.bids if side == "bid" else self.asks
        idx = self.bid_px if side == "bid" else self.ask_px
        if qty <= 0:
            if price in book:
                del book[price]
                try:
                    idx.remove(price)
                except ValueError:
                    pass
            return
        existed = price in book
        book[price] = qty
        if not existed:
            if side == "bid":
                insort(self.bid_px, price)
                self.bid_px.sort(reverse=True)
                self.bid_px[:] = self.bid_px[: self.max_depth]
            else:
                insort(self.ask_px, price)
                self.ask_px.sort()
                self.ask_px[:] = self.ask_px[: self.max_depth]

    def on_mbp_update(self, side: str, price: float, qty: float):
        self._insert(side, price, qty)

    def levels(self, top: int = 10) -> List[Dict[str, float]]:
        out = []
        bids = self.bid_px[:top]
        asks = self.ask_px[:top]
        L = max(len(bids), len(asks))
        for i in range(L):
            px_b = bids[i] if i < len(bids) else None
            px_a = asks[i] if i < len(asks) else None
            out.append({
                "price": float(px_b if px_b is not None else (px_a if px_a is not None else 0.0)),
                "bid": float(self.bids.get(px_b, 0.0) if px_b is not None else 0.0),
                "ask": float(self.asks.get(px_a, 0.0) if px_a is not None else 0.0),
            })
        return out


