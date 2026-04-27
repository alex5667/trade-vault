# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ETAResult:
    eta_sec: float
    rate_qty_per_sec: float
    depth_qty: float


class QueueETAEvaluator:
    """
    ETA до условного "филла" (съедения) depth на стороне
    по EMA скорости поглощения. Это L3-lite: не очередь FIFO, а
    практичный прокси.

    eta ~= depth_qty / taker_rate_ema_qty_per_sec
    """

    __slots__ = ("eps", "eta_cap_sec")

    def __init__(self, *, eps: float = 1e-9, eta_cap_sec: float = 300.0) -> None:
        self.eps = max(1e-12, float(eps))
        self.eta_cap_sec = max(1.0, float(eta_cap_sec))

    def eta(self, *, depth_qty: float, taker_rate_ema: float) -> ETAResult:
        d = max(0.0, float(depth_qty or 0.0))
        r = max(0.0, float(taker_rate_ema or 0.0))
        if d <= 0.0 or r <= self.eps:
            return ETAResult(eta_sec=self.eta_cap_sec, rate_qty_per_sec=r, depth_qty=d)
        eta = d / max(self.eps, r)
        return ETAResult(eta_sec=min(self.eta_cap_sec, eta), rate_qty_per_sec=r, depth_qty=d)


