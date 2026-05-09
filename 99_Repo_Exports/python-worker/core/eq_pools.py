from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from core.swing_detector import SwingPoint
import contextlib


def _bp_to_px(price: float, bp: float) -> float:
    """
    Преобразование basis points в абсолютную цену.
    tol_px = price * (bp / 10000)
    """
    p = float(price)
    if not math.isfinite(p) or p <= 0:
        return 0.0
    return abs(p) * (float(bp) / 10000.0)


@dataclass
class EQPool:
    """
    Liquidity pool (equal highs / equal lows) построенный из swing-экстремумов.

    kind:
      - "EQH": кластер равных swing high
      - "EQL": кластер равных swing low
    """
    pool_id: str
    kind: str
    level: float
    touches: int
    first_ts_ms: int
    last_ts_ms: int
    strength: float

    # Допуск (в цене), которым пул был актуализирован при последнем апдейте.
    last_tol_px: float = 0.0


class EQPoolTracker:
    """
    Трекер EQH/EQL пулов на микро-барах.

    Ключевая идея:
    - не используем каждый бар high/low (слишком шумно),
      а обновляем пулы ТОЛЬКО от swing-детектора (структурные экстремумы).

    Tolerance:
    - tol_px = max( bp_tol_px, atr_mult * atr )
    - если ATR недоступен => используем только bp

    State hygiene:
    - TTL по last_ts_ms
    - max_pools лимит
    """

    def __init__(
        self,
        symbol: str,
        eq_tol_bp: float = 6.0,
        eq_tol_atr_mult: float = 0.08,
        eq_min_touches: int = 2,
        eq_ttl_ms: int = 3_600_000,  # 1h default
        eq_max_pools: int = 64,
    ) -> None:
        self.symbol = symbol
        self.eq_tol_bp = float(eq_tol_bp)
        self.eq_tol_atr_mult = float(eq_tol_atr_mult)
        self.eq_min_touches = int(eq_min_touches)
        self.eq_ttl_ms = int(eq_ttl_ms)
        self.eq_max_pools = int(eq_max_pools)

        self._pools: list[EQPool] = []
        self._seq = 0

    def apply_config(self, cfg: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            self.eq_tol_bp = float(cfg.get("eq_tol_bp", self.eq_tol_bp))
        with contextlib.suppress(Exception):
            self.eq_tol_atr_mult = float(cfg.get("eq_tol_atr_mult", self.eq_tol_atr_mult))
        try:
            self.eq_min_touches = int(cfg.get("eq_min_touches", self.eq_min_touches))
            if self.eq_min_touches < 1:
                self.eq_min_touches = 1
        except Exception:
            pass
        try:
            self.eq_ttl_ms = int(cfg.get("eq_ttl_ms", self.eq_ttl_ms))
            if self.eq_ttl_ms < 10_000:
                self.eq_ttl_ms = 10_000
        except Exception:
            pass
        try:
            self.eq_max_pools = int(cfg.get("eq_max_pools", self.eq_max_pools))
            if self.eq_max_pools < 8:
                self.eq_max_pools = 8
        except Exception:
            pass

    def _tol_px(self, price: float, atr: float) -> float:
        bp_px = _bp_to_px(price, self.eq_tol_bp)
        atr_px = 0.0
        try:
            a = atr
            if math.isfinite(a) and a > 0:
                atr_px = self.eq_tol_atr_mult * a
        except Exception:
            atr_px = 0.0
        return max(bp_px, atr_px)

    def _cleanup(self, now_ts_ms: int) -> None:
        """
        Удаляем устаревшие пулы и ограничиваем max_pools.
        """
        ttl = self.eq_ttl_ms
        if ttl > 0:
            self._pools = [p for p in self._pools if (now_ts_ms - p.last_ts_ms) <= ttl]

        # если всё еще слишком много — удаляем самые слабые/старые
        if len(self._pools) > self.eq_max_pools:
            self._pools.sort(key=lambda p: (p.strength, p.last_ts_ms))
            self._pools = self._pools[-self.eq_max_pools :]

    def pools(self, kind: str | None = None, only_mature: bool = True) -> list[EQPool]:
        """
        only_mature=True => touches >= eq_min_touches
        """
        out = self._pools
        if kind:
            out = [p for p in out if p.kind == kind]
        if only_mature:
            out = [p for p in out if p.touches >= self.eq_min_touches]
        return list(out)

    def on_swing(self, sp: SwingPoint, atr: float) -> EQPool | None:
        """
        Вызывается на каждом swing (bar_close path).
        Возвращает обновлённый/созданный пул (или None).
        """
        kind = "EQH" if sp.kind == "high" else "EQL"
        price = float(sp.price)
        now_ts = int(sp.ts_ms)
        tol = self._tol_px(price, atr)

        # Подбираем существующий пул, куда попадает swing
        best: EQPool | None = None
        best_dist = float("inf")
        for p in self._pools:
            if p.kind != kind:
                continue
            d = abs(price - float(p.level))
            if d <= max(tol, p.last_tol_px) and d < best_dist:
                best = p
                best_dist = d

        if best is None:
            # создаём новый
            self._seq += 1
            pid = f"{self.symbol}:{kind}:{self._seq}"
            p = EQPool(
                pool_id=pid,
                kind=kind,
                level=price,
                touches=1,
                first_ts_ms=now_ts,
                last_ts_ms=now_ts,
                strength=1.0,
                last_tol_px=tol,
            )
            self._pools.append(p)
            self._cleanup(now_ts)
            return p

        # обновляем существующий пул
        best.touches += 1
        # level update: среднее по касаниям (устойчиво к дрейфу)
        best.level = (best.level * (best.touches - 1) + price) / float(best.touches)
        best.last_ts_ms = now_ts
        best.last_tol_px = tol
        # strength: пока просто touches; позже можно добавить recency/volume/HTF веса
        best.strength = float(best.touches)
        self._cleanup(now_ts)
        return best

    def nearest_pool(self, kind: str, price: float) -> tuple[EQPool, float] | None:
        """
        Возвращает ближайший mature пул и расстояние до него (px).
        """
        pools = self.pools(kind=kind, only_mature=True)
        if not pools:
            return None
        best = min(pools, key=lambda p: abs(float(p.level) - float(price)))
        return best, abs(float(best.level) - float(price))
