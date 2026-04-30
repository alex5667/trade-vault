from __future__ import annotations

import math
from dataclasses import dataclass
import os
from typing import Any, Callable, Dict, Optional, Tuple

from ..types.crypto_orderflow_handler_types import L2Snapshot


def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _safe_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return v if _is_finite(v) else float(default)


def _sum_top_notional(levels: Any, top_n: int) -> float:
    if not levels:
        return 0.0
    n = max(int(top_n), 0)
    if n <= 0:
        return 0.0
    s = 0.0
    for lv in list(levels)[:n]:
        # L2Level содержит (price, size, notional) в файле типов; откат к size, если notional отсутствует.
        notional = getattr(lv, "notional", None)
        if notional is None:
            notional = getattr(lv, "size", 0.0)
        s += _safe_f(notional, 0.0)
    return float(s)


def _ratio(a: float, b: float, eps: float = 1e-9) -> float:
    return float(a) / max(float(b), float(eps))


@dataclass(frozen=True)
class L2ConfirmCfg:
    """
    Общая конфигурация для L2-подтверждений.
    """
    top_n: int = 5
    max_age_ms: int = 1200
    min_total_notional: float = 0.0
    breakout_imbalance_min: float = 1.15
    absorption_imbalance_min: float = 1.20
    # вето по стенке для пробоя (противоположная стенка слишком близко)
    wall_dist_bps_max: float = 15.0

    @staticmethod
    def _env_pick(symbol: Optional[str], key: str) -> Optional[str]:
        """
        Symbol-first env lookup:
          BTC_OBI_TOP_N -> OBI_TOP_N
        Also supports a more generic alias:
          BTC_L2_TOP_N -> L2_TOP_N
        """
        if symbol:
            v = os.getenv(f"{symbol}_{key}")
            if v is not None and v != "":
                return v
        v2 = os.getenv(key)
        return v2 if v2 is not None and v2 != "" else None

    @classmethod
    def from_env(cls, symbol: Optional[str] = None, base: Optional["L2ConfirmCfg"] = None) -> "L2ConfirmCfg":
        """
        Build L2ConfirmCfg with ENV overrides.

        Intended usage (IMPORTANT for performance):
        - call once during handler/service init, not per tick.
        - store resulting cfg in self.l2_cfg or similar.
        """
        b = base or cls()

        # Prefer OBI_TOP_N (semantic: depth for OBI / book metrics)
        raw = cls._env_pick(symbol, "OBI_TOP_N")
        if raw is None:
            raw = cls._env_pick(symbol, "L2_TOP_N")

        top_n = b.top_n
        if raw is not None:
            try:
                top_n = int(raw)
            except Exception:
                top_n = b.top_n
        # hard safety bounds (avoid accidental huge arrays / CPU spikes)
        if top_n < 1:
            top_n = 1
        if top_n > 50:
            top_n = 50

        return cls(
            top_n=top_n
            max_age_ms=b.max_age_ms
            min_total_notional=b.min_total_notional
            breakout_imbalance_min=b.breakout_imbalance_min
            absorption_imbalance_min=b.absorption_imbalance_min
            wall_dist_bps_max=b.wall_dist_bps_max
        )


class L2ConfirmBreakout:
    """
    Breakout подтверждение по L2 без зависимости от handler.
    Возвращает (ok, details).
    """

    def __init__(
        self
        *
        cfg: L2ConfirmCfg
        get_snapshot: Callable[[Any], Optional[L2Snapshot]]
        get_snapshot_ts_ms: Callable[[Any], Optional[int]]
    ) -> None:
        self.cfg = cfg
        self._get_snapshot = get_snapshot
        self._get_snapshot_ts_ms = get_snapshot_ts_ms

    # ---- экстракторы по умолчанию (без зависимости от хендлера) ----
    @staticmethod
    def default_get_snapshot(ctx: Any) -> Optional[L2Snapshot]:
        # распространенные имена полей: l2_snapshot, l2, orderbook, book, l2_book
        snap = getattr(ctx, "l2_snapshot", None) or getattr(ctx, "l2", None) or getattr(ctx, "orderbook", None) or getattr(ctx, "book", None)
        return snap if isinstance(snap, L2Snapshot) else None

    @staticmethod
    def default_get_snapshot_ts_ms(ctx: Any) -> Optional[int]:
        v = getattr(ctx, "l2_ts_ms", None) or getattr(ctx, "orderbook_ts_ms", None) or getattr(ctx, "book_ts_ms", None)
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    def check(self, ctx: Any, *, dir_up: bool) -> Tuple[bool, Dict[str, Any]]:
        details: Dict[str, Any] = {
            "ok": False
            "dir_up": bool(dir_up)
            "reason": ""
        }

        snap = self._get_snapshot(ctx)
        if snap is None:
            details["reason"] = "no_l2_snapshot"
            return False, details

        ts_now = _safe_f(getattr(ctx, "ts", 0) or getattr(ctx, "ts_ms", 0) or 0.0, 0.0)
        ts_l2 = self._get_snapshot_ts_ms(ctx)
        if ts_l2 is not None and ts_now > 0:
            age = int(ts_now) - int(ts_l2)
            details["age_ms"] = age
            if age > int(self.cfg.max_age_ms):
                details["reason"] = "stale_l2"
                return False, details

        bids = getattr(snap, "bids", None) or []
        asks = getattr(snap, "asks", None) or []

        bid_not = _sum_top_notional(bids, self.cfg.top_n)
        ask_not = _sum_top_notional(asks, self.cfg.top_n)
        details["bid_topN_notional"] = bid_not
        details["ask_topN_notional"] = ask_not

        total = bid_not + ask_not
        details["topN_total_notional"] = total
        if _safe_f(self.cfg.min_total_notional, 0.0) > 0.0 and total < float(self.cfg.min_total_notional):
            details["reason"] = "too_thin_book"
            return False, details

        # Вето по стенке (стенка напротив близко -> пробой, скорее всего, будет заблокирован)
        wall_max = float(self.cfg.wall_dist_bps_max or 0.0)
        if wall_max > 0.0:
            if dir_up and bool(getattr(ctx, "wall_ask", False)):
                d = _safe_f(getattr(ctx, "wall_ask_dist_bps", 1e9), 1e9)
                details["wall_ask_dist_bps"] = d
                if d <= wall_max:
                    details["reason"] = "ask_wall_near"
                    return False, details
            if (not dir_up) and bool(getattr(ctx, "wall_bid", False)):
                d = _safe_f(getattr(ctx, "wall_bid_dist_bps", 1e9), 1e9)
                details["wall_bid_dist_bps"] = d
                if d <= wall_max:
                    details["reason"] = "bid_wall_near"
                    return False, details

        # Правило дисбаланса:
        #   пробой вверх: биды доминируют над асками (поддержка / лифт)
        #   пробой вниз: аски доминируют над бидами (давление / удар)
        imb_min = float(self.cfg.breakout_imbalance_min or 1.0)
        if dir_up:
            imb = _ratio(bid_not, ask_not)
        else:
            imb = _ratio(ask_not, bid_not)
        details["imbalance"] = imb
        details["imbalance_min"] = imb_min
        if imb < imb_min:
            details["reason"] = "imbalance_low"
            return False, details

        details["ok"] = True
        return True, details


class L2ConfirmAbsorption:
    """
    Absorption подтверждение по L2 без зависимости от handler.
    Возвращает (ok, details).
    """

    def __init__(
        self
        *
        cfg: L2ConfirmCfg
        get_snapshot: Callable[[Any], Optional[L2Snapshot]]
        get_snapshot_ts_ms: Callable[[Any], Optional[int]]
    ) -> None:
        self.cfg = cfg
        self._get_snapshot = get_snapshot
        self._get_snapshot_ts_ms = get_snapshot_ts_ms

    @staticmethod
    def default_get_snapshot(ctx: Any) -> Optional[L2Snapshot]:
        snap = getattr(ctx, "l2_snapshot", None) or getattr(ctx, "l2", None) or getattr(ctx, "orderbook", None) or getattr(ctx, "book", None)
        return snap if isinstance(snap, L2Snapshot) else None

    @staticmethod
    def default_get_snapshot_ts_ms(ctx: Any) -> Optional[int]:
        v = getattr(ctx, "l2_ts_ms", None) or getattr(ctx, "orderbook_ts_ms", None) or getattr(ctx, "book_ts_ms", None)
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    def check(self, ctx: Any, *, dir_up: bool) -> Tuple[bool, Dict[str, Any]]:
        details: Dict[str, Any] = {
            "ok": False
            "dir_up": bool(dir_up)
            "reason": ""
        }

        snap = self._get_snapshot(ctx)
        if snap is None:
            details["reason"] = "no_l2_snapshot"
            return False, details

        ts_now = _safe_f(getattr(ctx, "ts", 0) or getattr(ctx, "ts_ms", 0) or 0.0, 0.0)
        ts_l2 = self._get_snapshot_ts_ms(ctx)
        if ts_l2 is not None and ts_now > 0:
            age = int(ts_now) - int(ts_l2)
            details["age_ms"] = age
            if age > int(self.cfg.max_age_ms):
                details["reason"] = "stale_l2"
                return False, details

        bids = getattr(snap, "bids", None) or []
        asks = getattr(snap, "asks", None) or []

        bid_not = _sum_top_notional(bids, self.cfg.top_n)
        ask_not = _sum_top_notional(asks, self.cfg.top_n)
        details["bid_topN_notional"] = bid_not
        details["ask_topN_notional"] = ask_not

        total = bid_not + ask_not
        details["topN_total_notional"] = total
        if _safe_f(self.cfg.min_total_notional, 0.0) > 0.0 and total < float(self.cfg.min_total_notional):
            details["reason"] = "too_thin_book"
            return False, details

        # Абсорбция — это «затухание» импульса:
        #   импульс вверх (dir_up=True) => хотим, чтобы аски доминировали над бидами (стена продаж / рефилл)
        #   импульс вниз => хотим, чтобы биды доминировали над асками.
        imb_min = float(self.cfg.absorption_imbalance_min or 1.0)
        if dir_up:
            imb = _ratio(ask_not, bid_not)
        else:
            imb = _ratio(bid_not, ask_not)
        details["imbalance"] = imb
        details["imbalance_min"] = imb_min
        if imb < imb_min:
            details["reason"] = "imbalance_low"
            return False, details

        details["ok"] = True
        return True, details
