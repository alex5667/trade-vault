# domain/signal_outcome.py
"""
SignalOutcome — замыкание контура signal → outcome.

Датакласс и фабрика для построения записи исхода сигнала из TradeClosed.
Используется для:
  1) ML model training (is_win label, features at signal-time)
  2) Threshold calibration (r_multiple distribution)
  3) Post-trade analytics (PnL attribution, execution quality)

Fail-open: factory function NEVER raises; returns None on critical errors.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, Optional

log = logging.getLogger("signal_outcome")


@dataclass
class SignalOutcome:
    """Запись исхода сигнала для ML-фидбек-лупа."""

    # --- identity ---
    sid: str = ""
    order_id: str = ""
    symbol: str = ""
    strategy: str = ""
    source: str = ""
    tf: str = ""
    direction: str = "LONG"

    # --- signal-time features (snapshot at entry) ---
    entry_price: float = 0.0
    entry_ts_ms: int = 0
    sl: float = 0.0
    tp1_price: float = 0.0
    atr: float = 0.0
    entry_tag: str = ""
    regime: str = ""
    scenario: str = ""

    # --- outcome ---
    exit_price: float = 0.0
    exit_ts_ms: int = 0
    pnl_net: float = 0.0
    pnl_gross: float = 0.0
    fees: float = 0.0
    r_multiple: float = 0.0
    one_r_money: float = 0.0
    risk_usd: float = 0.0

    # --- execution path ---
    close_reason: str = ""
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    trailing_started: bool = False
    trailing_active: bool = False
    trailing_moves: int = 0
    duration_ms: int = 0

    # --- excursions ---
    mfe_pnl: float = 0.0
    mae_pnl: float = 0.0
    giveback: float = 0.0
    missed_profit: float = 0.0

    # --- ML label (computed at Python level for Redis; DB uses GENERATED column) ---
    is_win: bool = False

    # --- meta ---
    is_virtual: bool = False
    meta_enforce_cov_bucket: str = ""
    trace_id: str = ""
    event_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Flat dict suitable for Redis Stream XADD (all values → str)."""
        d = asdict(self)
        result: Dict[str, str] = {}
        for k, v in d.items():
            if isinstance(v, bool):
                result[k] = "1" if v else "0"
            elif v is None:
                result[k] = ""
            else:
                result[k] = str(v)
        return result


def from_trade_closed(closed: Any, pos: Any = None) -> Optional[SignalOutcome]:
    """
    Фабрика: строит SignalOutcome из TradeClosed (+ опционально PositionState).

    Fail-open: при любых ошибках логирует WARNING и возвращает None.
    Caller должен проверять результат перед использованием.
    """
    try:
        # --- identity ---
        sid = str(getattr(closed, "sid", "") or "")
        order_id = str(getattr(closed, "order_id", "") or "")
        symbol = str(getattr(closed, "symbol", "") or "")
        strategy = str(getattr(closed, "strategy", "") or "")
        source = str(getattr(closed, "source", "") or "")
        tf = str(getattr(closed, "tf", "") or "")
        direction = str(getattr(closed, "direction", "LONG") or "LONG")

        # --- signal-time features ---
        entry_price = float(getattr(closed, "entry_price", 0.0) or 0.0)
        entry_ts_ms = int(getattr(closed, "entry_ts_ms", 0) or 0)

        # SL: prefer closed.sl, fallback to signal_payload
        sl = float(getattr(closed, "sl", 0.0) or 0.0)
        if sl == 0.0:
            sp = getattr(closed, "signal_payload", {}) or {}
            sl = float(sp.get("sl", 0.0) or 0.0)

        # TP1: prefer closed.tp1_price, then tp_levels[0]
        tp1_price = float(getattr(closed, "tp1_price", 0.0) or 0.0)
        if tp1_price == 0.0:
            tp_levels = getattr(closed, "tp_levels", []) or []
            if tp_levels:
                tp1_price = float(tp_levels[0])

        atr = float(getattr(closed, "atr", 0.0) or 0.0)
        entry_tag = str(getattr(closed, "entry_tag", "") or "")

        # regime/scenario: try direct attrs, then signal_payload
        sp = getattr(closed, "signal_payload", {}) or {}
        regime = str(getattr(closed, "regime", "") or sp.get("regime", "") or "")
        scenario = str(getattr(closed, "scenario", "") or sp.get("scenario", "") or "")

        # --- outcome ---
        exit_price = float(getattr(closed, "exit_price", 0.0) or 0.0)
        exit_ts_ms = int(getattr(closed, "exit_ts_ms", 0) or 0)
        pnl_net = float(getattr(closed, "pnl_net", 0.0) or 0.0)
        pnl_gross = float(getattr(closed, "pnl_gross", 0.0) or 0.0)
        fees = float(getattr(closed, "fees", 0.0) or 0.0)
        r_multiple = float(getattr(closed, "r_multiple", 0.0) or 0.0)
        one_r_money = float(getattr(closed, "one_r_money", 0.0) or 0.0)
        risk_usd = float(getattr(closed, "risk_usd", 0.0) or 0.0)

        # --- execution path ---
        close_reason = str(getattr(closed, "close_reason", "") or "")
        tp1_hit = bool(getattr(closed, "tp1_hit", False))
        tp2_hit = bool(getattr(closed, "tp2_hit", False))
        tp3_hit = bool(getattr(closed, "tp3_hit", False))
        trailing_started = bool(getattr(closed, "trailing_started", False))
        trailing_active = bool(getattr(closed, "trailing_active", False))
        trailing_moves = int(getattr(closed, "trailing_moves", 0) or 0)
        duration_ms = int(getattr(closed, "duration_ms", 0) or 0)

        # --- excursions ---
        mfe_pnl = float(getattr(closed, "mfe_pnl", 0.0) or 0.0)
        mae_pnl = float(getattr(closed, "mae_pnl", 0.0) or 0.0)
        giveback = float(getattr(closed, "giveback", 0.0) or 0.0)
        missed_profit = float(getattr(closed, "missed_profit", 0.0) or 0.0)

        # --- ML label ---
        is_win = r_multiple >= 1.0

        # --- meta ---
        is_virtual = bool(getattr(closed, "is_virtual", False))
        meta_enforce_cov_bucket = str(getattr(closed, "meta_enforce_cov_bucket", "") or "")
        trace_id = str(getattr(closed, "trace_id", "") or "")
        event_id = str(getattr(closed, "event_id", "") or "")

        return SignalOutcome(
            sid=sid
            order_id=order_id
            symbol=symbol
            strategy=strategy
            source=source
            tf=tf
            direction=direction
            entry_price=entry_price
            entry_ts_ms=entry_ts_ms
            sl=sl
            tp1_price=tp1_price
            atr=atr
            entry_tag=entry_tag
            regime=regime
            scenario=scenario
            exit_price=exit_price
            exit_ts_ms=exit_ts_ms
            pnl_net=pnl_net
            pnl_gross=pnl_gross
            fees=fees
            r_multiple=r_multiple
            one_r_money=one_r_money
            risk_usd=risk_usd
            close_reason=close_reason
            tp1_hit=tp1_hit
            tp2_hit=tp2_hit
            tp3_hit=tp3_hit
            trailing_started=trailing_started
            trailing_active=trailing_active
            trailing_moves=trailing_moves
            duration_ms=duration_ms
            mfe_pnl=mfe_pnl
            mae_pnl=mae_pnl
            giveback=giveback
            missed_profit=missed_profit
            is_win=is_win
            is_virtual=is_virtual
            meta_enforce_cov_bucket=meta_enforce_cov_bucket
            trace_id=trace_id
            event_id=event_id
        )

    except Exception as e:
        log.warning("⚠️ from_trade_closed failed (fail-open): %s", e)
        return None
