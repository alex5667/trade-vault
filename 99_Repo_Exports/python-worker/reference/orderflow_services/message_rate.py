from __future__ import annotations
"""
Phase E / P4: Message Rate Tracker (OTR / Quote Stuffing proxy)

Tracks per-symbol message rates at 1-Hz granularity:
  - book_update_rate_hz: EMA of book updates per second
  - book_update_rate_z:  robust z-score of book update rate
  - trade_msg_rate_hz:   EMA of aggTrade messages per second
  - trade_msg_rate_z:    robust z-score of trade message rate
  - cancel_rate_z:       robust z-score fed from L3-lite cancel_rate_ema
  - otr:                 Order-To-Trade ratio proxy = book_rate / trade_rate
  - otr_z:              robust z-score of OTR

Design:
  - 1Hz bucketed via integer(ts_ms // 1000) → each bucket counts msgs in that second.
  - EMA update happens once per second (bucket boundary crossing).
  - All z-scores use RollingRobustZ (median/MAD, fail-open).
  - No locking: single async hot-path writer, safe for asyncio.
  - Fail-open: any exception → silent pass, values unchanged.
"""


import os
import time
import logging
from typing import Optional

# Reuse existing robust stats from core
from core.robust_stats import RollingRobustZ

logger = logging.getLogger("orderflow_message_rate")

# ── ENV ──────────────────────────────────────────────────────────────────────
_ALPHA = float(os.getenv("MSG_RATE_ALPHA", "0.20") or 0.20)
_Z_WINDOW = int(os.getenv("MSG_RATE_Z_WINDOW", "120") or 120)


class MessageRateTracker:
    """
    1-Hz bucketed EMA tracker for book / trade message rates.

    Usage:
      tracker = MessageRateTracker()
      # on book update:
      tracker.on_book_msg(book_ts_ms)
      # on trade tick:
      tracker.on_trade_msg(tick_ts_ms)
      # to push cancel EMA from L3-lite:
      tracker.observe_cancel_rate_ema(cancel_rate_ema)

    All state fields are public floats; safe to read from tick_processor.
    """

    def __init__(self, alpha: float = _ALPHA, z_window: int = _Z_WINDOW) -> None:
        self.alpha = float(alpha)

        # ── Book rate ────────────────────────────────────────────────────────
        self.book_update_rate_hz: float = 0.0
        self.book_update_rate_z: float = 0.0
        self._book_bucket: int = 0       # current 1-s bucket (ts_ms // 1000)
        self._book_cnt: int = 0           # count within current bucket
        self._book_z: RollingRobustZ = RollingRobustZ(window=max(32, z_window))

        # ── Trade rate ───────────────────────────────────────────────────────
        self.trade_msg_rate_hz: float = 0.0
        self.trade_msg_rate_z: float = 0.0
        self._trade_bucket: int = 0
        self._trade_cnt: int = 0
        self._trade_z: RollingRobustZ = RollingRobustZ(window=max(32, z_window))

        # ── Cancel rate (from L3-lite EMA, unitless 0..1) ────────────────────
        self.cancel_rate_z: float = 0.0
        self._cancel_z: RollingRobustZ = RollingRobustZ(window=max(32, z_window))

        # ── OTR = book_msgs / trade_msgs (order-to-trade ratio proxy) ────────
        self.otr: float = 0.0
        self.otr_z: float = 0.0
        self._otr_z: RollingRobustZ = RollingRobustZ(window=max(32, z_window))

    # ── Public API ────────────────────────────────────────────────────────────

    def on_book_msg(self, ts_ms: int) -> None:
        """Called on every book snapshot/update from BookProcessor.process_book()."""
        try:
            bucket = int(ts_ms) // 1000
            if bucket != self._book_bucket:
                # New 1-s bucket: flush previous count as instantaneous rate (Hz)
                if self._book_bucket > 0:
                    inst = float(self._book_cnt)  # msgs / 1s = Hz
                    # EMA update
                    self.book_update_rate_hz = (
                        self.alpha * inst + (1.0 - self.alpha) * self.book_update_rate_hz
                    )
                    # Robust z
                    try:
                        self._book_z.update(self.book_update_rate_hz)
                        self.book_update_rate_z = float(
                            self._book_z.z(self.book_update_rate_hz)
                        )
                    except Exception:
                        pass
                    # Recompute OTR at bucket boundary
                    self._update_otr()
                self._book_bucket = bucket
                self._book_cnt = 1
            else:
                self._book_cnt += 1
        except Exception:
            pass

    def on_trade_msg(self, ts_ms: int) -> None:
        """Called on every aggTrade tick from TickProcessor (denominator for OTR)."""
        try:
            bucket = int(ts_ms) // 1000
            if bucket != self._trade_bucket:
                if self._trade_bucket > 0:
                    inst = float(self._trade_cnt)
                    self.trade_msg_rate_hz = (
                        self.alpha * inst + (1.0 - self.alpha) * self.trade_msg_rate_hz
                    )
                    try:
                        self._trade_z.update(self.trade_msg_rate_hz)
                        self.trade_msg_rate_z = float(
                            self._trade_z.z(self.trade_msg_rate_hz)
                        )
                    except Exception:
                        pass
                self._trade_bucket = bucket
                self._trade_cnt = 1
            else:
                self._trade_cnt += 1
        except Exception:
            pass

    def observe_cancel_rate_ema(self, cancel_rate_ema: float) -> None:
        """
        Push current L3-lite cancel_*_rate_ema value (already an EMA, 0..∞).
        Called from BookProcessor after l3_stats snap is available.
        """
        try:
            v = float(cancel_rate_ema or 0.0)
            self._cancel_z.update(v)
            self.cancel_rate_z = float(self._cancel_z.z(v))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _update_otr(self) -> None:
        """
        Recompute OTR = book_rate / max(trade_rate, 1.0).

        Protect against illiquid / silent trade periods: denominator floored at 1.0.
        OTR > 1 = more book churn than trades (normal for futures micro-hedging);
        OTR >> 10..50 = potential quote stuffing / spoofing.
        """
        try:
            denom = max(float(self.trade_msg_rate_hz), 1.0)
            self.otr = float(self.book_update_rate_hz) / denom
            self._otr_z.update(self.otr)
            self.otr_z = float(self._otr_z.z(self.otr))
        except Exception:
            pass

    def snapshot(self) -> dict:
        """Return lightweight dict for strategy / indicators consumption."""
        return {
            "book_update_rate_hz": float(self.book_update_rate_hz),
            "book_update_rate_z": float(self.book_update_rate_z),
            "trade_msg_rate_hz": float(self.trade_msg_rate_hz),
            "trade_msg_rate_z": float(self.trade_msg_rate_z),
            "cancel_rate_z": float(self.cancel_rate_z),
            "otr": float(self.otr),
            "otr_z": float(self.otr_z),
        }
