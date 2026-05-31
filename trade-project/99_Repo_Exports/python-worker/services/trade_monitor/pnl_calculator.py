# services/trade_monitor/pnl_calculator.py
"""
PnL calculation, R-value, commission-adjusted exit price, and stats update.

Extracted from TradeMonitorService._get_spec / _calculate_r_value /
_resolve_closed_at / _calc_commission_adjusted_exit_price /
_update_stats_from_dicts (monolith lines 2184-2121, 3346-3378, 5060-5122).

Design:
  - Stateless callable (no threading, no locks).
  - All external I/O (Redis, RegimeGuard persist) goes through injected deps.
  - Fail-open on every path: exceptions are caught and logged, never propagated.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from services.pnl_math import SymbolSpec, get_symbol_info, spec_from_symbol_info

logger = logging.getLogger(__name__)


class PnlCalculator:
    """
    Pure computation helpers for trade-close analytics.

    Dependencies (injected, no direct Redis/DB construction):
      redis       — Redis client (decode_responses=True).
      regime_guard — optional RegimeGuard instance; can be None.
    """

    def __init__(
        self,
        redis: Any,
        regime_guard: Any | None = None,
        *,
        log: logging.Logger | None = None,
    ) -> None:
        self._redis = redis
        self._regime_guard = regime_guard
        self._logger = log or logger

    # ------------------------------------------------------------------
    # Symbol spec
    # ------------------------------------------------------------------

    def get_spec(self, symbol: str) -> SymbolSpec:
        """Fetch SymbolSpec from Redis; returns empty SymbolSpec on failure."""
        try:
            info = get_symbol_info(symbol, self._redis)
            return spec_from_symbol_info(info)
        except Exception:
            return SymbolSpec()

    # ------------------------------------------------------------------
    # R-value
    # ------------------------------------------------------------------

    def calc_r_value(self, pos: Any, closed: Any) -> float:
        """Return pnl_net / risk_amount.  0.0 if risk_amount ≤ 0 or missing."""
        pnl = getattr(closed, "pnl_net", 0.0) or 0.0
        risk = getattr(pos, "risk_amount", 0.0) or 0.0
        return pnl / risk if risk > 0 else 0.0

    # ------------------------------------------------------------------
    # Closed-at resolution
    # ------------------------------------------------------------------

    def resolve_closed_at(self, closed: Any) -> datetime:
        """Convert exit_ts_ms / closed_at field to timezone-aware datetime (UTC)."""
        closed_at = getattr(closed, "exit_ts_ms", None) or getattr(
            closed, "closed_at", None
        )
        if closed_at is None:
            return datetime.now(UTC)
        if isinstance(closed_at, (int, float)):
            ts_sec = float(closed_at)
            if ts_sec > 946_684_800_000:  # epoch ms threshold
                ts_sec /= 1000.0
            return datetime.fromtimestamp(ts_sec, tz=UTC)
        if not hasattr(closed_at, "tzinfo"):
            return datetime.now(UTC)
        return closed_at  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Commission-adjusted exit price
    # (used by OrphanRecoveryPolicy when no real price is available)
    # ------------------------------------------------------------------

    def commission_adjusted_exit_price(
        self,
        entry_px: float,
        direction: str,
        spec: SymbolSpec,
    ) -> float:
        """
        Return entry_price adjusted by round-trip commission so that gross PnL
        equals −fees, giving analytics realistic cost information instead of 0.

        Formula mirrors monolith _calc_commission_adjusted_exit_price (lines 3346-3378).
        """
        try:
            if entry_px <= 0:
                return entry_px
            taker_bps = float(getattr(spec, "taker_fee_bps", 5.0) or 5.0)
            round_trip_bps = taker_bps * 2.0  # open + close
            adjustment_factor = round_trip_bps / 10_000.0
            direction_up = str(direction or "LONG").strip().upper()
            if direction_up == "LONG":
                return entry_px * (1.0 - adjustment_factor)
            else:  # SHORT
                return entry_px * (1.0 + adjustment_factor)
        except Exception:
            return entry_px  # fail-open: gross PnL = 0 is better than crash

    # ------------------------------------------------------------------
    # Stats update + RegimeGuard integration
    # ------------------------------------------------------------------

    def update_stats(
        self,
        pos_dict: dict[str, Any],
        closed_dict: dict[str, Any],
        *,
        submit_persist_task_fn: Any = None,
    ) -> None:
        """
        Update StatsAggregator and optionally submit a RegimeGuard persist task.

        Args:
            pos_dict        — dict copy of PositionState (safe after position eviction).
            closed_dict     — dict copy of TradeClosed.
            submit_persist_task_fn — callable(task, tags) from TradeMonitorService;
                                     used to schedule RegimeGuard persistence.
        """
        # Virtual trades are excluded from global stats.
        is_virtual = bool(
            pos_dict.get("is_virtual") or closed_dict.get("is_virtual")
        )
        if not is_virtual:
            try:
                from services.stats_aggregator import StatsAggregator  # lazy import

                StatsAggregator.update_stats(self._redis, pos_dict, closed_dict)
            except Exception as e:
                self._logger.warning("stats update failed: %s", e)

        if self._regime_guard:
            try:
                signal_id = str(
                    pos_dict.get("sid") or closed_dict.get("sid") or ""
                )
                family = str(
                    pos_dict.get("family") or closed_dict.get("family") or "unknown"
                )
                venue = str(
                    pos_dict.get("venue") or closed_dict.get("venue") or "unknown"
                )
                symbol = str(
                    pos_dict.get("symbol") or closed_dict.get("symbol") or "unknown"
                )
                timeframe = str(
                    pos_dict.get("tf")
                    or pos_dict.get("timeframe")
                    or closed_dict.get("tf")
                    or closed_dict.get("timeframe")
                    or "unknown"
                )

                # Build lightweight proxy objects to reuse calc_r_value / resolve_closed_at
                class _Proxy:
                    def __init__(self, d: dict) -> None:
                        self.__dict__.update(d)

                r_value = self.calc_r_value(_Proxy(pos_dict), _Proxy(closed_dict))
                closed_at = self.resolve_closed_at(_Proxy(closed_dict))

                persist_task = self._regime_guard.on_signal_closed(
                    signal_id=signal_id,
                    family=family,
                    venue=venue,
                    symbol=symbol,
                    timeframe=timeframe,
                    r_value=r_value,
                    closed_at=closed_at,
                )

                if callable(persist_task) and callable(submit_persist_task_fn):
                    submit_persist_task_fn(
                        persist_task, {"family": family, "venue": venue}
                    )
            except Exception as e:
                self._logger.warning("regime guard update failed: %s", e)
