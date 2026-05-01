from __future__ import annotations
"""
Signal Performance Tracker: analyzes executed signals for TTD, MFE/MAE, and outcomes.
Calculates performance metrics from historical trade data.
"""


from typing import List, Optional, Tuple
from datetime import datetime

from .models import ExtendedSignalContext, SignalPerformance, Side, Bar1m


class SignalPerformanceTracker:
    """
    Оффлайн-анализ (или батч-процесс) для расчёта:
    - TTD (bars, seconds)
    - MFE/MAE в R
    - итогового результата сделки в R
    """

    def __init__(self, r_target: float = 1.0, max_ttd_bars: int = 30):
        """
        Args:
            r_target: Target R multiple for TTD calculation (default 1.0R)
            max_ttd_bars: Maximum bars to look ahead for TTD (default 30 bars)
        """
        self.r_target = r_target
        self.max_ttd_bars = max_ttd_bars

    def _compute_ttd_bars(
        self,
        side: Side,
        entry_price: float,
        stop_price: float,
        bars_after_entry: List[Bar1m],
    ) -> Optional[int]:
        """
        TTD: через сколько баров max_favorable_excursion впервые >= r_target R.
        Если не произошло — возвращаем None.

        Args:
            side: Trade direction
            entry_price: Entry price
            stop_price: Stop loss price
            bars_after_entry: 1m bars after entry

        Returns:
            Number of bars to reach R target, or None if not reached
        """
        if not bars_after_entry:
            return None

        R = abs(entry_price - stop_price)
        if R <= 0:
            return None

        best_price = entry_price
        for i, bar in enumerate(bars_after_entry, start=1):
            if side == Side.LONG:
                best_price = max(best_price, bar.high)
                mfe_R = (best_price - entry_price) / R
            else:
                best_price = min(best_price, bar.low)
                mfe_R = (entry_price - best_price) / R

            if mfe_R >= self.r_target:
                return i

            if i >= self.max_ttd_bars:
                break

        return None

    def _compute_mfe_mae_R(
        self,
        side: Side,
        entry_price: float,
        stop_price: float,
        bars_during_trade: List[Bar1m],
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate MFE (Max Favorable Excursion) and MAE (Max Adverse Excursion) in R.

        Args:
            side: Trade direction
            entry_price: Entry price
            stop_price: Stop loss price
            bars_during_trade: 1m bars during the trade

        Returns:
            Tuple of (MFE_R, MAE_R) or (None, None) if no bars
        """
        if not bars_during_trade:
            return None, None

        R = abs(entry_price - stop_price)
        if R <= 0:
            return None, None

        mfe_price = entry_price
        mae_price = entry_price

        for bar in bars_during_trade:
            if side == Side.LONG:
                mfe_price = max(mfe_price, bar.high)
                mae_price = min(mae_price, bar.low)
            else:
                mfe_price = min(mfe_price, bar.low)
                mae_price = max(mae_price, bar.high)

        if side == Side.LONG:
            mfe_R = (mfe_price - entry_price) / R
            mae_R = (mae_price - entry_price) / R
        else:
            mfe_R = (entry_price - mfe_price) / R
            mae_R = (entry_price - mae_price) / R

        return mfe_R, mae_R

    def _compute_realized_R(
        self,
        side: Side,
        entry_price: float,
        stop_price: float,
        exit_price: float,
    ) -> Optional[float]:
        """
        Calculate realized R for the trade.

        Args:
            side: Trade direction
            entry_price: Entry price
            stop_price: Stop loss price
            exit_price: Exit price

        Returns:
            Realized R or None if calculation impossible
        """
        R = abs(entry_price - stop_price)
        if R <= 0:
            return None

        if side == Side.LONG:
            return (exit_price - entry_price) / R
        else:
            return (entry_price - exit_price) / R

    def build_performance(
        self,
        ctx: ExtendedSignalContext,
        bars: List[Bar1m],
        entry_ts: Optional[datetime],
        exit_ts: Optional[datetime],
        entry_price: Optional[float],
        exit_price: Optional[float],
        stop_price: Optional[float],
        expired_without_entry: bool = False,
    ) -> SignalPerformance:
        """
        Build SignalPerformance from signal context and execution data.

        Args:
            ctx: Extended signal context
            bars: Continuous series of 1m bars >= ts_signal
            entry_ts: When position was entered
            exit_ts: When position was closed
            entry_price: Entry price
            exit_price: Exit price
            stop_price: Stop loss price
            expired_without_entry: True if signal expired without entry

        Returns:
            Complete SignalPerformance object
        """
        # Базовые значения
        ts_signal = ctx.ts_signal

        # Outcome-кейс 1: сигнал протух, входа нет
        if expired_without_entry or entry_ts is None or entry_price is None:
            perf = SignalPerformance(
                signal_id=ctx.signal_id,
                symbol=ctx.symbol,
                side=ctx.side,
                setup_type=ctx.setup_type,
                ts_signal=ts_signal,
                ts_entry=None,
                ts_exit=None,
                price_at_signal=ctx.price_at_signal,
                entry_price=None,
                exit_price=None,
                stop_price=stop_price,
                realized_R=None,
                mfe_R=None,
                mae_R=None,
                ttd_bars=None,
                ttd_seconds=None,
                outcome="expired",
                bars_to_entry=None,
                bars_to_exit=None,
                notes="Signal expired without entry",
            )
            return perf

        # Фильтруем бары
        bars_after_signal = [b for b in bars if b.ts >= ts_signal]
        bars_after_entry = [b for b in bars if b.ts >= entry_ts]
        bars_during_trade = [b for b in bars if entry_ts <= b.ts <= (exit_ts or b.ts)]

        # bars_to_entry / bars_to_exit
        bars_to_entry = sum(1 for b in bars_after_signal if ts_signal <= b.ts <= entry_ts)
        bars_to_exit = None
        if exit_ts:
            bars_to_exit = sum(1 for b in bars_after_signal if ts_signal <= b.ts <= exit_ts)

        # TTD
        ttd_bars = self._compute_ttd_bars(ctx.side, entry_price, stop_price, bars_after_entry)
        ttd_seconds = None
        if ttd_bars is not None and bars_after_entry:
            # 1 бар ~ 60 сек (если 1m), можно уточнить по фактическому времени
            ttd_seconds = ttd_bars * 60.0

        # MFE/MAE
        mfe_R, mae_R = self._compute_mfe_mae_R(ctx.side, entry_price, stop_price, bars_during_trade)

        # Realized R
        realized_R = None
        if exit_ts is not None and exit_price is not None:
            realized_R = self._compute_realized_R(ctx.side, entry_price, stop_price, exit_price)

        # Outcome
        outcome = "unknown"
        if exit_ts is not None and exit_price is not None:
            # Примитивная логика: если exit_price ≈ stop_price
            if abs(exit_price - stop_price) < ctx.tick_size * 2:
                outcome = "stopped"
            elif realized_R is not None and realized_R > 0:
                outcome = "realized"
            else:
                outcome = "closed"
        else:
            outcome = "open"  # сделка ещё открыта

        perf = SignalPerformance(
            signal_id=ctx.signal_id,
            symbol=ctx.symbol,
            side=ctx.side,
            setup_type=ctx.setup_type,
            ts_signal=ts_signal,
            ts_entry=entry_ts,
            ts_exit=exit_ts,
            price_at_signal=ctx.price_at_signal,
            entry_price=entry_price,
            exit_price=exit_price,
            stop_price=stop_price,
            realized_R=realized_R,
            mfe_R=mfe_R,
            mae_R=mae_R,
            ttd_bars=ttd_bars,
            ttd_seconds=ttd_seconds,
            outcome=outcome,
            bars_to_entry=bars_to_entry,
            bars_to_exit=bars_to_exit,
            notes=None,
        )
        return perf
