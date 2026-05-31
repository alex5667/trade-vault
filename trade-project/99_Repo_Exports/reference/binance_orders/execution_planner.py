"""
ExecutionPlanner: Risk-based execution planning for signals.

Builds detailed execution plans from SignalContext with:
- Microstructural stop placement
- Entry zones in R multiples
- Dynamic position sizing based on confidence scores
- TTD-aware expiry times

Production-ready for scanner_infra integration.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping, Tuple

from .context import SignalContext
from .models import ExecutionPlan, Side, SymbolSetupConfig




class ExecutionPlanner:
    """
    Builds execution plans from SignalContext:
    - Stop behind microstructural levels + ATR buffer
    - Entry zones in R multiples
    - Dynamic risk sizing based on FinalScore
    - TTD-aware expiry bars
    """

    def __init__(self, setup_configs: Mapping[Tuple[str, str], SymbolSetupConfig]):
        self._configs = setup_configs

    # --- Public API ---

    def build_plan(self, ctx: SignalContext) -> ExecutionPlan | None:
        """
        Build execution plan from SignalContext.

        All required fields are already in SignalContext dataclass.
        """
        cfg = self._get_config(ctx.symbol, ctx.setup_type)
        if cfg is None:
            return None

        side = ctx.side
        atr = max(ctx.atr_1m, 1e-6)

        # 1) Stop behind microstructural level
        stop_price, stop_R = self._compute_stop(ctx, side, atr, cfg)
        if stop_price is None:
            return None

        if stop_R > cfg.max_stop_R:
            # Stop too wide → don't trade
            return None

        # 2) Entry zone in R
        entry_low, entry_high = self._compute_entry_zone(
            side=side,
            stop_price=stop_price,
            price_at_signal=ctx.price_at_signal,
            atr=atr,
            cfg=cfg,
        )

        # 3) Dynamic risk based on FinalScore
        risk_R = self._compute_risk_R(ctx.final_score, cfg)

        if risk_R <= 0:
            return None

        # 4) Portfolio risk check
        risk_usd, position_size = self._compute_position_size(
            ctx=ctx,
            stop_price=stop_price,
            entry_price=(entry_low + entry_high) / 2.0,
            desired_risk_R=risk_R,
            cfg=cfg,
        )
        if risk_usd <= 0 or position_size <= 0:
            return None

        # 5) TP levels (in R from stop)
        tp_levels = self._compute_tp_levels(
            side=side,
            stop_price=stop_price,
            entry_price=(entry_low + entry_high) / 2.0,
            tp_Rs=cfg.default_tp_R,
            atr=atr,
        )
        partials = [0.33, 0.33, 0.34]  # example: 3 exits ~1/3 each

        # 6) Signal lifetime
        expiry_bars = self._resolve_expiry_bars(ctx, cfg)

        plan = ExecutionPlan(
            signal_id=ctx.signal_id,
            symbol=ctx.symbol,
            side=side,
            setup_type=ctx.setup_type,
            ts_signal=ctx.ts_signal,
            price_at_signal=ctx.price_at_signal,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            stop_price=stop_price,
            tp_levels=tp_levels,
            partials=partials,
            pos_risk_R=risk_R,
            risk_usd=risk_usd,
            position_size=position_size,
            expiry_bars=expiry_bars,
            created_at=datetime.now(timezone.utc),
            meta={},
        )
        return plan

    # --- Internal methods ---

    def _get_config(self, symbol: str, setup_type: str) -> SymbolSetupConfig | None:
        return self._configs.get((symbol, setup_type))



    def _compute_stop(
        self,
        ctx: SignalContext,
        side: Side,
        atr: float,
        cfg: SymbolSetupConfig,
    ) -> tuple[float | None, float]:
        """
        Stop behind microstructural level:
        - Find nearest swing (low for LONG, high for SHORT) in recent bars
        - Add ATR buffer
        - Fallback: price_at_signal ± min_stop_ticks * tick_size
        Returns (stop_price, stop in R).
        """
        tick = max(ctx.tick_size, 1e-6)
        price0 = ctx.price_at_signal

        swings: list[SwingPoint] = getattr(ctx, "local_swings", []) or []
        relevant_type = "low" if side == Side.LONG else "high"

        swing_price: float | None = None
        for sp in sorted(swings, key=lambda x: x.ts, reverse=True):
            if sp.type == relevant_type:
                swing_price = sp.price
                break

        if swing_price is not None:
            if side == Side.LONG:
                raw_stop = swing_price - cfg.atr_buffer_ratio * atr
            else:
                raw_stop = swing_price + cfg.atr_buffer_ratio * atr
        else:
            # Fallback: minimum technical stop from signal price
            if side == Side.LONG:
                raw_stop = price0 - cfg.min_stop_ticks * tick
            else:
                raw_stop = price0 + cfg.min_stop_ticks * tick

        # Round to tick size
        stop_price = round(round(raw_stop / tick) * tick, 10)

        # Stop in R: |price0 - stop| / ATR
        stop_R = abs(price0 - stop_price) / max(atr, 1e-6)
        return stop_price, stop_R

    def _compute_entry_zone(
        self,
        side: Side,
        stop_price: float,
        price_at_signal: float,
        atr: float,
        cfg: SymbolSetupConfig,
    ) -> tuple[float, float]:
        """
        Entry zone in R relative to stop.
        For LONG: R = (entry - stop) / ATR
        For SHORT: R = (stop - entry) / ATR
        """
        if side == Side.LONG:
            low = stop_price + cfg.entry_zone_min_R * atr
            high = stop_price + cfg.entry_zone_max_R * atr
        else:
            high = stop_price - cfg.entry_zone_min_R * atr
            low = stop_price - cfg.entry_zone_max_R * atr

        return min(low, high), max(low, high)

    @staticmethod
    def _compute_risk_R(score: float, cfg: SymbolSetupConfig) -> float:
        """
        Map FinalScore → risk multiplier.
        Example:
          score < b0      → 0.5R
          b0 ≤ score < b1 → 1.0R
          b1 ≤ score < b2 → 1.5R
          b2 ≤ score      → 2.0R
        """
        b0, b1, b2 = cfg.score_buckets
        m0, m1, m2, m3 = cfg.risk_multipliers

        if score < b0:
            mult = m0
        elif score < b1:
            mult = m1
        elif score < b2:
            mult = m2
        else:
            mult = m3

        risk_R = min(mult, cfg.max_risk_R_per_trade)
        return max(risk_R, 0.0)

    def _compute_position_size(
        self,
        ctx: SignalContext,
        stop_price: float,
        entry_price: float,
        desired_risk_R: float,
        cfg: SymbolSetupConfig,
    ) -> tuple[float, float]:
        """
        Convert desired_risk_R → USD → lots.
        Contract: ctx.contract_size (e.g., 100 oz for XAU)
        """
        acc: AccountState = ctx.account_state
        atr = max(ctx.atr_1m, 1e-6)

        # 1R in USD
        r_usd = (desired_risk_R * atr) * ctx.contract_size

        # Per-trade limit (% of equity)
        deal_limit_usd = acc.equity_usd * (acc.max_risk_per_trade_pct / 100.0)

        # Portfolio limit
        portfolio_limit_usd = acc.equity_usd * (cfg.max_portfolio_risk_pct / 100.0)
        remaining_portfolio = max(portfolio_limit_usd - acc.open_risk_usd, 0.0)

        max_risk_usd = min(r_usd, deal_limit_usd, remaining_portfolio)
        if max_risk_usd <= 0:
            return 0.0, 0.0

        # Stop price in USD per contract
        stop_per_contract = abs(entry_price - stop_price) * ctx.contract_size
        if stop_per_contract <= 0:
            return 0.0, 0.0

        position_size = max_risk_usd / stop_per_contract
        return max_risk_usd, position_size

    @staticmethod
    def _compute_tp_levels(
        side: Side,
        stop_price: float,
        entry_price: float,
        tp_Rs: Tuple[float, float, float],
        atr: float,
    ) -> list[float]:
        """
        Convert TP in R → real prices.
        """
        levels: list[float] = []
        for r in tp_Rs:
            if side == Side.LONG:
                level = stop_price + r * atr
            else:
                level = stop_price - r * atr
            levels.append(level)
        return levels

    @staticmethod
    def _resolve_expiry_bars(ctx: SignalContext, cfg: SymbolSetupConfig) -> int:
        """
        If ctx.ttd_expiry_bars exists (from TTD table), use it.
        Otherwise use cfg.expiry_bars.
        """
        v = getattr(ctx, "ttd_expiry_bars", None)
        if isinstance(v, int) and v > 0:
            return v
        return cfg.expiry_bars