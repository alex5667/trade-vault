"""
Execution Planner: builds detailed execution plans for signals.
Includes risk management, entry zones, stop levels, and TP targets.
"""

from __future__ import annotations

from typing import Tuple, List, Optional

from .models import (
    ExtendedSignalContext,
    ExecutionPlan,
    Side,
    SwingPoint,
    SymbolSetupConfig,
)
from .setup_config import ExecutionSetupRepository


class ExecutionPlanner:
    """
    Планировщик исполнения сигналов.
    Строит ExecutionPlan из ExtendedSignalContext с учетом риска, микроструктуры и TTD.
    """

    def __init__(
        self,
        setup_repo: ExecutionSetupRepository,
    ):
        """
        setup_repo: репозиторий конфигураций сетапов.
        """
        self.setup_repo = setup_repo

    def _get_config(self, ctx: ExtendedSignalContext) -> SymbolSetupConfig:
        """
        Получить конфигурацию для данного symbol/setup_type.
        Если из контекста пришел ttd_expiry_bars — перекрываем expiry_bars.
        """
        cfg = self.setup_repo.get_by_name(ctx.symbol, ctx.setup_type)

        # Если из контекста пришел ttd_expiry_bars — перекрываем expiry_bars
        if ctx.ttd_expiry_bars is not None:
            cfg = SymbolSetupConfig(**{**cfg.__dict__, "expiry_bars": ctx.ttd_expiry_bars})

        return cfg

    # ---------- Микроструктурные уровни для стопа ----------

    def _select_support_level(self, ctx: ExtendedSignalContext) -> Optional[float]:
        """
        Для long: ищем наиболее "толковый" минимум ниже текущей цены.
        Критерии:
        - type == 'low'
        - price < price_at_signal
        - сортируем по:
            1) максимальному volume,
            2) максимальной delta (купили много),
            3) близости к текущей цене
        """
        lows = [s for s in ctx.local_swings if s.type == "low" and s.price < ctx.price_at_signal]
        if not lows:
            return None

        # Простой скор: volume * 0.7 + max(delta, 0) * 0.3, penalize distance
        def score(sp: SwingPoint) -> float:
            vol_score = sp.volume
            delta_score = max(sp.delta, 0.0)
            dist = ctx.price_at_signal - sp.price + 1e-6
            return 0.7 * vol_score + 0.3 * delta_score - 0.1 * dist

        best = max(lows, key=score)
        return best.price

    def _select_resistance_level(self, ctx: ExtendedSignalContext) -> Optional[float]:
        """
        Для short: зеркально _select_support_level.
        """
        highs = [s for s in ctx.local_swings if s.type == "high" and s.price > ctx.price_at_signal]
        if not highs:
            return None

        def score(sp: SwingPoint) -> float:
            vol_score = sp.volume
            delta_score = max(-sp.delta, 0.0)  # для short ищем продавцов
            dist = sp.price - ctx.price_at_signal + 1e-6
            return 0.7 * vol_score + 0.3 * delta_score - 0.1 * dist

        best = max(highs, key=score)
        return best.price

    def _build_stop_price(self, ctx: ExtendedSignalContext, cfg: SymbolSetupConfig) -> float:
        """
        Построить уровень стопа на основе микроструктуры и ATR.
        """
        tick = ctx.tick_size
        atr_buf_price = cfg.atr_buffer_ratio * ctx.atr_1m if ctx.atr_1m > 0 else 0.0
        min_stop_price_move = max(cfg.min_stop_ticks * tick, atr_buf_price)

        if ctx.side == Side.LONG:
            support = self._select_support_level(ctx)
            if support is None:
                # fallback: просто ATR ниже
                return ctx.price_at_signal - max(min_stop_price_move, ctx.atr_1m)
            stop = support - min_stop_price_move
        else:
            resistance = self._select_resistance_level(ctx)
            if resistance is None:
                return ctx.price_at_signal + max(min_stop_price_move, ctx.atr_1m)
            stop = resistance + min_stop_price_move

        return stop

    def _build_entry_zone(self, ctx: ExtendedSignalContext, cfg: SymbolSetupConfig, stop_price: float) -> Tuple[float, float]:
        """
        Entry зона в R относительно стопа.
        Для long: SupportLevel мы уже использовали для стопа, но R посчитаем от price_at_signal.
        """
        if ctx.side == Side.LONG:
            R = ctx.price_at_signal - stop_price
            if R <= 0:
                R = max(ctx.atr_1m, ctx.tick_size * cfg.min_stop_ticks)
            zone_low = stop_price + cfg.entry_zone_min_R * R
            zone_high = stop_price + cfg.entry_zone_max_R * R
        else:
            R = stop_price - ctx.price_at_signal
            if R <= 0:
                R = max(ctx.atr_1m, ctx.tick_size * cfg.min_stop_ticks)
            zone_high = stop_price - cfg.entry_zone_min_R * R
            zone_low = stop_price - cfg.entry_zone_max_R * R

        # Упростим: гарантируем zone_low < zone_high
        if zone_low > zone_high:
            zone_low, zone_high = zone_high, zone_low

        return zone_low, zone_high

    def _select_htf_targets(self, ctx: ExtendedSignalContext, entry_ref_price: float) -> List[float]:
        """
        Из списка HTFLevels выбираем несколько ближайших уровней по направлению сделки.
        """
        levels = []
        if not ctx.htf_levels:
            return levels

        if ctx.side == Side.LONG:
            candidates = [lvl.price for lvl in ctx.htf_levels if lvl.price > entry_ref_price]
            levels = sorted(candidates)[:3]
        else:
            candidates = [lvl.price for lvl in ctx.htf_levels if lvl.price < entry_ref_price]
            levels = sorted(candidates, reverse=True)[:3]

        return levels

    def _build_tp_levels(
        self,
        ctx: ExtendedSignalContext,
        cfg: SymbolSetupConfig,
        stop_price: float,
        entry_zone_low: float,
        entry_zone_high: float,
    ) -> List[float]:
        """
        Построить уровни тейк-профита: HTF-уровни + дефолтные R-based цели.
        """
        # Берем референсный вход как середину зоны
        entry_ref = 0.5 * (entry_zone_low + entry_zone_high)
        R = abs(entry_ref - stop_price)
        if R <= 0:
            R = max(ctx.atr_1m, ctx.tick_size * cfg.min_stop_ticks)

        # 1) HTF цели
        htf_targets = self._select_htf_targets(ctx, entry_ref)

        # 2) R-бейсд цели
        tp_from_R: List[float] = []
        for r_mult in cfg.default_tp_R:
            if ctx.side == Side.LONG:
                tp_from_R.append(entry_ref + r_mult * R)
            else:
                tp_from_R.append(entry_ref - r_mult * R)

        # Слить всё в список, убрав дубликаты и отсортировав
        all_targets = sorted(set(htf_targets + tp_from_R))

        # Оставим максимум 3
        if len(all_targets) > 3:
            if ctx.side == Side.LONG:
                all_targets = all_targets[:3]
            else:
                all_targets = all_targets[:3]  # уже отсортированы сверху вниз

        return all_targets

    def _score_to_risk_mult(self, final_score: float, cfg: SymbolSetupConfig) -> float:
        """
        Ступенчатая функция score -> risk_mult.
        buckets: (b1, b2, b3), risk_multipliers: (m_low, m_mid, m_high, m_top)
        """
        b1, b2, b3 = cfg.score_buckets
        m_low, m_mid, m_high, m_top = cfg.risk_multipliers

        s = final_score

        if s < b1:
            return m_low
        elif s < b2:
            return m_mid
        elif s < b3:
            return m_high
        else:
            return m_top

    def _compute_position_size(
        self,
        ctx: ExtendedSignalContext,
        cfg: SymbolSetupConfig,
        stop_price: float,
    ) -> Tuple[float, float, float]:
        """
        Рассчитать размер позиции на основе риска и состояния счета.

        Возвращает: (pos_risk_R, risk_usd, position_size).
        """
        if ctx.account_state is None:
            raise ValueError("AccountState is required for risk sizing")

        acc = ctx.account_state

        # Базовый риск per trade в % от equity
        base_risk_pct = acc.max_risk_per_trade_pct  # например 0.5

        # risk_mult от скора
        risk_mult = self._score_to_risk_mult(ctx.final_score, cfg)

        # итоговый риск по сделке в %
        total_risk_pct = min(base_risk_pct * risk_mult, cfg.max_risk_R_per_trade * base_risk_pct)

        # лимит по портфелю: если уже много риска, можем зажать
        max_portfolio_risk_usd = acc.equity_usd * cfg.max_portfolio_risk_pct / 100.0
        remaining_risk_usd = max(max_portfolio_risk_usd - acc.open_risk_usd, 0.0)

        risk_usd = acc.equity_usd * total_risk_pct / 100.0
        if risk_usd > remaining_risk_usd > 0:
            risk_usd = remaining_risk_usd
        elif remaining_risk_usd <= 0:
            # Нет свободного риска — решение за вами (можно выкинуть сигнал)
            risk_usd = 0.0

        # R в ценовых пунктах:
        R_price = abs(ctx.price_at_signal - stop_price)
        if R_price <= 0:
            R_price = max(ctx.atr_1m, ctx.tick_size * cfg.min_stop_ticks)

        # Приблизительный размер позиции:
        # risk_usd ≈ R_price * contract_size * position_size
        if R_price <= 0:
            position_size = 0.0
        else:
            position_size = risk_usd / (R_price * ctx.contract_size)

        # pos_risk_R: сколько R мы рискуем от баланса (по сути == total_risk_pct, но в "R")
        pos_risk_R = risk_mult  # можно считать, что base_risk = 1R

        return pos_risk_R, risk_usd, position_size

    def build_plan(self, ctx: ExtendedSignalContext) -> Optional[ExecutionPlan]:
        """
        Основной метод: из ExtendedSignalContext => ExecutionPlan.
        Если сетап не проходит проверки (слишком широкий стоп, нет риска и т.п.) — можно вернуть None.
        """
        cfg = self._get_config(ctx)

        # 1) Стоп
        stop_price = self._build_stop_price(ctx, cfg)

        # 2) Проверка максимального размера стопа в R относительно ATR
        R_price = abs(ctx.price_at_signal - stop_price)
        if ctx.atr_1m > 0 and R_price > cfg.max_stop_R * ctx.atr_1m:
            # Стоп слишком широкий — сетап отбрасываем
            return None

        # 3) Entry зона
        entry_low, entry_high = self._build_entry_zone(ctx, cfg, stop_price)

        # 4) TP уровни
        tp_levels = self._build_tp_levels(ctx, cfg, stop_price, entry_low, entry_high)

        # 5) Партиалы (простой вариант — по 1/3)
        if tp_levels:
            partials = [1.0 / len(tp_levels)] * len(tp_levels)
        else:
            # если по какой-то причине нет TP — ставим один с полной позицией
            tp_levels = [ctx.price_at_signal + (ctx.atr_1m if ctx.side == Side.LONG else -ctx.atr_1m)]
            partials = [1.0]

        # 6) Риск и размер позиции
        pos_risk_R, risk_usd, position_size = self._compute_position_size(ctx, cfg, stop_price)
        if risk_usd <= 0 or position_size <= 0:
            # Нет свободного риска / получился нулевой размер
            return None

        # 7) expiry_bars
        expiry_bars = cfg.expiry_bars

        plan = ExecutionPlan(
            signal_id=ctx.signal_id,
            symbol=ctx.symbol,
            side=ctx.side,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            stop_price=stop_price,
            tp_levels=tp_levels,
            partials=partials,
            pos_risk_R=pos_risk_R,
            risk_usd=risk_usd,
            position_size=position_size,
            expiry_bars=expiry_bars,
        )
        return plan
