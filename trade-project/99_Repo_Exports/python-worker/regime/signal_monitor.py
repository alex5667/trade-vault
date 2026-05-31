from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .crypto_conf_scorer import CryptoConfScorer, CryptoConfScorerConfig


@dataclass
class SignalQualityMetrics:
    """Метрики качества сигнала с L3-метриками"""
    signal_id: str
    symbol: str
    family: str
    timestamp: datetime
    raw_score: float
    final_score: float
    l3_confidence: float

    # L3-метрики
    spread_bps: float = 0.0
    obi_5: float = 0.0
    obi_20: float = 0.0
    obi_50: float = 0.0
    obi_persistence_score: float = 0.0
    cancel_to_trade_bid_5s: float = 0.0
    cancel_to_trade_ask_5s: float = 0.0
    cancel_to_trade_bid_20s: float = 0.0
    cancel_to_trade_ask_20s: float = 0.0
    microprice_shift_bps_20: float = 0.0
    microprice_velocity_bps: float = 0.0
    queue_pressure_bid: float = 0.0
    queue_pressure_ask: float = 0.0
    market_depth_imbalance: float = 0.0

    # Результат (если известен)
    pnl_r: float | None = None
    is_win: bool | None = None


@dataclass
class QualityStats:
    """Статистика качества по символу/семейству"""
    symbol: str
    family: str
    total_signals: int = 0
    win_rate: float = 0.0
    avg_raw_score: float = 0.0
    avg_final_score: float = 0.0
    avg_l3_confidence: float = 0.0
    avg_spread_bps: float = 0.0
    avg_obi_5: float = 0.0
    signals_with_results: int = 0

    # Корреляции
    l3_vs_win_rate: float = 0.0
    spread_vs_win_rate: float = 0.0
    obi_vs_win_rate: float = 0.0


class SignalQualityMonitor:
    """
    Мониторинг качества сигналов с L3-метриками.
    Анализирует корреляции между L3-метриками и результатами сигналов.
    """

    def __init__(self, max_history_days: int = 30):
        self.logger = logging.getLogger("SignalQualityMonitor")
        self.max_history_days = max_history_days

        # История сигналов: (symbol, family) -> deque[SignalQualityMetrics]
        self.signal_history: dict[tuple[str, str], deque[SignalQualityMetrics]] = defaultdict(
            lambda: deque(maxlen=1000)
        )

        # Текущая статистика
        self.current_stats: dict[tuple[str, str], QualityStats] = {}

        # Conf scorer для анализа (опционально)
        try:
            from .crypto_conf_scorer import L3Profile, L3Thresholds
            # Создаем базовую конфигурацию по умолчанию
            default_thresholds = L3Thresholds(
                spread_max_ok_bps=5.0,
                spread_hard_limit_bps=20.0,
                cancel_soft=0.3,
                cancel_hard=0.7,
                obi_good_min=0.6,
                obi_bad_max=0.3,
                mp_drift_max_bps=2.0,
            )
            default_profile = L3Profile(l3=default_thresholds)
            config = CryptoConfScorerConfig(
                default_profile=default_profile,
                by_symbol={}
            )
            self.conf_scorer = CryptoConfScorer(config)
        except Exception as e:
            self.logger.warning(f"⚠️ CryptoConfScorer unavailable: {e}. Continuing without L3 analysis.")
            self.conf_scorer = None

    def record_signal(
        self,
        signal_id: str,
        symbol: str,
        family: str,
        ctx: Any,  # SignalContext с L3-метриками
        raw_score: float,
        final_score: float,
    ) -> None:
        """Записать сигнал для мониторинга."""

        # Вычислить L3 confidence
        l3_confidence = self.conf_scorer(ctx, symbol)

        metrics = SignalQualityMetrics(
            signal_id=signal_id,
            symbol=symbol,
            family=family,
            timestamp=datetime.now(),
            raw_score=raw_score,
            final_score=final_score,
            l3_confidence=l3_confidence,
            spread_bps=getattr(ctx, 'spread_bps', 0.0),
            obi_5=getattr(ctx, 'obi_5', 0.0),
            obi_20=getattr(ctx, 'obi_20', 0.0),
            obi_50=getattr(ctx, 'obi_50', 0.0),
            obi_persistence_score=getattr(ctx, 'obi_persistence_score', 0.0),
            cancel_to_trade_bid_5s=getattr(ctx, 'cancel_to_trade_bid_5s', 0.0),
            cancel_to_trade_ask_5s=getattr(ctx, 'cancel_to_trade_ask_5s', 0.0),
            cancel_to_trade_bid_20s=getattr(ctx, 'cancel_to_trade_bid_20s', 0.0),
            cancel_to_trade_ask_20s=getattr(ctx, 'cancel_to_trade_ask_20s', 0.0),
            microprice_shift_bps_20=getattr(ctx, 'microprice_shift_bps_20', 0.0),
            microprice_velocity_bps=getattr(ctx, 'microprice_velocity_bps', 0.0),
            queue_pressure_bid=getattr(ctx, 'queue_pressure_bid', 0.0),
            queue_pressure_ask=getattr(ctx, 'queue_pressure_ask', 0.0),
            market_depth_imbalance=getattr(ctx, 'market_depth_imbalance', 0.0),
        )

        key = (symbol, family)
        self.signal_history[key].append(metrics)

        # Очистить старые записи
        cutoff_date = datetime.now() - timedelta(days=self.max_history_days)
        while self.signal_history[key] and self.signal_history[key][0].timestamp < cutoff_date:
            self.signal_history[key].popleft()

        self.logger.debug(f"Recorded signal {signal_id} for {symbol}:{family}")

    def record_result(self, signal_id: str, pnl_r: float) -> None:
        """Записать результат сигнала."""

        is_win = pnl_r > 0

        # Найти сигнал во всех историях
        for key, history in self.signal_history.items():
            for signal in history:
                if signal.signal_id == signal_id:
                    signal.pnl_r = pnl_r
                    signal.is_win = is_win
                    self.logger.debug(f"Recorded result for signal {signal_id}: {pnl_r:+.2f}R ({'WIN' if is_win else 'LOSS'})")
                    break

    def update_stats(self) -> dict[tuple[str, str], QualityStats]:
        """Обновить статистику качества для всех символов/семейств."""

        for key, history in self.signal_history.items():
            symbol, family = key

            if not history:
                continue

            # Фильтруем сигналы с результатами
            signals_with_results = [s for s in history if s.pnl_r is not None]
            if not signals_with_results:
                continue

            total_signals = len(history)
            signals_with_results_count = len(signals_with_results)

            # Базовые метрики
            wins = sum(1 for s in signals_with_results if s.is_win)
            win_rate = wins / signals_with_results_count if signals_with_results_count > 0 else 0.0

            avg_raw_score = sum(s.raw_score for s in history) / total_signals
            avg_final_score = sum(s.final_score for s in history) / total_signals
            avg_l3_confidence = sum(s.l3_confidence for s in history) / total_signals
            avg_spread_bps = sum(s.spread_bps for s in history) / total_signals
            avg_obi_5 = sum(s.obi_5 for s in history) / total_signals

            # Корреляции (простая оценка)
            l3_vs_win = self._calculate_correlation(
                [s.l3_confidence for s in signals_with_results],
                [1.0 if s.is_win else 0.0 for s in signals_with_results]
            )
            spread_vs_win = self._calculate_correlation(
                [s.spread_bps for s in signals_with_results],
                [1.0 if s.is_win else 0.0 for s in signals_with_results]
            )
            obi_vs_win = self._calculate_correlation(
                [s.obi_5 for s in signals_with_results],
                [1.0 if s.is_win else 0.0 for s in signals_with_results]
            )

            stats = QualityStats(
                symbol=symbol,
                family=family,
                total_signals=total_signals,
                win_rate=win_rate,
                avg_raw_score=avg_raw_score,
                avg_final_score=avg_final_score,
                avg_l3_confidence=avg_l3_confidence,
                avg_spread_bps=avg_spread_bps,
                avg_obi_5=avg_obi_5,
                signals_with_results=signals_with_results_count,
                l3_vs_win_rate=l3_vs_win,
                spread_vs_win_rate=spread_vs_win,
                obi_vs_win_rate=obi_vs_win,
            )

            self.current_stats[key] = stats

        return dict(self.current_stats)

    def _calculate_correlation(self, x_values: list[float], y_values: list[float]) -> float:
        """Простая корреляция Пирсона."""
        if len(x_values) != len(y_values) or len(x_values) < 2:
            return 0.0

        try:
            n = len(x_values)
            sum_x = sum(x_values)
            sum_y = sum(y_values)
            sum_xy = sum(x * y for x, y in zip(x_values, y_values))
            sum_x2 = sum(x * x for x in x_values)
            sum_y2 = sum(y * y for y in y_values)

            numerator = n * sum_xy - sum_x * sum_y
            denominator = ((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2)) ** 0.5

            if denominator == 0:
                return 0.0

            return numerator / denominator
        except Exception:
            return 0.0

    def get_quality_report(self, symbol: str | None = None, family: str | None = None) -> str:
        """Сгенерировать отчет о качестве сигналов."""

        self.update_stats()

        lines = ["📊 Signal Quality Report", "=" * 50]

        for key, stats in self.current_stats.items():
            sym, fam = key

            if symbol and sym != symbol:
                continue
            if family and fam != family:
                continue

            lines.extend([
                f"\n🔸 {sym}:{fam}",
                f"   Signals: {stats.total_signals} total, {stats.signals_with_results} with results",
                f"   Win Rate: {stats.win_rate:.1%}",
                f"   Avg Raw Score: {stats.avg_raw_score:.2f}",
                f"   Avg Final Score: {stats.avg_final_score:.2f}",
                f"   Avg L3 Confidence: {stats.avg_l3_confidence:.2f}",
                f"   Avg Spread: {stats.avg_spread_bps:.1f} bps",
                f"   Avg OBI-5: {stats.avg_obi_5:.3f}",
                "   Correlations:",
                f"     L3 vs Win Rate: {stats.l3_vs_win_rate:.3f}",
                f"     Spread vs Win Rate: {stats.spread_vs_win_rate:.3f}",
                f"     OBI vs Win Rate: {stats.obi_vs_win_rate:.3f}",
            ])

        if len(lines) == 2:
            lines.append("\n❌ No data available")

        return "\n".join(lines)

    def get_alerts(self) -> list[str]:
        """Получить алерты о проблемах качества."""

        self.update_stats()
        alerts = []

        for key, stats in self.current_stats.items():
            symbol, family = key

            # Проверка низкого качества L3-метрик
            if stats.avg_l3_confidence < -0.5:
                alerts.append(
                    f"⚠️  {symbol}:{family} - Poor L3 confidence: {stats.avg_l3_confidence:.2f}"
                )

            # Проверка низкой корреляции L3 с результатами
            if abs(stats.l3_vs_win_rate) < 0.1 and stats.signals_with_results > 10:
                alerts.append(
                    f"⚠️  {symbol}:{family} - Weak L3-win correlation: {stats.l3_vs_win_rate:.3f}"
                )

            # Проверка экстремального спреда
            if stats.avg_spread_bps > 15.0:
                alerts.append(
                    f"⚠️  {symbol}:{family} - High average spread: {stats.avg_spread_bps:.1f} bps"
                )

        return alerts
