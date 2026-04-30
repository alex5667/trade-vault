#!/usr/bin/env python3
"""
Trailing Edge Analyzer - мини-анализатор pnl_if_fixed_exit vs pnl_net (edge трейлинга).

Анализирует последние N сделок по символу и считает edge трейлинга:
- expectancy managed vs baseline
- доля сделок, где трейлинг улучшил/ухудшил результат
- метрики giveback/missed по трейлинговым сделкам

Интегрирован в PeriodicReporter для автоматической отправки отчетов в Telegram.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import redis

from analytics.tag_stats import Trade
from common.log import setup_logger
from services.trade_closed_hydrator import hydrate_trade_closed_batch
from domain.normalizers import canon_source, canon_symbol
from services.trade_closed_hydrator import hydrate_trade_closed_batch

logger = setup_logger("TrailingEdgeAnalyzer")


@dataclass
class Trade:
    source: str
    symbol: str
    exit_ts_ms: int
    pnl_net: float
    close_reason: str = ""
    close_reason_raw: str = ""
    close_reason_detail: str = ""
    trailing_started: bool = False
    trailing_active: bool = False
    trailing_profile: str = ""
    trail_profile: str = ""


def _norm_map(m: Dict[str, Any]) -> Dict[str, str]:
    """
    Нормализатор входа из Redis stream/hash.
    В проекте decode_responses=True, но на всякий случай приводим всё к str.
    """
    out: Dict[str, str] = {}
    for k, v in (m or {}).items():
        if v is None:
            continue
        out[str(k)] = str(v)
    return out


def _si(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _sf(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _sb(v: Any) -> bool:
    """
    bool из Redis:
      '1'/'0', 'true'/'false', 'True'/'False'
    """
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _build_trade_from_fields(fields: Dict[str, str]) -> Optional[Trade]:
    """
    Строит Trade из ПЛОСКОГО dict.
    Важно: fields уже должен быть "гидрирован" (если stream compact/частичный).
    """
    src = canon_source(fields.get("source") or fields.get("strategy") or "")
    sym = canon_symbol(fields.get("symbol") or "")
    exit_ts = _si(fields.get("exit_ts_ms") or fields.get("closed_time") or 0, 0)
    if exit_ts <= 0:
        return None

    tprof = str(fields.get("trailing_profile") or "")
    aprof = str(fields.get("trail_profile") or tprof or "")
    if not tprof and aprof:
        tprof = aprof

    # ✅ REFACTOR: Robust trailing detection (User Req 4.4)
    # Trailing started IF:
    # 1. Flag is explicitly True
    # 2. OR explicit profile is set
    # 3. OR movements > 0
    # 4. OR close reason bucket is TRAIL_SL (normalization already done externally or needs doing here)
    
    t_started = _sb(fields.get("trailing_started"))
    t_moves = _si(fields.get("trailing_moves_count") or fields.get("trailing_moves") or 0)
    
    # Check close reason/bucket
    raw_cr = (fields.get("close_reason") or "").upper()
    bucket = (fields.get("close_bucket") or raw_cr).upper() # assume hydrated fields might have it
    
    # Is it a trailing exit?
    is_trail_exit = (bucket == "TRAIL_SL") or ("TRAIL" in raw_cr)
    
    trailing_started_final = (t_started) or (aprof != "") or (t_moves > 0) or (is_trail_exit)
    # Note: trailing_started might be True while is_trail_exit is False (e.g. SL hit before trail closed it)
    
    pnl_net_val = _sf(fields.get("pnl_net") or 0.0, 0.0)

    return Trade(
        source=src
        symbol=sym
        exit_ts_ms=exit_ts
        pnl_net=pnl_net_val
        close_reason=raw_cr
        close_reason_raw=str(fields.get("close_reason_raw") or "")
        close_reason_detail=str(fields.get("close_reason_detail") or "")
        trailing_started=trailing_started_final,  # <--- UPDATED
        trailing_active=_sb(fields.get("trailing_active"))
        trailing_profile=tprof, # original field
        trail_profile=aprof,    # combined/fallback
        # we can store is_trail_exit if needed for stricter analysis, but analyzer mainly uses trailing_started filter?
        # Analyzer uses "if t.trailing_started: trailing_trades++"
        # So this fixes "closed_by_trail 0%" if we trust this flag.
    )


@dataclass
class TrailingEdgeResult:
    """Результаты анализа trailing edge для отправки в Telegram."""

    symbol: str
    source: str
    total_trades: int
    trailing_trades: int

    # Expectancy метрики
    exp_managed_r: float
    exp_baseline_r: float
    delta_exp_r: float

    # Доли сравнения
    share_better: float  # managed > baseline
    share_worse: float   # managed < baseline
    share_equal: float   # managed ≈ baseline

    # Средняя разница
    avg_diff_usd: float

    # Диагностика
    analysis_window: str  # "last_200_trades" или "last_24h"

    def to_telegram_message(self) -> str:
        """Форматирует результат для отправки в Telegram."""
        lines = [
            "🎯 <b>Trailing Edge Analysis</b>"
            f"📊 {self.source} / {self.symbol}"
            f"🕐 Окно: {self.analysis_window}"
            ""
            f"📈 Всего сделок: <b>{self.total_trades}</b>"
            f"🎯 Трейлинговых: <b>{self.trailing_trades}</b>"
            ""
            "💰 <b>Expectancy (R)</b>"
            f"  Managed: <b>{self.exp_managed_r:+.3f}</b>"
            f"  Baseline: <b>{self.exp_baseline_r:+.3f}</b>"
            f"  ΔEdge: <b>{self.delta_exp_r:+.3f}</b>"
            ""
            "📊 <b>Распределение сделок</b>"
            f"  Лучше: <b>{self.share_better*100:.1f}%</b>"
            f"  Хуже: <b>{self.share_worse*100:.1f}%</b>"
            f"  Равно: <b>{self.share_equal*100:.1f}%</b>"
            ""
            f"💵 Средняя разница: <b>{self.avg_diff_usd:+.3f} USD</b>"
        ]

        # Добавляем интерпретацию
        if abs(self.delta_exp_r) > 0.1:
            if self.delta_exp_r > 0:
                lines.append("✅ <b>Трейлинг дает преимущество!</b>")
            else:
                lines.append("⚠️ <b>Трейлинг ухудшает результат</b>")
        else:
            lines.append("🤔 <b>Трейлинг нейтрален</b>")

        # Рекомендации
        if self.share_better > 0.6 and self.delta_exp_r > 0.05:
            lines.append("🚀 <i>Рекомендуется усилить трейлинг</i>")
        elif self.share_worse > 0.6 and self.delta_exp_r < -0.05:
            lines.append("🛑 <i>Рекомендуется ослабить трейлинг</i>")
        elif self.trailing_trades < 10:
            lines.append("📝 <i>Недостаточно трейлинговых сделок для анализа</i>")

        return "\n".join(lines)

    def generate_trailing_recommendation(self) -> Optional[Dict[str, Any]]:
        """
        Генерирует рекомендации по настройке трейлинга на основе анализа.

        Returns:
            Словарь с рекомендациями или None если рекомендаций нет
        """
        if self.total_trades < 20:  # Недостаточно данных
            return None

        recommendations = {
            "analysis_timestamp": get_ny_time_millis()
            "confidence_level": "low",  # low, medium, high
            "actions": []
        }

        # Определяем уровень уверенности
        if self.total_trades >= 100:
            recommendations["confidence_level"] = "high"
        elif self.total_trades >= 50:
            recommendations["confidence_level"] = "medium"

        # Логика принятия решений
        strong_positive_edge = (
            self.delta_exp_r > 0.1 and
            self.share_better > 0.6 and
            self.trailing_trades >= 10
        )

        strong_negative_edge = (
            self.delta_exp_r < -0.1 and
            self.share_worse > 0.6 and
            self.trailing_trades >= 10
        )

        moderate_positive_edge = (
            self.delta_exp_r > 0.05 and
            self.share_better > 0.55 and
            self.trailing_trades >= 5
        )

        moderate_negative_edge = (
            self.delta_exp_r < -0.05 and
            self.share_worse > 0.55 and
            self.trailing_trades >= 5
        )

        # Формируем рекомендации
        if strong_positive_edge:
            recommendations["actions"].append({
                "type": "increase_trailing_aggression"
                "reason": f"Strong positive edge: ΔExp={self.delta_exp_r:.3f}R, better={self.share_better*100:.1f}%"
                "suggested_changes": {
                    "trailing_tp1_offset_atr": "decrease_by_20_percent"
                    "trailing_profile": "more_aggressive"
                }
            })
        elif strong_negative_edge:
            recommendations["actions"].append({
                "type": "decrease_trailing_aggression"
                "reason": f"Strong negative edge: ΔExp={self.delta_exp_r:.3f}R, worse={self.share_worse*100:.1f}%"
                "suggested_changes": {
                    "trailing_tp1_offset_atr": "increase_by_50_percent"
                    "trailing_profile": "more_conservative"
                }
            })
        elif moderate_positive_edge:
            recommendations["actions"].append({
                "type": "slight_increase_trailing_aggression"
                "reason": f"Moderate positive edge: ΔExp={self.delta_exp_r:.3f}R, better={self.share_better*100:.1f}%"
                "suggested_changes": {
                    "trailing_tp1_offset_atr": "decrease_by_10_percent"
                }
            })
        elif moderate_negative_edge:
            recommendations["actions"].append({
                "type": "slight_decrease_trailing_aggression"
                "reason": f"Moderate negative edge: ΔExp={self.delta_exp_r:.3f}R, worse={self.share_worse*100:.1f}%"
                "suggested_changes": {
                    "trailing_tp1_offset_atr": "increase_by_25_percent"
                }
            })

        # Рекомендация по доле трейлинговых сделок
        trailing_ratio = self.trailing_trades / self.total_trades if self.total_trades > 0 else 0

        if trailing_ratio < 0.3 and strong_positive_edge:
            recommendations["actions"].append({
                "type": "increase_trailing_coverage"
                "reason": f"Low trailing coverage ({trailing_ratio*100:.1f}%) with positive edge"
                "suggested_changes": {
                    "trailing_share_target": 0.7
                }
            })
        elif trailing_ratio > 0.8 and strong_negative_edge:
            recommendations["actions"].append({
                "type": "decrease_trailing_coverage"
                "reason": f"High trailing coverage ({trailing_ratio*100:.1f}%) with negative edge"
                "suggested_changes": {
                    "trailing_share_target": 0.4
                }
            })

        return recommendations if recommendations["actions"] else None


class TrailingEdgeAnalyzer:
    """Анализатор trailing edge для интеграции в PeriodicReporter."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.eps = 1e-6

    def analyze_last_trades(
        self
        source: str
        symbol: str
        limit: int = 200
        since_hours: Optional[int] = None
    ) -> Optional[TrailingEdgeResult]:
        """
        Анализирует последние N сделок для trailing edge анализа.

        Args:
            source: Источник сигналов
            symbol: Торговая пара
            limit: Максимальное количество сделок
            since_hours: Ограничение по времени (часы назад)

        Returns:
            TrailingEdgeResult или None если недостаточно данных
        """
        try:
            trades = self._load_trades(source, symbol, limit, since_hours)
            if len(trades) < 10:  # Минимум 10 сделок для анализа
                logger.debug(f"Недостаточно сделок для анализа: {len(trades)} < 10")
                return None

            return self._analyze_trades(trades, source, symbol, limit, since_hours)

        except Exception as e:
            logger.error(f"Ошибка анализа trailing edge для {source}/{symbol}: {e}")
            return None

    def _load_trades(
        self
        source: str
        symbol: str
        limit: int
        since_hours: Optional[int]
    ) -> List[Trade]:
        """
        Загружает сделки из Redis stream trades:closed.

        Важно:
          - при TRADES_CLOSED_STREAM_COMPACT=1 payload может быть частичным
            поэтому используем hydrate_trade_closed_batch() (pipeline) для восстановления из order:{id}.
          - также нормализуем alias trailing_profile <-> trail_profile (делает hydrator).
        """
        threshold_ms = None
        if since_hours:
            threshold_ms = int(get_ny_time_millis() - since_hours * 3600 * 1000)

        entries = self.redis.xrevrange("trades:closed", max="+", min="-", count=max(10, limit * 4)) or []

        # 1) Сначала нормализуем stream-fields
        raw_items: List[Dict[str, str]] = []
        for _msg_id, fields in entries:
            raw_items.append(_norm_map(fields or {}))

        # 2) Гидрируем пачкой (1 roundtrip через pipeline для всех order:{id}, кому нужно)
        hydrated_items = hydrate_trade_closed_batch(
            self.redis
            raw_items
            require_closed=False,   # анализ best-effort
            merge_precedence="hash" # hash — source of truth
        )

        # 3) Фильтруем по source/symbol и строим Trade объекты
        trades: List[Trade] = []
        for fields in hydrated_items:
            t = _build_trade_from_fields(fields)
            if not t:
                continue
            if t.source != source or t.symbol != symbol:
                continue
            if threshold_ms and t.exit_ts_ms < threshold_ms:
                continue
            trades.append(t)
            if len(trades) >= int(limit):
                break

        # Сортируем по времени (новые сверху)
        trades.sort(key=lambda t: t.exit_ts_ms, reverse=True)
        return trades

    def _analyze_trades(
        self
        trades: List[Trade]
        source: str
        symbol: str
        limit: int
        since_hours: Optional[int]
    ) -> TrailingEdgeResult:
        """Выполняет анализ trailing edge на списке сделок."""

        # Определяем окно анализа
        if since_hours:
            analysis_window = f"last_{since_hours}h"
        else:
            analysis_window = f"last_{len(trades)}_trades"

        total_trades = len(trades)
        trailing_trades = sum(1 for t in trades if t.trailing_started or t.trailing_active)

        # Считаем R-метрики
        r_managed = []
        r_baseline = []
        diffs_r = []
        diffs_usd = []

        better_count = worse_count = equal_count = 0

        for trade in trades:
            if abs(trade.one_r_money) < self.eps:
                continue

            r_m = trade.pnl_net / trade.one_r_money
            r_b = trade.pnl_if_fixed_exit / trade.one_r_money

            r_managed.append(r_m)
            r_baseline.append(r_b)
            diffs_r.append(r_m - r_b)
            diffs_usd.append(trade.pnl_net - trade.pnl_if_fixed_exit)

            # Считаем распределение
            delta = r_m - r_b
            if delta > self.eps:
                better_count += 1
            elif delta < -self.eps:
                worse_count += 1
            else:
                equal_count += 1

        # Вычисляем статистики
        exp_managed_r = sum(r_managed) / len(r_managed) if r_managed else 0.0
        exp_baseline_r = sum(r_baseline) / len(r_baseline) if r_baseline else 0.0
        delta_exp_r = exp_managed_r - exp_baseline_r

        share_better = better_count / total_trades if total_trades > 0 else 0.0
        share_worse = worse_count / total_trades if total_trades > 0 else 0.0
        share_equal = equal_count / total_trades if total_trades > 0 else 0.0

        avg_diff_usd = sum(diffs_usd) / len(diffs_usd) if diffs_usd else 0.0

        return TrailingEdgeResult(
            symbol=symbol
            source=source
            total_trades=total_trades
            trailing_trades=trailing_trades
            exp_managed_r=exp_managed_r
            exp_baseline_r=exp_baseline_r
            delta_exp_r=delta_exp_r
            share_better=share_better
            share_worse=share_worse
            share_equal=share_equal
            avg_diff_usd=avg_diff_usd
            analysis_window=analysis_window
        )
