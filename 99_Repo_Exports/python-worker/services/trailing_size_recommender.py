# -*- coding: utf-8 -*-
from __future__ import annotations
"""
TrailingSizeRecommender

Модуль для расчёта рекомендуемого размера трейлинг-стопа
на основании истории закрытых сделок (TradeClosed).

Идея:
- работаем в R-пространстве (pnl_net / one_r_money, mfe_pnl / one_r_money);
- подбираем lock_r — сколько R разумно "залочить" после TP1;
- конвертируем lock_r в TRAILING_TP1_OFFSET_ATR через stop_atr_mult.

Ожидается, что вызывающий код сам отфильтрует сделки по:
- source (например, CryptoOrderFlow),
- symbol (ETHUSDT / BTCUSDT),
- entry_tag (опционально).
"""


import statistics as stats
from dataclasses import dataclass
from typing import Iterable, Optional, Dict, Any, List, NamedTuple


EPS = 1e-9


@dataclass
class ClosedTradeSnapshot:
    """
    Упрощённое представление TradeClosed для анализа трейлинга.
    Поля названы так, чтобы было легко заполнить из TradeClosed.__dict__.
    """
    source: str
    symbol: str
    strategy: str
    entry_tag: str

    exit_ts_ms: int

    pnl_net: float
    pnl_if_fixed_exit: float
    one_r_money: float

    mfe_pnl: float
    mae_pnl: float
    giveback: float
    missed_profit: float

    fees_money: float

    trailing_started: bool
    trailing_active: bool
    close_reason: str
    close_reason_raw: str
    close_reason_detail: str = ""

    @classmethod
    def from_trade_closed_dict(cls, d: Dict[str, Any]) -> "ClosedTradeSnapshot":
        """
        Удобный конструктор из словаря (например, TradeClosed.__dict__
        или payload из Redis/JSON).
        """
        return cls(
            source=str(d.get("source") or ""),
            symbol=str(d.get("symbol") or ""),
            strategy=str(d.get("strategy") or ""),
            entry_tag=str(d.get("entry_tag") or ""),

            exit_ts_ms=int(d.get("exit_ts_ms") or 0),

            pnl_net=float(d.get("pnl_net") or 0.0),
            pnl_if_fixed_exit=float(d.get("pnl_if_fixed_exit") or 0.0),
            one_r_money=float(d.get("one_r_money") or 0.0),

    mfe_pnl=float(d.get("mfe_pnl") or 0.0),
    mae_pnl=float(d.get("mae_pnl") or 0.0),
    giveback=float(d.get("giveback") or 0.0),
    missed_profit=float(d.get("missed_profit") or 0.0),
    
    fees_money=float(d.get("fees_money") or d.get("fees") or d.get("commission") or 0.0),

    trailing_started=bool(d.get("trailing_started") or d.get("trailing_active") or False),
    trailing_active=bool(d.get("trailing_active") or False),
    close_reason=str(d.get("close_reason") or ""),
    close_reason_raw=str(d.get("close_reason_raw") or ""),
    close_reason_detail=str(d.get("close_reason_detail") or ""),
        )


class TrailingSizeRecommendation(NamedTuple):
    # Основная рекомендация
    lock_r: float                   # сколько R имеет смысл "залочить" после TP1
    lock_r_low: float               # нижняя граница разумного диапазона
    lock_r_high: float              # верхняя граница

    trailing_tp1_offset_atr: float       # рекомендованный TRAILING_TP1_OFFSET_ATR
    trailing_tp1_offset_atr_low: float
    trailing_tp1_offset_atr_high: float

    # Диагностика по выборке
    sample_size_win: int
    avg_r_win: float
    median_r_win: float
    median_mfe_r_win: float
    avg_giveback_r_win: float
    avg_giveback_ratio_win: float

    # Новые поля для trailing_only и confidence
    trailing_only: bool             # True если использовали только trailing-сделки
    sample_size_total: int          # общее число сделок в выборке (до фильтра по win/loss)
    confidence: float               # оценка уверенности в рекомендации (0..1)
    std_mfe_r: float                # std отклонение MFE_R
    std_giveback_ratio: float       # std отклонение giveback_ratio


def _quantile(xs: List[float], q: float) -> float:
    """
    Простейшая реализация квантиля q (0..1) без numpy.
    """
    if not xs:
        return 0.0
    if q <= 0:
        return min(xs)
    if q >= 1:
        return max(xs)
    xs_sorted = sorted(xs)
    idx = q * (len(xs_sorted) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(xs_sorted) - 1)
    w = idx - lo
    return xs_sorted[lo] * (1 - w) + xs_sorted[hi] * w


def _compute_confidence(
    mfe_r_win: List[float],
    giveback_ratio_win: List[float],
    wins_count: int,
    min_trades_required: int,
) -> Tuple[float, float, float]:
    """
    Возвращает (confidence, std_mfe_r, std_giveback_ratio)

    Идея:
    - чем больше сделок, тем выше базовый уровень;
    - чем меньше разброс MFE_R и giveback_ratio, тем выше доверие;
    - ограничиваем всё в [0, 1].
    """
    if wins_count <= 0 or not mfe_r_win:
        return 0.0, 0.0, 0.0

    if len(mfe_r_win) >= 2:
        std_mfe_r = float(stats.pstdev(mfe_r_win))
    else:
        std_mfe_r = 0.0

    if len(giveback_ratio_win) >= 2:
        std_giveback_ratio = float(stats.pstdev(giveback_ratio_win))
    else:
        std_giveback_ratio = 0.0

    # фактор по количеству сделок
    # при wins_count == min_trades_required → ~0.6
    # при wins_count >= 2 * min_trades_required → → 1.0
    n_factor_raw = wins_count / float(max(min_trades_required, 1))
    n_factor = max(0.0, min(1.0, 0.6 * min(n_factor_raw, 2.0)))

    # фактор по разбросу:
    # считаем, что MFE_R обычно 0..3, ratio 0..1.
    vol_penalty = (std_mfe_r / 3.0) + std_giveback_ratio
    vol_factor = 1.0 / (1.0 + max(vol_penalty, 0.0))  # 1 / (1 + x)

    confidence = max(0.0, min(1.0, n_factor * vol_factor))
    return confidence, std_mfe_r, std_giveback_ratio


def recommend_trailing_size(
    trades: Iterable[ClosedTradeSnapshot],
    *,
    stop_atr_mult: float,
    min_trades: int = 50,
    winners_only: bool = True,
    mfe_quantile: float = 0.25,
    trailing_only: bool = False,
    fees_in_r_median: float = 0.0,
) -> Optional[TrailingSizeRecommendation]:
    """
    Рассчитывает рекомендованный размер трейлинга (lock_r и TRAILING_TP1_OFFSET_ATR)
    по выборке закрытых сделок.

    Args:
        trades: Iterable[ClosedTradeSnapshot] — уже отфильтрованные сделки
                по source/symbol/entry_tag/интервалу времени.
        stop_atr_mult: множитель ATR, который использовался/используется для SL
                       (stop_atr_mult в твоём конфиге / spec).
        min_trades: минимальное количество выигрышных сделок, чтобы давать рекомендацию.
        winners_only: использовать только выигрышные сделки (pnl_net > 0) для оценки.
        mfe_quantile: какой квантиль MFE_R использовать как базу для lock_r.
                      Например, 0.25 → 25-й перцентиль (75% победителей имели MFE_R выше).
        trailing_only: использовать только сделки, где трейлинг был запущен/active.
        fees_in_r_median: медианная комиссия в выражении R (для Breakeven Guard).

    Returns:
        TrailingSizeRecommendation или None, если данных недостаточно.
    """
    r_win: List[float] = []
    mfe_r_win: List[float] = []
    giveback_r_win: List[float] = []
    giveback_ratio_win: List[float] = []

    count_total = 0
    for t in trades:
        # фильтр по трейлингу
        if trailing_only and not (t.trailing_started or t.trailing_active):
            continue

        count_total += 1

        one_r = float(t.one_r_money or 0.0)
        if one_r <= EPS:
            continue

        r = float(t.pnl_net) / one_r
        mfe_r = float(t.mfe_pnl) / one_r
        giveback_r = float(t.giveback) / one_r if t.giveback else (mfe_r - r)

        if winners_only and t.pnl_net <= 0:
            continue

        # Игнорируем сделки без адекватного MFE
        if mfe_r <= 0:
            continue

        r_win.append(r)
        mfe_r_win.append(mfe_r)
        giveback_r_win.append(giveback_r)

        # Giveback ratio = сколько долей от MFE мы отдали обратно рынку
        # (защита от деления на 0)
        g_ratio = giveback_r / max(mfe_r, EPS)
        giveback_ratio_win.append(g_ratio)

    n = len(r_win)
    if n < min_trades:
        return None

    # Базовая статистика
    avg_r = sum(r_win) / n
    median_r = _quantile(r_win, 0.5)
    median_mfe_r = _quantile(mfe_r_win, 0.5)
    avg_giveback_r = sum(giveback_r_win) / n
    avg_giveback_ratio = sum(giveback_ratio_win) / n

    # 1) Базовый lock_r — по квантилю MFE_R
    base_lock_r = _quantile(mfe_r_win, mfe_quantile)

    # 2) Кэп по медиане реализованного R, чтобы не завышать
    #    Например, не больше 90% медианы выигрышей.
    lock_cap_by_r = 0.9 * median_r

    raw_lock_r = min(base_lock_r, lock_cap_by_r)

    # 3) Флоор/клип
    #    - не меньше 0.25R или fees_in_r (Breakeven Guard),
    #    - не больше 1.0R (иначе слишком агрессивный трейлинг).
    floor_locked = max(0.25, fees_in_r_median)
    lock_r = max(floor_locked, min(raw_lock_r, 1.0))

    # 4) Диапазон (низ/верх) вокруг основной оценки
    lock_r_low = max(0.03, lock_r * 0.7)
    lock_r_high = min(1.5, lock_r * 1.3)

    # 5) Перевод в ATR: TRAILING_TP1_OFFSET_ATR = lock_r * stop_atr_mult
    trailing_tp1_offset_atr = lock_r * float(stop_atr_mult or 1.0)
    trailing_tp1_offset_atr_low = lock_r_low * float(stop_atr_mult or 1.0)
    trailing_tp1_offset_atr_high = lock_r_high * float(stop_atr_mult or 1.0)

    # 6) Вычисление confidence
    confidence, std_mfe_r, std_giveback_ratio = _compute_confidence(
        mfe_r_win,
        giveback_ratio_win,
        n,
        min_trades_required=min_trades,
    )

    return TrailingSizeRecommendation(
        lock_r=lock_r,
        lock_r_low=lock_r_low,
        lock_r_high=lock_r_high,
        trailing_tp1_offset_atr=trailing_tp1_offset_atr,
        trailing_tp1_offset_atr_low=trailing_tp1_offset_atr_low,
        trailing_tp1_offset_atr_high=trailing_tp1_offset_atr_high,
        sample_size_win=n,
        avg_r_win=avg_r,
        median_r_win=median_r,
        median_mfe_r_win=median_mfe_r,
        avg_giveback_r_win=avg_giveback_r,
        avg_giveback_ratio_win=avg_giveback_ratio,
        trailing_only=trailing_only,
        sample_size_total=count_total,
        confidence=confidence,
        std_mfe_r=std_mfe_r,
        std_giveback_ratio=std_giveback_ratio,
    )
