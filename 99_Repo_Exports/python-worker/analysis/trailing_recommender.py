# analysis/trailing_recommender.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple
import math
import statistics as stats

EPS = 1e-9


@dataclass
class ClosedTradeSnapshot:
    """
    Минимальный срез закрытой сделки для анализа трейлинга.
    Совместим с TradeClosed: поля читаются из Redis (trades:closed).
    """
    source: str
    symbol: str
    pnl_net: float
    one_r_money: float
    mfe_pnl: float
    giveback: float
    trailing_started: bool
    trailing_active: bool
    exit_ts_ms: int
    entry_tag: str = ""   # ← добавили для группировки по тегам


@dataclass
class TrailingSizeRecommendation:
    """
    Рекомендация по размеру трейлинга после TP1 в терминах R и ATR.
    """
    source: str
    symbol: str
    trailing_only: bool

    # выборка
    sample_size: int       # сколько сделок прошло фильтры
    wins_count: int        # сколько win внутри выборки
    min_trades_required: int

    # основное
    lock_r: float                 # рекомендованный минимум "залочки" в R
    lock_offset_atr: float        # соответствующий TRAILING_TP1_OFFSET_ATR
    avg_mfe_r: float              # средний MFE в R по win-сделкам
    median_mfe_r: float           # медианный MFE в R
    avg_giveback_r: float         # средний giveback в R
    avg_giveback_ratio: float     # средний giveback/MFE

    # оценка "уверенности"
    std_mfe_r: float
    std_giveback_ratio: float
    confidence: float             # 0..1

    # служебное
    stop_atr_mult: float


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    v = sorted(values)
    q = min(max(q, 0.0), 1.0)
    pos = (len(v) - 1) * q
    i = int(pos)
    if i >= len(v) - 1:
        return v[-1]
    frac = pos - i
    return v[i] * (1.0 - frac) + v[i + 1] * frac


def _compute_confidence(
    mfe_r_win: Sequence[float],
    giveback_ratio_win: Sequence[float],
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
    source: str,
    symbol: str,
    stop_atr_mult: float,
    min_trades: int = 50,
    min_wins: Optional[int] = None,
    winners_only: bool = True,
    mfe_quantile: float = 0.25,
    trailing_only: bool = False,
    # Escape hatch: allow high-confidence cases with fewer trades
    escape_min_wins: int = 20,
    escape_confidence: float = 0.90,
) -> Optional[TrailingSizeRecommendation]:
    """
    Строит рекомендацию по lock_r и TRAILING_TP1_OFFSET_ATR на основе выборки сделок.

    trailing_only:
        False → используем все win-сделки (базовый теоретический edge входа),
        True  → только сделки, где трейлинг был запущен (t.trailing_started / active).
    """
    r_win: List[float] = []
    mfe_r_win: List[float] = []
    giveback_r_win: List[float] = []
    giveback_ratio_win: List[float] = []

    count_total = 0
    for t in trades:
        if t.symbol != symbol or t.source != source:
            continue

        # фильтр по трейлингу
        if trailing_only and not (t.trailing_started or t.trailing_active):
            continue

        count_total += 1

        one_r = float(t.one_r_money or 0.0)
        if one_r <= EPS:
            continue

        pnl_net = float(t.pnl_net or 0.0)
        mfe = float(t.mfe_pnl or 0.0)
        giveback = float(t.giveback or 0.0)

        # r = pnl_net / one_r  <-- unused locally for stats, but keeping structure
        # mfe_r = mfe / one_r
        # giveback_r = giveback / one_r
        
        # calculate normalized values
        current_mfe_r = mfe / one_r
        current_giveback_r = giveback / one_r
        current_pnl_r = pnl_net / one_r

        if winners_only and pnl_net <= 0.0:
            continue
        
        if current_mfe_r <= 0.0:
            continue

        # Suspicious data filter: MFE > 100R is likely a data error or extreme outlier
        if current_mfe_r > 100.0:
            continue

        r_win.append(current_pnl_r)
        mfe_r_win.append(current_mfe_r)
        giveback_r_win.append(current_giveback_r)

        g_ratio = current_giveback_r / max(current_mfe_r, EPS)
        # Cap ratio at 1.5 to avoid skews
        g_ratio = max(0.0, min(g_ratio, 1.5))
        giveback_ratio_win.append(g_ratio)

    wins = len(r_win)
    
    # Gate A: Data Sufficiency
    # Expert recommendation: check both n_total and n_wins
    # Default min_wins to min_trades for backward compatibility (if not explicitly provided)
    # BUT logic implies min_wins is critical. We use safe defaults if None.
    effective_min_wins = min_wins if min_wins is not None else max(10, min_trades // 3)
    
    # Primary gate: check both total trades and wins
    primary_gate_passed = (count_total >= min_trades) and (wins >= effective_min_wins)
    
    if not primary_gate_passed:
        # Escape hatch: allow high-confidence cases with fewer data
        # (e.g., BTC with n_wins=20 but confidence=0.90)
        # We need at least escape_min_wins WINS to even consider computing confidence.
        # BUT: if we have enough total trades OR enough wins (but not both), 
        # we should still proceed if wins >= escape_min_wins.
        if wins < escape_min_wins:
            return None
        # If escape hatch applies (wins >= escape_min_wins), we proceed to compute stats 
        # and checking confidence at the end.
    
    # ... computation ...
    if wins == 0:
        return None
    avg_mfe_r = float(sum(mfe_r_win) / wins)
    median_mfe_r = _quantile(mfe_r_win, 0.5)
    avg_giveback_r = float(sum(giveback_r_win) / wins)
    avg_giveback_ratio = float(sum(giveback_ratio_win) / wins)

    # квантиль по MFE_R – на его основе выбираем lock_r
    mfe_q = _quantile(mfe_r_win, mfe_quantile)

    # эвристика:
    # - не меньше 0.1R
    # - не больше 0.7R
    # - масштабируем от квантиля
    lock_r = mfe_q * 0.3
    lock_r = max(0.1, min(lock_r, 0.7))

    # перевод в ATR-офсет:
    # 1R в ATR-единицах ≈ stop_atr_mult
    stop_atr_mult = float(stop_atr_mult or 1.0)
    lock_offset_atr = lock_r * stop_atr_mult

    confidence, std_mfe_r, std_giveback_ratio = _compute_confidence(
        mfe_r_win,
        giveback_ratio_win,
        wins,
        min_trades_required=min_trades,
    )

    # Sanity check: if std is suspiciously 0.0 with enough trades, likely garbage data (constant 500)
    if wins >= 5 and std_mfe_r < 1e-12:
        confidence = 0.0
    
    # Escape hatch final check: if we failed the primary gate, we MUST match high confidence
    if not primary_gate_passed:
        if confidence < escape_confidence:
            # Escape hatch failed: insufficient confidence
            return None

    return TrailingSizeRecommendation(
        source=source,
        symbol=symbol,
        trailing_only=trailing_only,
        sample_size=count_total,
        wins_count=wins,
        min_trades_required=min_trades,
        lock_r=lock_r,
        lock_offset_atr=lock_offset_atr,
        avg_mfe_r=avg_mfe_r,
        median_mfe_r=median_mfe_r,
        avg_giveback_r=avg_giveback_r,
        avg_giveback_ratio=avg_giveback_ratio,
        std_mfe_r=std_mfe_r,
        std_giveback_ratio=std_giveback_ratio,
        confidence=confidence,
        stop_atr_mult=stop_atr_mult,
    )
