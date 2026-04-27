# signal_thresholds_manager.py
from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict, deque
from typing import Deque, Dict, Hashable, Optional, Tuple

from .signal_types import SignalKind, SignalTypeConf


@dataclass
class ObservedScores:
    raw_scores: Deque[float]
    final_scores: Deque[float]
    regime_scores: Deque[float]
    geo_scores: Deque[float]
    liq_scores: Deque[float]


@dataclass
class DynamicThresholds:
    min_raw_score: float
    min_final_score: float
    golden_regime_min: float
    golden_geometry_min: float
    golden_liquidity_min: float
    source: str = "dynamic"


class SignalThresholdsManager:
    """
    Держит rolling-статистику по (symbol, signal_kind) и
    на её основе возвращает динамические пороги (квантильный автотюнинг).

    Используется как мягкая надстройка над статическим SignalTypeConf:
      - не ломает ваши конфиги,
      - просто подправляет пороги под конкретный инструмент.
    """

    def __init__(
        self,
        history_size: int = 1000,
        warmup_min_samples: int = 200,
        raw_quantile: float = 0.70,
        final_quantile: float = 0.70,
        golden_quantile: float = 0.85,
    ) -> None:
        self._history_size = history_size
        self._warmup_min_samples = warmup_min_samples
        self._raw_q = raw_quantile
        self._final_q = final_quantile
        self._golden_q = golden_quantile

        self._store: Dict[Tuple[Hashable, SignalKind], ObservedScores] = defaultdict(
            lambda: ObservedScores(
                raw_scores=deque(maxlen=self._history_size),
                final_scores=deque(maxlen=self._history_size),
                regime_scores=deque(maxlen=self._history_size),
                geo_scores=deque(maxlen=self._history_size),
                liq_scores=deque(maxlen=self._history_size),
            )
        )

    @staticmethod
    def _quantile(xs, q: float, default: float) -> float:
        arr = sorted(x for x in xs if x is not None)
        n = len(arr)
        if n == 0:
            return default
        q = max(0.0, min(1.0, q))
        idx = int(round((n - 1) * q))
        return float(arr[idx])

    def observe(
        self,
        symbol: Hashable,
        kind: SignalKind,
        raw_score: float,
        final_score: float,
        regime_score_norm: float,
        geometry_score: float,
        liq_score: float,
    ) -> None:
        """
        Записываем только уже "осмысленные" сигналы — те, что прошли basic-фильтры
        и получили ненулевой final_score.
        """
        if final_score == 0.0:
            return

        key = (symbol, kind)
        bucket = self._store[key]

        bucket.raw_scores.append(abs(raw_score))
        bucket.final_scores.append(abs(final_score))
        bucket.regime_scores.append(max(0.0, min(1.0, regime_score_norm)))
        bucket.geo_scores.append(max(0.0, min(1.0, geometry_score)))
        bucket.liq_scores.append(max(0.0, min(1.0, liq_score)))

    def get_thresholds(
        self,
        symbol: Hashable,
        kind: SignalKind,
        base_conf: SignalTypeConf,
    ) -> Optional[DynamicThresholds]:
        key = (symbol, kind)
        bucket = self._store.get(key)
        if bucket is None or len(bucket.raw_scores) < self._warmup_min_samples:
            # Мало истории — используем только стату.
            return None

        # --- квантильные оценки ---
        q_raw = self._quantile(bucket.raw_scores, self._raw_q, base_conf.min_raw_score)
        q_final = self._quantile(
            bucket.final_scores, self._final_q, base_conf.min_final_score
        )

        q_regime = self._quantile(
            bucket.regime_scores, self._golden_q, base_conf.golden_regime_min
        )
        q_geo = self._quantile(
            bucket.geo_scores, self._golden_q, base_conf.golden_geometry_min
        )
        q_liq = self._quantile(
            bucket.liq_scores, self._golden_q, base_conf.golden_liquidity_min
        )

        # --- мягкое "пришивание" к базовым значениям (clamp) ---
        # чтобы автотюнинг не уехал на x10 из-за артефактов.
        def _clamp(val: float, base: float, lo_mul: float, hi_mul: float) -> float:
            lo = base * lo_mul
            hi = base * hi_mul
            if lo == hi == 0.0:
                return val
            return max(lo, min(hi, val))

        min_raw = _clamp(q_raw, base_conf.min_raw_score, 0.7, 1.5)
        min_final = _clamp(q_final, base_conf.min_final_score, 0.7, 1.5)

        golden_regime_min = _clamp(
            q_regime, base_conf.golden_regime_min, 0.8, 1.2
        )
        golden_geometry_min = _clamp(
            q_geo, base_conf.golden_geometry_min, 0.8, 1.2
        )
        golden_liquidity_min = _clamp(
            q_liq, base_conf.golden_liquidity_min, 0.8, 1.2
        )

        return DynamicThresholds(
            min_raw_score=min_raw,
            min_final_score=min_final,
            golden_regime_min=golden_regime_min,
            golden_geometry_min=golden_geometry_min,
            golden_liquidity_min=golden_liquidity_min,
            source="dynamic",
        )
