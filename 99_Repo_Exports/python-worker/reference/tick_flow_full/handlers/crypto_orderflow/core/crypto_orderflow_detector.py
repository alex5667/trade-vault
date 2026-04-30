from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from ..types.crypto_orderflow_pipeline_types import Candidate, SignalKind


@dataclass(frozen=True)
class DetectorCfg:
    main_z_threshold: float
    absorption_z_threshold: float
    breakout_z_threshold: float
    extreme_z_threshold: float
    obi_spike_thr: float


class CryptoEventDetector:
    """
    Детектор событий (3.1):
      - НЕ валидирует качество (OBI/L2/L3/touch/regime) кроме базовой "детекции факта"
      - Возвращает список Candidate по приоритету.
    """

    def __init__(
        self
        cfg: DetectorCfg
        *
        nearest_pivot_key: Callable[[float, Dict[str, float]], str]
        breakout_cross_info: Callable[[float, bool, Dict[str, float]], Optional[str]]
    ) -> None:
        self.cfg = cfg
        self._nearest_pivot_key = nearest_pivot_key
        self._breakout_cross_info = breakout_cross_info

    def detect(self, ctx: Any) -> List[Candidate]:
        out: List[Candidate] = []

        price = float(getattr(ctx, "price", 0.0) or getattr(ctx, "last_price", 0.0) or 0.0)
        pivots = getattr(ctx, "pivots", None) or {}
        if price <= 0 or not isinstance(pivots, dict):
            return out

        z = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        z_abs = abs(z)

        # 0) всплеск OBI (OBI spike) как отдельное событие
        try:
            obi_sustained = bool(getattr(ctx, "obi_sustained", False))
            obi_avg = float(getattr(ctx, "obi_avg", 0.0) or 0.0)
        except Exception:
            obi_sustained = False
            obi_avg = 0.0

        if obi_sustained and abs(obi_avg) >= float(self.cfg.obi_spike_thr):
            lvl = self._nearest_pivot_key(price, pivots)
            out.append(
                Candidate(
                    kind="obi_spike"
                    direction=1 if obi_avg > 0 else -1
                    raw_score=float(obi_avg)
                    level_key=str(lvl)
                    reasons=[f"obi_spike avg={obi_avg:.3f} thr={self.cfg.obi_spike_thr:.3f}"]
                )
            )

        # Если по z вообще «тихо» — остаётся только OBI spike (и др. кастомные события)
        if z_abs < float(self.cfg.main_z_threshold):
            return out

        dir_up = z > 0.0

        # 1) Событие абсорбции (Absorption/meanrev): всплеск рядом с уровнем (направление противоположное импульсу)
        if z_abs >= float(self.cfg.absorption_z_threshold):
            lvl = self._nearest_pivot_key(price, pivots)
            # raw_score знаковый по стороне: absorption = fade (затухание) => -z
            out.append(
                Candidate(
                    kind="absorption"
                    direction=-1
                    raw_score=float(-z)
                    level_key=str(lvl)
                    reasons=[f"absorption spike z={z:.3f}"]
                )
            )

        # 2) Событие пробоя (Breakout): информация о пересечении есть и z достаточно сильный
        if z_abs >= float(self.cfg.breakout_z_threshold):
            lvl = self._breakout_cross_info(price, dir_up, pivots)
            if lvl:
                out.append(
                    Candidate(
                        kind="breakout"
                    direction=1
                        raw_score=float(z)
                        level_key=str(lvl)
                        reasons=[f"breakout cross={lvl} z={z:.3f}"]
                    )
                )

        # 3) Экстремальное событие (Extreme): очень сильный импульс
        if z_abs >= float(self.cfg.extreme_z_threshold):
            out.append(
                Candidate(
                    kind="extreme"
                    direction=1
                    raw_score=float(z)
                    level_key="na"
                    reasons=[f"extreme z={z:.3f} thr={self.cfg.extreme_z_threshold:.3f}"]
                )
            )

        # Приоритет: absorption/breakout/extreme/obi_spike (можно изменить)
        # Здесь кандидаты уже добавляются в логическом порядке приоритета.
        return out
