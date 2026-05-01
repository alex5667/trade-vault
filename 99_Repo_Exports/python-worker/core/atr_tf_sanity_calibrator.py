# -*- coding: utf-8 -*-
from __future__ import annotations
"""
ATR TF Sanity Calibrator
=======================
Задача:
  - В реальном времени (bar_close) собирать распределение ATR_bps для нескольких TF.
  - Выбирать "исполнительный" ATR TF (atr_tf_exec), чтобы медианная ATR_bps
    была достаточной для fees-aware gate (rocket_v1) и/или ATR-floor.

Ключевые требования:
  - Детерминизм: обновления используют bar.end_ts_ms (не wall clock).
  - Readiness: пока n < min_samples, не переключаем TF (shadow-only диагностика).
  - Hysteresis + hold-down: не дергать TF часто.
  - Persist/Load: состояние квантилий P² (P2Quantile.to_state / from_state).
"""


import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

from core.quantile_p2 import P2Quantile


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


@dataclass
class AtrTfChoice:
    tf: str
    n: int
    src: str                 # "static" | "calib_p50"
    target_bps: float
    picked_p50_bps: float
    tfs_p50: Dict[str, float]


class AtrTfSanityCalibrator:
    """
    Для каждого regime храним P²-оценку медианы (p50) atr_bps по каждому TF.
    На основе этих медиан выбираем TF, удовлетворяющий target_bps.

    Логика выбора:
      - берем минимальный TF, у которого p50_bps >= target_bps
      - если ни один не удовлетворяет -> берем TF с максимальным p50_bps
      - если данных недостаточно -> возвращаем fallback_tf

    Hysteresis:
      - переключаемся только если new_p50_bps >= target_bps*(1 + switch_margin)
      - и прошло hold_ms с последнего переключения
    """

    def __init__(
        self,
        *,
        min_samples: int = 300,
        switch_margin: float = 0.08,   # 8% "зазор" над target для переключения
        hold_ms: int = 10 * 60_000,    # 10 минут
    ) -> None:
        self.min_samples = int(max(10, min_samples))
        self.switch_margin = float(max(0.0, switch_margin))
        self.hold_ms = int(max(0, hold_ms))

        # regime -> tf -> P2Quantile(p=0.5)
        self._p50: Dict[str, Dict[str, P2Quantile]] = {}
        self._n: Dict[str, int] = {}

        # runtime-side bookkeeping (persist separately if нужно)
        self.last_switch_ts_ms: int = 0
        self.last_tf: str = ""

    def update_many(self, *, regime: str, atr_bps_by_tf: Dict[str, float]) -> None:
        r = str(regime or "na")
        if r not in self._p50:
            self._p50[r] = {}

        any_upd = False
        for tf, v in (atr_bps_by_tf or {}).items():
            try:
                vv = float(v)
            except Exception:
                continue
            if not (math.isfinite(vv) and vv > 0):
                continue
            q = self._p50[r].get(tf)
            if q is None:
                q = P2Quantile(p=0.50)
                self._p50[r][tf] = q
            q.update(vv)
            any_upd = True

        if any_upd:
            self._n[r] = int(self._n.get(r, 0) + 1)

    def _p50_map(self, r: str) -> Dict[str, float]:
        out: Dict[str, float] = {}
        m = self._p50.get(r) or {}
        for tf, q in m.items():
            try:
                v = q.value()
                if v is not None and math.isfinite(float(v)) and float(v) > 0:
                    out[str(tf)] = float(v)
            except Exception:
                continue
        return out

    def _tf_sort_key(self, tf: str) -> int:
        # Simple heuristic: 1m=1, 1h=60, etc.
        # Format: "15m", "1h", "4h"
        s = tf.lower().strip()
        if s.endswith("m"):
            try:
                return int(s[:-1])
            except:
                return 999999
        if s.endswith("h"):
            try:
                return int(s[:-1]) * 60
            except:
                return 999999
        if s.endswith("d"):
            try:
                return int(s[:-1]) * 1440
            except:
                return 999999
        # Fallback for unknown
        return 999999

    def recommend_tf(
        self,
        *,
        regime: str,
        target_bps: float,
        fallback_tf: str,
        now_ts_ms: int,
        current_tf: str = "",
        allow_switch: bool = True,
    ) -> AtrTfChoice:
        """
        Возвращает рекомендуемый TF.
        Если недостаточно данных -> fallback_tf.
        """
        r = str(regime or "na")
        n = int(self._n.get(r, 0))
        target = float(max(0.0, target_bps))

        p50s = self._p50_map(r)
        if n < self.min_samples or not p50s:
            return AtrTfChoice(
                tf=str(fallback_tf),
                n=n,
                src="static",
                target_bps=target,
                picked_p50_bps=float(p50s.get(fallback_tf, 0.0) or 0.0),
                tfs_p50=p50s,
            )

        # deterministic order: sort by logical timeframe size
        tfs_sorted = sorted(p50s.keys(), key=self._tf_sort_key)

        # find minimal TF satisfying target
        pick = ""
        for tf in tfs_sorted:
            if float(p50s.get(tf, 0.0) or 0.0) >= target:
                pick = tf
                break
        if not pick:
            # none satisfies -> pick max p50
            pick = max(p50s.items(), key=lambda kv: float(kv[1] or 0.0))[0]

        picked_p50 = float(p50s.get(pick, 0.0) or 0.0)

        # Hysteresis / hold-down
        if allow_switch and current_tf and pick != current_tf:
            # 1) hold-down
            if self.hold_ms > 0 and (now_ts_ms - int(self.last_switch_ts_ms or 0)) < self.hold_ms:
                pick = current_tf
                picked_p50 = float(p50s.get(pick, 0.0) or 0.0)
            else:
                # 2) margin above target
                need = target * (1.0 + float(self.switch_margin))
                if picked_p50 < need:
                    pick = current_tf
                    picked_p50 = float(p50s.get(pick, 0.0) or 0.0)
                else:
                    # commit switch bookkeeping
                    self.last_switch_ts_ms = int(now_ts_ms)
                    self.last_tf = str(pick)

        return AtrTfChoice(
            tf=str(pick or fallback_tf),
            n=n,
            src="calib_p50",
            target_bps=target,
            picked_p50_bps=float(picked_p50),
            tfs_p50=p50s,
        )

    # ---------------- Persistence (per symbol/regime) ----------------
    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> Dict[str, Any]:
        r = str(regime or "na")
        tfs: Dict[str, Any] = {}
        for tf, q in (self._p50.get(r) or {}).items():
            try:
                tfs[str(tf)] = q.to_state()
            except Exception:
                continue
        return {
            "v": 1,
            "symbol": str(symbol),
            "regime": r,
            "updated_ts_ms": int(updated_ts_ms),
            "n": int(self._n.get(r, 0)),
            "p50_by_tf": tfs,
            # switching bookkeeping is runtime-local; можно тоже сохранять при желании
        }

    def load_regime_state(self, state: Dict[str, Any]) -> None:
        try:
            r = str((state or {}).get("regime") or "na")
            n = int((state or {}).get("n", 0) or 0)
            p50_by_tf = (state or {}).get("p50_by_tf") or {}
            if r not in self._p50:
                self._p50[r] = {}
            if isinstance(p50_by_tf, dict):
                for tf, st in p50_by_tf.items():
                    if not isinstance(st, dict):
                        continue
                    self._p50[r][str(tf)] = P2Quantile.from_state(st)
            self._n[r] = n
        except Exception:
            return
