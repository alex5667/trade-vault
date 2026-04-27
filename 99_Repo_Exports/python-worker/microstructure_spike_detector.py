# microstructure_spike_detector.py
"""
Microstructure Spike Detector - детектор всплесков на основе z-score.
"""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List
import math

try:
    import numpy as np
except ImportError:
    np = None

@dataclass
class SpikeConfig:
    """Конфигурация детектора всплесков."""
    maxlen: int = 500
    z_delta_th: float = 3.0
    z_vol_th: float = 3.0
    z_speed_th: float = 3.0

class MicrostructureSpikeDetector:
    """
    Оценивает z-score для:
      - delta (approx: sign(mid change)*volume)
      - volume
      - speed (|d mid| / dt)
    Окно — deque последних тиков.
    """
    
    def __init__(self, maxlen: int = 500):
        self.cfg = SpikeConfig(maxlen=maxlen)
        self.window: Deque[Dict] = deque(maxlen=maxlen)

    def feed_ticks(self, ticks: List[Dict]):
        """Добавить тики в окно."""
        for t in ticks:
            self.window.append(t)

    def _arr(self, key: str):
        """Извлечь массив значений из окна."""
        if np is None:
            # Fallback без numpy
            values = [x.get(key, 0.0) for x in self.window]
            return values
        return np.array([x.get(key, 0.0) for x in self.window], dtype=float)

    def _z(self, x) -> float:
        """Вычислить z-score последнего значения."""
        if np is not None and isinstance(x, np.ndarray):
            if x.size < 10:
                return 0.0
            m, s = float(np.mean(x)), float(np.std(x))
        else:
            # Fallback без numpy
            if len(x) < 10:
                return 0.0
            mean = sum(x) / len(x)
            variance = sum((xi - mean) ** 2 for xi in x) / len(x)
            m, s = mean, math.sqrt(variance)
        
        if s <= 1e-12:
            return 0.0
        
        if np is not None and isinstance(x, np.ndarray):
            last_val = float(x[-1])
        else:
            last_val = x[-1] if x else 0.0
        
        return (last_val - m) / s

    def compute(self) -> Dict:
        """Вычислить метрики всплесков."""
        if len(self.window) < 2:
            return {"ok": False}
        
        last, prev = self.window[-1], self.window[-2]
        bid, ask = last.get("bid", 0.0), last.get("ask", 0.0)
        mid = (bid + ask) / 2.0 if bid and ask else last.get("last", 0.0)
        prev_bid, prev_ask = prev.get("bid", 0.0), prev.get("ask", 0.0)
        prev_mid = (prev_bid + prev_ask) / 2.0 if prev_bid and prev_ask else prev.get("last", 0.0)
        dt = max(1.0, (last["ts"] - prev["ts"]) / 1000.0)

        # эвристическая delta: знак(движения) * объем тика
        vol = float(last.get("volume", 0.0))
        delta = math.copysign(vol, mid - prev_mid)

        # добавим производные фичи в последний тик, чтобы z считался по массивам
        self.window[-1]["_delta"] = delta
        self.window[-1]["_vol"] = vol
        self.window[-1]["_speed"] = abs(mid - prev_mid) / dt

        z_delta = self._z(self._arr("_delta"))
        z_vol = self._z(self._arr("_vol"))
        z_speed = self._z(self._arr("_speed"))

        extreme = (abs(z_delta) >= self.cfg.z_delta_th) or (z_vol >= self.cfg.z_vol_th) or (z_speed >= self.cfg.z_speed_th)

        return {
            "ok": True,
            "mid": mid,
            "z_delta": z_delta,
            "z_vol": z_vol,
            "z_speed": z_speed,
            "extreme": extreme
        }
