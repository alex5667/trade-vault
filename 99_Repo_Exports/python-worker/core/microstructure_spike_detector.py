"""
MicrostructureSpikeDetector — детектор всплесков.
"""

import time
from collections import deque
from dataclasses import dataclass

import numpy as np


@dataclass
class SpikeConfig:
    z_delta_thr: float = 3.0
    z_extreme_thr: float = 4.5
    speed_z_thr: float = 3.0
    win_ticks: int = 300
    win_speed_sec: int = 30


class MicrostructureSpikeDetector:
    def __init__(self, cfg: SpikeConfig):
        self.cfg = cfg
        self.mid_q: deque[float] = deque(maxlen=cfg.win_ticks)
        self.ts_q: deque[float] = deque(maxlen=cfg.win_ticks)
        self.delta_q: deque[float] = deque(maxlen=cfg.win_ticks)
        self.speed_q: deque[float] = deque(maxlen=cfg.win_ticks)
        self.range_q: deque[float] = deque(maxlen=cfg.win_ticks)

    def update(self, bid: float, ask: float, volume: float = 1.0, delta_hint: float = None, ts_ms: int = None) -> dict:  # type: ignore
        ts = ts_ms / 1000.0 if ts_ms else time.time()
        mid = (bid + ask) / 2.0 if (bid and ask) else 0.0

        last_ts = self.ts_q[-1] if self.ts_q else ts
        dt = max(ts - last_ts, 1e-3)
        tick_speed = 1.0 / dt

        if delta_hint is None:
            prev_mid = self.mid_q[-1] if self.mid_q else mid
            delta = np.sign(mid - prev_mid) * max(volume, 1.0)
        else:
            delta = float(delta_hint)

        prev = list(self.mid_q)[-30:] if len(self.mid_q) >= 30 else list(self.mid_q)
        rng = (max(prev) - min(prev)) if prev else 0.0

        self.ts_q.append(ts)
        self.mid_q.append(mid)
        self.delta_q.append(delta)
        self.speed_q.append(tick_speed)
        self.range_q.append(rng)

        # ✅ GPU Support: используем GPU для вычисления z-scores
        def z(x):
            if len(x) < 10:
                return 0.0

            # Пытаемся использовать GPU сервис
            try:
                from services.gpu_compute_service import get_gpu_service
                gpu_service = get_gpu_service()
                if gpu_service and gpu_service.is_gpu_available():
                    a = np.array(x, dtype=np.float32)
                    z_scores = gpu_service.compute_z_scores(a, window=len(a))  # type: ignore
                    return float(z_scores[-1]) if len(z_scores) > 0 else 0.0
            except Exception:
                pass  # Fallback to CPU

            # CPU fallback
            a = np.array(x, dtype=float)
            m, s = a.mean(), a.std()
            if s == 0:
                return 0.0
            return (a[-1] - m) / s

        z_delta = z(self.delta_q)
        z_speed = z(self.speed_q)
        z_range = z(self.range_q)

        extreme = abs(z_delta) >= self.cfg.z_extreme_thr
        trigger = (abs(z_delta) >= self.cfg.z_delta_thr) or (z_speed >= self.cfg.speed_z_thr)

        return {
            "mid": mid,
            "tick_speed": tick_speed,
            "z_delta": z_delta,
            "z_speed": z_speed,
            "z_range": z_range,
            "extreme": bool(extreme),
            "trigger": bool(trigger),
            "dir_up": (z_delta > 0),
        }


