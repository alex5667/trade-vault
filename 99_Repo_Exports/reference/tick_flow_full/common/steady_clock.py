from __future__ import annotations

import os
import time


class SteadyClock:
    """
    "Стабильное" now_ms на базе time.monotonic().
    Цель: защита от скачков системного времени (NTP/backward jumps),
    чтобы future/past guards и watermark не ломались.

    Идея:
      epoch_offset_ms = wall_now_ms - monotonic_ms  (на старте)
      steady_now_ms   = monotonic_ms + epoch_offset_ms

    Опционально: медленная подстройка offset (без рывков).
    """

    def __init__(self) -> None:
        self._alpha = float(os.getenv("STEADY_CLOCK_ALPHA", "0.01"))  # 0 -> без подстройки
        self._max_adjust_ms = int(os.getenv("STEADY_CLOCK_MAX_ADJUST_MS", "250"))  # clamp per call
        self._last_steady_ms = 0
        self._epoch_offset_ms = self._wall_now_ms() - self._mono_ms()

    def _wall_now_ms(self) -> int:
        return int(time.time() * 1000.0)

    def _mono_ms(self) -> int:
        return int(time.monotonic() * 1000.0)

    def now_ms(self) -> int:
        mono = self._mono_ms()
        wall = self._wall_now_ms()
        target_offset = wall - mono

        if self._alpha > 0.0:
            # мягкая подстройка offset без резких прыжков
            diff = target_offset - self._epoch_offset_ms
            step = int(max(-self._max_adjust_ms, min(self._max_adjust_ms, diff)))
            self._epoch_offset_ms = int(self._epoch_offset_ms + step * self._alpha)

        steady = int(mono + self._epoch_offset_ms)
        # гарантия монотонности now_ms
        if steady < self._last_steady_ms:
            steady = self._last_steady_ms
        self._last_steady_ms = steady
        return steady
