from __future__ import annotations

import math


class StreamingRSI:
    """
    Streaming RSI (Wilder).
    """

    def __init__(self, period: int = 14) -> None:
        self.period = int(period)
        if self.period < 2: self.period = 2

        self.prev: float | None = None
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self.value: float | None = None

    def apply_config(self, cfg: dict, key: str = "rsi_period") -> None:
        try:
            p = int(cfg.get(key, self.period))
            if p >= 2:
                self.period = p
        except Exception:
            pass

    def update(self, x: float) -> float | None:
        if not math.isfinite(x):
            return self.value

        if self.prev is None:
            self.prev = x
            return self.value

        ch = x - self.prev
        self.prev = x
        gain = max(ch, 0.0)
        loss = max(-ch, 0.0)

        if self.avg_gain is None or self.avg_loss is None:
            self.avg_gain = gain
            self.avg_loss = loss
        else:
            k = 1.0 / float(self.period)
            self.avg_gain = (1.0 - k) * self.avg_gain + k * gain
            self.avg_loss = (1.0 - k) * self.avg_loss + k * loss

        if self.avg_loss is None or self.avg_loss <= 1e-12:
            self.value = 100.0
            return self.value

        rs = (self.avg_gain or 0.0) / (self.avg_loss or 1e-12)
        self.value = 100.0 - (100.0 / (1.0 + rs))
        return self.value
