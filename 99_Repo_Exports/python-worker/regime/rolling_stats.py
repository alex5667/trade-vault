from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque


@dataclass
class RollingWindowStats:
    window_size: int
    r_values: Deque[float] = field(default_factory=deque)

    def add(self, r: float) -> None:
        self.r_values.append(r)
        if len(self.r_values) > self.window_size:
            self.r_values.popleft()

    @property
    def trades(self) -> int:
        return len(self.r_values)

    @property
    def hitrate(self) -> float:
        if not self.r_values:
            return 0.0
        wins = sum(1 for x in self.r_values if x > 0)
        return wins / len(self.r_values)

    @property
    def expectancy_r(self) -> float:
        if not self.r_values:
            return 0.0
        return sum(self.r_values) / len(self.r_values)

    @property
    def dd_r(self) -> float:
        """
        Max drawdown по R внутри окна (отрицательное число, например -5.3R).
        Считаем по equity-path.
        """
        if not self.r_values:
            return 0.0

        equity = 0.0
        max_equity = 0.0
        max_dd = 0.0  # отрицательная просадка

        for r in self.r_values:
            equity += r
            if equity > max_equity:
                max_equity = equity
            dd = equity - max_equity  # <= 0
            if dd < max_dd:
                max_dd = dd

        return max_dd
