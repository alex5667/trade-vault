import math

class GapEmaTracker:
    """
    Exponential Moving Average tracker for sequence gaps.
    Outputs a value between 0.0 and 1.0.
    """
    __slots__ = ('tau_ms', 'ema', 'last_ts_ms')

    def __init__(self, tau_ms: int = 10000):
        self.tau_ms = max(1, int(tau_ms))
        self.ema = 0.0
        self.last_ts_ms = 0
    
    def update(self, is_gap: bool, ts_ms: int) -> float:
        is_gap_val = 1.0 if is_gap else 0.0
        # Initialize on first update
        if self.last_ts_ms <= 0:
            self.last_ts_ms = int(ts_ms)
            self.ema = is_gap_val
            return self.ema
        
        dt = int(ts_ms) - self.last_ts_ms
        if dt < 0:
            dt = 0
        
        alpha = 1.0 - math.exp(-dt / self.tau_ms)
        self.ema = self.ema + alpha * (is_gap_val - self.ema)
        self.last_ts_ms = int(ts_ms)
        return self.ema
