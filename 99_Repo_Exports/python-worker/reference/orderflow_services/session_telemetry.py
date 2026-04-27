
class PassRateEmaBySession:
    """EMA(pass) per (tier, session). Useful for pass-rate telemetry segmented by sessions."""

    def __init__(self, *, alpha: float = 0.05):
        a = float(alpha)
        if not (0.0 < a <= 1.0):
            a = 0.05
        self.alpha = a
        self._ema = {}  # (tier:int, session:str) -> float

    def update(self, *, tier: int, session: str, passed: bool) -> float:
        key = (int(tier), str(session))
        prev = float(self._ema.get(key, 0.5))
        x = 1.0 if bool(passed) else 0.0
        v = (1.0 - self.alpha) * prev + self.alpha * x
        self._ema[key] = float(v)
        return float(v)

    def get(self, *, tier: int, session: str) -> float:
        return float(self._ema.get((int(tier), str(session)), 0.0))
