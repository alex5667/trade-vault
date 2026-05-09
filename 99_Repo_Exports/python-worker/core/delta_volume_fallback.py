from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _median(xs) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    ys = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return 0.5 * (float(ys[mid - 1]) + float(ys[mid]))


def robust_z(x: float, buf) -> float:
    """Robust z-score using median/MAD. Returns 0 when MAD is tiny."""
    if not buf:
        return 0.0
    xs = list(buf)
    med = _median(xs)
    dev = [abs(v - med) for v in xs]
    mad = _median(dev)
    if mad <= 1e-12:
        return 0.0
    # 0.6745 makes MAD comparable to std for normal
    return 0.6745 * (float(x) - med) / mad


def signed_qty_from_tick(tick: Any) -> float:
    """Best-effort signed volume delta from a trade tick.

    Supports common crypto schemas:
      - side: 'buy'/'sell' or 'B'/'S'
      - is_buyer_maker (Binance): True means aggressor is SELL
      - qty fields: qty, q, size, amount
    """
    if tick is None:
        return 0.0
    if not isinstance(tick, dict):
        # allow objects with attributes
        try:
            tick = tick.__dict__
        except Exception:
            return 0.0

    qty = (
        tick.get("qty")
        or tick.get("q")
        or tick.get("size")
        or tick.get("amount")
        or tick.get("volume")
        or 0.0
    )
    q = _f(qty, 0.0)
    if q == 0.0:
        return 0.0

    side = str(tick.get("side") or tick.get("taker_side") or "").lower()
    if side in {"buy", "b", "bid"}:
        return q
    if side in {"sell", "s", "ask"}:
        return -q

    # Binance: isBuyerMaker=True => buyer is maker => aggressor is SELL
    if "is_buyer_maker" in tick:
        try:
            ibm = bool(tick.get("is_buyer_maker"))
            return -q if ibm else q
        except Exception:
            pass
    if "m" in tick:
        try:
            ibm = bool(tick.get("m"))
            return -q if ibm else q
        except Exception:
            pass

    # fallback: unknown side
    return 0.0


@dataclass
class VolumeDeltaZState:
    window: int = 200
    buf: deque[float] = None  # type: ignore

    def __post_init__(self):
        if self.buf is None:
            self.buf = deque(maxlen=int(self.window))

    def update(self, d: float) -> float:
        self.buf.append(float(d))
        return float(robust_z(float(d), self.buf))


def volume_delta_z_from_tick(runtime: Any, tick: Any, *, window: int = 200) -> tuple[float | None, float]:
    """Return (z, raw_signed_qty) using per-runtime state.

    Designed for quarantine mode: robust and deterministic, OK to be a bit heavier.
    """
    d = signed_qty_from_tick(tick)
    if d == 0.0:
        return 0.0, 0.0

    st = getattr(runtime, "_vol_delta_z_state", None)
    if st is None:
        st = VolumeDeltaZState(window=int(window))
        runtime._vol_delta_z_state = st

    z = st.update(d)
    # clamp extreme z to avoid destabilizing downstream
    if z > 20.0:
        z = 20.0
    elif z < -20.0:
        z = -20.0
    return float(z), float(d)

