from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import math


@dataclass
class ReclaimEvent:
    """
    Reclaim confirmed:
    - after sweep(return) event
    - hold N bars on the "inside" side without re-raid
    """
    ts_ms: int
    pool_id: str
    kind: str          # "EQH" | "EQL"
    level: float
    tol_px: float
    hold_bars: int
    direction_bias: str  # "LONG" | "SHORT"


@dataclass
class _Pending:
    pool_id: str
    kind: str
    level: float
    tol_px: float
    direction_bias: str
    since_ts_ms: int
    bars_ok: int
    expire_ts_ms: int


class ReclaimDetector:
    """
    FSM:
      on_sweep_return() -> pending
      on_bar_close() -> if holds inside for N bars and no re-raid => emit reclaim
    """
    def __init__(self, hold_bars: int = 2, valid_ms: int = 120_000) -> None:
        self.hold_bars = int(hold_bars)
        self.valid_ms = int(valid_ms)
        self._pending: Optional[_Pending] = None
        self._last: Optional[ReclaimEvent] = None

    def apply_config(self, cfg: Dict[str, Any]) -> None:
        try:
            self.hold_bars = int(cfg.get("reclaim_hold_bars", self.hold_bars))
            if self.hold_bars < 1:
                self.hold_bars = 1
        except Exception:
            pass
        try:
            self.valid_ms = int(cfg.get("reclaim_valid_ms", self.valid_ms))
            if self.valid_ms < 5_000:
                self.valid_ms = 5_000
        except Exception:
            pass

    def on_sweep_return(self, sweep_event: Any) -> None:
        """
        sweep_event is expected to have:
          pool_id, pool_kind(EQH/EQL), level, tol_px, ts_ms, direction_bias
        """
        try:
            ts = int(getattr(sweep_event, "ts_ms"))
            pid = str(getattr(sweep_event, "pool_id"))
            # handle both naming conventions if present, bias towards obj attribute
            if hasattr(sweep_event, "pool_kind"):
                kind = str(getattr(sweep_event, "pool_kind"))
            else:
                kind = str(getattr(sweep_event, "kind")) # fallback
            
            lvl = float(getattr(sweep_event, "level"))
            tol = float(getattr(sweep_event, "tol_px"))
            db = str(getattr(sweep_event, "direction_bias")).upper()
        except Exception:
            return

        self._pending = _Pending(
            pool_id=pid,
            kind=kind,
            level=lvl,
            tol_px=tol,
            direction_bias=db,
            since_ts_ms=ts,
            bars_ok=0,
            expire_ts_ms=ts + self.valid_ms,
        )

    def on_bar_close(self, bar: Any) -> Optional[ReclaimEvent]:
        if self._pending is None:
            return None

        ts = int(getattr(bar, "end_ts_ms", 0) or 0)
        if ts <= 0 or ts > self._pending.expire_ts_ms:
            self._pending = None
            return None

        lvl = float(self._pending.level)
        tol = float(self._pending.tol_px)
        c = float(getattr(bar, "close", 0.0) or 0.0)
        h = float(getattr(bar, "high", c) or c)
        l = float(getattr(bar, "low", c) or c)

        # inside condition + no re-raid
        if self._pending.kind == "EQH":
            # after EQH sweep, inside is BELOW level
            inside = (c < lvl)
            re_raid = (h > (lvl + tol))
        else:
            # after EQL sweep, inside is ABOVE level
            inside = (c > lvl)
            re_raid = (l < (lvl - tol))
        
        if re_raid:
            # fail fast: new raid cancels reclaim
            self._pending = None
            return None

        if inside:
            self._pending.bars_ok += 1
        else:
            # lost holding -> reset counter but keep pending until expire
            self._pending.bars_ok = 0

        if self._pending.bars_ok >= self.hold_bars:
            ev = ReclaimEvent(
                ts_ms=ts,
                pool_id=self._pending.pool_id,
                kind=self._pending.kind,
                level=lvl,
                tol_px=tol,
                hold_bars=self.hold_bars,
                direction_bias=self._pending.direction_bias,
            )
            self._last = ev
            self._pending = None
            return ev

        return None

    def last(self) -> Optional[ReclaimEvent]:
        return self._last
