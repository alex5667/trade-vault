from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass
from typing import Any, Optional
import os
import time


@dataclass
class Pending:
    signal_id: str
    symbol: str
    kind: str
    side: int
    level_price: float
    ts_ms: int
    horizon_ms: int
    min_hold_bps: float


class OutcomeLabeler:
    """
    Post-breakout acceptance (НЕ онлайн-фильтр):
      - сохраняем pending для breakout
      - по истечению horizon проверяем удержание относительно уровня
      - выдаём label-event (для обучения порогов/калибровки)
    """
    def __init__(self) -> None:
        self._horizon_ms = int(os.getenv("LABEL_BO_HORIZON_MS", "15000"))
        self._hold_bps = float(os.getenv("LABEL_BO_MIN_HOLD_BPS", "2.0"))
        self._pending: dict[str, Pending] = {}

    def _now_ms(self) -> int:
        return get_ny_time_millis()

    def register_breakout(self, *, signal_id: str, ctx: Any, side: int, level_price: Optional[float]) -> None:
        sym = str(getattr(ctx, "symbol", "") or "")
        ts_ms = int(getattr(ctx, "ts", None) or self._now_ms())
        if not sym or level_price is None:
            return
        self._pending[signal_id] = Pending(
            signal_id=signal_id,
            symbol=sym,
            kind="breakout",
            side=int(side),
            level_price=float(level_price),
            ts_ms=ts_ms,
            horizon_ms=self._horizon_ms,
            min_hold_bps=self._hold_bps,
        )

    def on_ctx(self, ctx: Any) -> list[dict[str, Any]]:
        now = int(getattr(ctx, "ts", None) or self._now_ms())
        sym = str(getattr(ctx, "symbol", "") or "")
        price = float(getattr(ctx, "price", 0.0) or 0.0)
        if not sym or price <= 0:
            return []

        out: list[dict[str, Any]] = []
        done: list[str] = []
        for sid, p in self._pending.items():
            if p.symbol != sym:
                continue
            if (now - p.ts_ms) < p.horizon_ms:
                continue

            # acceptance: удержались "за уровнем" на min_hold_bps
            hold_bps = abs(price - p.level_price) / max(p.level_price, 1e-9) * 10_000.0
            accepted = False
            if p.side >= 0:
                accepted = (price >= p.level_price) and (hold_bps >= p.min_hold_bps)
            else:
                accepted = (price <= p.level_price) and (hold_bps >= p.min_hold_bps)

            out.append(
                {
                    "kind": "label_update",
                    "signal_id": p.signal_id,
                    "symbol": p.symbol,
                    "ts": now,
                    "label": "post_breakout_accept",
                    "value": 1 if accepted else 0,
                    "meta": {"hold_bps": float(hold_bps), "level_price": float(p.level_price), "side": int(p.side)},
                }
            )
            done.append(sid)

        for sid in done:
            self._pending.pop(sid, None)
        return out
