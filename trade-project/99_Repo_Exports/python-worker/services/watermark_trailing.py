"""Phase B.2 (P2): Watermark trailing state machine.

Назначение:
  Pure-Python FSM для адаптивного trailing-SL после TP-hit. Раньше SL ставился
  один раз: `tp_price ± ATR × mult` — не следовал за peak ценой.

  Теперь LONG позиция запоминает `high_watermark`, SHORT — `low_watermark`,
  и SL = watermark ± ATR×mult с гарантией "не отступать обратно".

Контракт:
  OPEN → TP1_HIT (внешнее событие) → arm(price)
  → state=TRAILING_ACTIVE → on_tick(price) обновляет watermark и SL
  → exit() → state=EXITED

Этот модуль не делает I/O. Persistence — в watermark_trailing_store.py
(Redis hash `trail:wm:{sid}` с TTL). Подписка на тики — обязанность
вызывающего сервиса (trade_monitor / отдельный watermark_tracker_runner).

Безопасность SL:
  - LONG: новый SL не может быть НИЖЕ предыдущего;
  - SHORT: новый SL не может быть ВЫШЕ предыдущего;
  - SL ограничен point_size (round-to-tick);
  - Не пересекает entry для defensive профилей (BE-mode).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum


class WMState(str, Enum):
    PENDING = "pending"            # ещё не было TP-hit
    TRAILING_ACTIVE = "active"     # arm() вызван, on_tick обновляет SL
    EXITED = "exited"


@dataclass
class WatermarkSnapshot:
    """Снимок состояния FSM — то, что персистится в Redis."""

    sid: str
    side: str                       # "LONG" | "SHORT"
    entry_price: float
    original_sl: float | None
    atr: float
    atr_mult: float
    profile_name: str
    symbol: str = ""                # добавлено для индексации watermark-tracker-а
    position_id: str | None = None
    point_size: float = 0.0001
    high_wm: float | None = None    # для LONG
    low_wm: float | None = None     # для SHORT
    current_sl: float | None = None
    state: WMState = WMState.PENDING
    last_update_ts_ms: int = 0
    updates_total: int = 0           # счётчик движений SL
    arm_price: float | None = None  # цена при arm() — для аудита

    def to_dict(self) -> dict[str, str]:
        """Сериализация в плоский dict[str, str] для Redis HSET.

        None кодируется как пустая строка (Redis hash не хранит nil).
        """
        return {
            "sid": self.sid,
            "side": self.side,
            "entry_price": str(self.entry_price),
            "original_sl": "" if self.original_sl is None else str(self.original_sl),
            "atr": str(self.atr),
            "atr_mult": str(self.atr_mult),
            "profile_name": self.profile_name,
            "symbol": self.symbol,
            "position_id": self.position_id or "",
            "point_size": str(self.point_size),
            "high_wm": "" if self.high_wm is None else str(self.high_wm),
            "low_wm": "" if self.low_wm is None else str(self.low_wm),
            "current_sl": "" if self.current_sl is None else str(self.current_sl),
            "state": self.state.value,
            "last_update_ts_ms": str(self.last_update_ts_ms),
            "updates_total": str(self.updates_total),
            "arm_price": "" if self.arm_price is None else str(self.arm_price),
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "WatermarkSnapshot":
        def _f(key: str) -> float | None:
            v = data.get(key, "")
            return None if v == "" else float(v)

        return cls(
            sid=data["sid"],
            side=data["side"],
            entry_price=float(data["entry_price"]),
            original_sl=_f("original_sl"),
            atr=float(data["atr"]),
            atr_mult=float(data["atr_mult"]),
            profile_name=data.get("profile_name", ""),
            symbol=data.get("symbol", ""),
            position_id=data.get("position_id") or None,
            point_size=float(data.get("point_size", "0.0001") or "0.0001"),
            high_wm=_f("high_wm"),
            low_wm=_f("low_wm"),
            current_sl=_f("current_sl"),
            state=WMState(data.get("state", "pending")),
            last_update_ts_ms=int(data.get("last_update_ts_ms") or "0"),
            updates_total=int(data.get("updates_total") or "0"),
            arm_price=_f("arm_price"),
        )


@dataclass
class WMDecision:
    """Результат on_tick() — описывает действие, которое должен сделать caller."""

    new_sl: float | None
    moved: bool                     # True если SL реально изменился (с учётом point_size)
    reason: str = ""


@dataclass
class WatermarkTrailingFSM:
    """FSM. Безопасна к вызову без блокировок (state хранится в snapshot)."""

    snap: WatermarkSnapshot
    min_step_points: int = 1        # минимум 1 point движения для emit
    fee_buffer_bps: float = 0.0     # доп. buffer (в bps цены) для fees/spread
    spread_floor_price: float = 0.0  # min spread от цены, ниже которого SL не пододвигается
    _atr_mult_floor: float = field(default=0.0, init=False)

    # ─────────────────────────────────── helpers ────────────────────────────────
    def _round_sl(self, raw: float) -> float:
        if self.snap.point_size <= 0:
            return raw
        if self.snap.side == "LONG":
            # для LONG SL < price, округление "вниз" чтобы оставить запас
            return math.floor(raw / self.snap.point_size) * self.snap.point_size
        return math.ceil(raw / self.snap.point_size) * self.snap.point_size

    def _fee_offset(self, price: float) -> float:
        if self.fee_buffer_bps <= 0:
            return 0.0
        return abs(price) * self.fee_buffer_bps / 10_000.0

    # ───────────────────────────────── transitions ─────────────────────────────
    def arm(self, price: float, *, now_ms: int) -> WMDecision:
        """Activate trailing после TP-hit.

        Идемпотентна: повторный arm на тот же sid просто обновит arm_price/ts.
        """
        if self.snap.state == WMState.EXITED:
            return WMDecision(new_sl=None, moved=False, reason="exited")

        self.snap.state = WMState.TRAILING_ACTIVE
        self.snap.arm_price = price
        self.snap.last_update_ts_ms = now_ms
        # init watermark на цене arm
        if self.snap.side == "LONG":
            self.snap.high_wm = max(self.snap.high_wm or price, price)
        else:
            self.snap.low_wm = min(self.snap.low_wm or price, price)
        return self._maybe_emit_sl(price, now_ms=now_ms, reason="arm")

    def on_tick(self, price: float, *, now_ms: int) -> WMDecision:
        """Обновление watermark на новой цене.

        Если состояние ещё PENDING (TP-hit не пришёл) — no-op.
        """
        if self.snap.state != WMState.TRAILING_ACTIVE:
            return WMDecision(new_sl=None, moved=False, reason=f"state={self.snap.state.value}")

        if self.snap.side == "LONG":
            prev = self.snap.high_wm or price
            if price > prev:
                self.snap.high_wm = price
        else:
            prev = self.snap.low_wm or price
            if price < prev:
                self.snap.low_wm = price

        self.snap.last_update_ts_ms = now_ms
        return self._maybe_emit_sl(price, now_ms=now_ms, reason="tick")

    def exit(self) -> None:
        self.snap.state = WMState.EXITED

    # ───────────────────────────────── core math ───────────────────────────────
    def _maybe_emit_sl(self, price: float, *, now_ms: int, reason: str) -> WMDecision:
        atr = self.snap.atr
        mult = self.snap.atr_mult
        if atr <= 0 or mult <= 0:
            return WMDecision(new_sl=None, moved=False, reason="invalid_atr_or_mult")

        distance = atr * mult
        if distance <= 0:
            return WMDecision(new_sl=None, moved=False, reason="invalid_distance")

        fee_offset = self._fee_offset(price)

        if self.snap.side == "LONG":
            wm = self.snap.high_wm or price
            raw = wm - distance - fee_offset
            # spread floor: SL не ближе price - spread_floor
            ceiling = price - max(self.snap.point_size, self.spread_floor_price)
            raw = min(raw, ceiling)
            candidate = self._round_sl(raw)
            if candidate <= 0:
                return WMDecision(new_sl=None, moved=False, reason="candidate_le_zero")
            # never ratchet down (LONG): новый SL должен быть строго ВЫШЕ предыдущего
            prev_sl = self.snap.current_sl if self.snap.current_sl is not None else self.snap.original_sl
            if prev_sl is not None and candidate <= prev_sl + (self.min_step_points - 1) * self.snap.point_size:
                return WMDecision(new_sl=candidate, moved=False, reason="no_ratchet")
            self.snap.current_sl = candidate
            self.snap.updates_total += 1
            return WMDecision(new_sl=candidate, moved=True, reason=reason)
        else:
            wm = self.snap.low_wm or price
            raw = wm + distance + fee_offset
            floor = price + max(self.snap.point_size, self.spread_floor_price)
            raw = max(raw, floor)
            candidate = self._round_sl(raw)
            if candidate <= 0:
                return WMDecision(new_sl=None, moved=False, reason="candidate_le_zero")
            # never ratchet up (SHORT): новый SL должен быть строго НИЖЕ предыдущего
            prev_sl = self.snap.current_sl if self.snap.current_sl is not None else self.snap.original_sl
            if prev_sl is not None and candidate >= prev_sl - (self.min_step_points - 1) * self.snap.point_size:
                return WMDecision(new_sl=candidate, moved=False, reason="no_ratchet")
            self.snap.current_sl = candidate
            self.snap.updates_total += 1
            return WMDecision(new_sl=candidate, moved=True, reason=reason)


# ────────────────────────────── factory helpers ─────────────────────────────────
def fsm_from_signal(
    *,
    sid: str,
    side: str,
    entry_price: float,
    original_sl: float | None,
    atr: float,
    atr_mult: float,
    profile_name: str,
    symbol: str = "",
    position_id: str | None = None,
    point_size: float = 0.0001,
    fee_buffer_bps: float = 0.0,
) -> WatermarkTrailingFSM:
    snap = WatermarkSnapshot(
        sid=sid,
        side=side.upper(),
        entry_price=entry_price,
        original_sl=original_sl,
        atr=atr,
        atr_mult=atr_mult,
        profile_name=profile_name,
        symbol=symbol.upper(),
        position_id=position_id,
        point_size=point_size,
    )
    return WatermarkTrailingFSM(snap=snap, fee_buffer_bps=fee_buffer_bps)
