from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.eq_pools import EQPool
from core.microbar import MicroBar


@dataclass
class SweepEvent:
    """
    Подтвержденный sweep (liquidity raid + return).

    kind:
      - "EQH_SWEEP": рейд над EQH + возврат закрытием ниже уровня
      - "EQL_SWEEP": рейд под EQL + возврат закрытием выше уровня
    direction_bias:
      - EQH_SWEEP => "SHORT"
      - EQL_SWEEP => "LONG"
    """
    kind: str
    direction_bias: str
    ts_ms: int                 # ts подтверждения (бар, на котором подтвердили)
    pool_id: str
    pool_kind: str             # EQH/EQL
    level: float
    touches: int
    tol_px: float
    breach_ts_ms: int
    breach_px: float
    confirm_px: float


@dataclass
class _Pending:
    pool_id: str
    pool_kind: str
    level: float
    tol_px: float
    breach_ts_ms: int
    breach_px: float
    expire_ts_ms: int


class SweepDetector:
    """
    Sweep detector на микро-барах.

    Логика:
    - Ищем breach уровня (high/low ушёл за level +- tol)
    - Затем ждём подтверждения "return" (close вернулся через уровень) в течение N баров
      (confirm_bars * tf_ms)

    Дедупликация:
    - cooldown_ms на один pool_id, чтобы не спамить одним уровнем
    """

    def __init__(
        self,
        confirm_bars: int = 3,
        cooldown_ms: int = 60_000,
        valid_ms: int = 120_000,   # окно "актуальности" sweep-события для attach к delta_spike
    ) -> None:
        self.confirm_bars = int(confirm_bars)
        self.cooldown_ms = int(cooldown_ms)
        self.valid_ms = int(valid_ms)

        self._pending: dict[str, _Pending] = {}
        self._last_emit_ts: dict[str, int] = {}

    def apply_config(self, cfg: dict[str, Any]) -> None:
        try:
            self.confirm_bars = int(cfg.get("sweep_confirm_bars", self.confirm_bars))
            if self.confirm_bars < 1:
                self.confirm_bars = 1
        except Exception:
            pass
        try:
            self.cooldown_ms = int(cfg.get("sweep_cooldown_ms", self.cooldown_ms))
            if self.cooldown_ms < 0:
                self.cooldown_ms = 0
        except Exception:
            pass
        try:
            self.valid_ms = int(cfg.get("sweep_valid_ms", self.valid_ms))
            if self.valid_ms < 5_000:
                self.valid_ms = 5_000
        except Exception:
            pass

    def _cooldown_ok(self, pool_id: str, now_ts_ms: int) -> bool:
        last = self._last_emit_ts.get(pool_id)
        if last is None:
            return True
        return (now_ts_ms - last) >= self.cooldown_ms

    def _mark_emit(self, pool_id: str, now_ts_ms: int) -> None:
        self._last_emit_ts[pool_id] = now_ts_ms

    def update_bar(self, bar: MicroBar, pools: list[EQPool]) -> list[SweepEvent]:
        """
        Вызывается на каждом bar_close.
        Возвращает 0..k sweep событий.
        """
        out: list[SweepEvent] = []
        now_ts = int(bar.end_ts_ms)
        tf_ms = int(bar.tf_ms) if getattr(bar, "tf_ms", 0) else 1000
        expire_add = self.confirm_bars * tf_ms

        # 1) Сначала проверяем существующие pending
        to_drop: list[str] = []
        confirmed_this_bar: set[str] = set()
        for pid, pend in self._pending.items():
            if now_ts > pend.expire_ts_ms:
                to_drop.append(pid)
                continue

            # confirmation = return through level
            if pend.pool_kind == "EQH":
                if float(bar.close) < float(pend.level):
                    if self._cooldown_ok(pid, now_ts):
                        ev = SweepEvent(
                            kind="EQH_SWEEP",
                            direction_bias="SHORT",
                            ts_ms=now_ts,
                            pool_id=pid,
                            pool_kind="EQH",
                            level=float(pend.level),
                            touches=0,
                            tol_px=float(pend.tol_px),
                            breach_ts_ms=int(pend.breach_ts_ms),
                            breach_px=float(pend.breach_px),
                            confirm_px=float(bar.close),
                        )
                        out.append(ev)
                        self._mark_emit(pid, now_ts)
                        confirmed_this_bar.add(pid)
                    to_drop.append(pid)

            elif pend.pool_kind == "EQL":
                if float(bar.close) > float(pend.level):
                    if self._cooldown_ok(pid, now_ts):
                        ev = SweepEvent(
                            kind="EQL_SWEEP",
                            direction_bias="LONG",
                            ts_ms=now_ts,
                            pool_id=pid,
                            pool_kind="EQL",
                            level=float(pend.level),
                            touches=0,
                            tol_px=float(pend.tol_px),
                            breach_ts_ms=int(pend.breach_ts_ms),
                            breach_px=float(pend.breach_px),
                            confirm_px=float(bar.close),
                        )
                        out.append(ev)
                        self._mark_emit(pid, now_ts)
                        confirmed_this_bar.add(pid)
                    to_drop.append(pid)

        for pid in to_drop:
            self._pending.pop(pid, None)

        # 2) Потом — детект новых breach (если ещё не pending и не был только что confirmed)
        # Сканируем mature пулы
        for p in pools:
            pid = p.pool_id
            if pid in self._pending or pid in confirmed_this_bar:
                continue
            if not self._cooldown_ok(pid, now_ts):
                continue

            level = float(p.level)
            tol = float(p.last_tol_px or 0.0)
            if tol <= 0:
                tol = 0.0

            if p.kind == "EQH":
                # breach: high ушёл выше level + tol
                if float(bar.high) > (level + tol):
                    # immediate confirm if close already returned ниже
                    if float(bar.close) < level:
                        ev = SweepEvent(
                            kind="EQH_SWEEP",
                            direction_bias="SHORT",
                            ts_ms=now_ts,
                            pool_id=pid,
                            pool_kind="EQH",
                            level=level,
                            touches=int(p.touches),
                            tol_px=tol,
                            breach_ts_ms=now_ts,
                            breach_px=float(bar.high),
                            confirm_px=float(bar.close),
                        )
                        out.append(ev)
                        self._mark_emit(pid, now_ts)
                    else:
                        # pending until return occurs
                        self._pending[pid] = _Pending(
                            pool_id=pid,
                            pool_kind="EQH",
                            level=level,
                            tol_px=tol,
                            breach_ts_ms=now_ts,
                            breach_px=float(bar.high),
                            expire_ts_ms=now_ts + expire_add,
                        )

            elif p.kind == "EQL":
                # breach: low ушёл ниже level - tol
                if float(bar.low) < (level - tol):
                    if float(bar.close) > level:
                        ev = SweepEvent(
                            kind="EQL_SWEEP",
                            direction_bias="LONG",
                            ts_ms=now_ts,
                            pool_id=pid,
                            pool_kind="EQL",
                            level=level,
                            touches=int(p.touches),
                            tol_px=tol,
                            breach_ts_ms=now_ts,
                            breach_px=float(bar.low),
                            confirm_px=float(bar.close),
                        )
                        out.append(ev)
                        self._mark_emit(pid, now_ts)
                    else:
                        self._pending[pid] = _Pending(
                            pool_id=pid,
                            pool_kind="EQL",
                            level=level,
                            tol_px=tol,
                            breach_ts_ms=now_ts,
                            breach_px=float(bar.low),
                            expire_ts_ms=now_ts + expire_add,
                        )

        return out
