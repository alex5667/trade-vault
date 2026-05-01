from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest


@dataclass
class DummyTick:
    """
    Минимальный тик для process_tick().
    В вашем коде используются как минимум:
      - tick.ts_ms
      - tick.price / tick.last (fallbackы)
    trigger_prices() мы патчим, поэтому bid/ask здесь не нужны.
    """

    ts_ms: int
    price: float
    last: float = 0.0


def _ev_type(ev: Any) -> str:
    if isinstance(ev, dict):
        return str(ev.get("event_type") or "")
    return str(getattr(ev, "event_type", "") or "")


def _ev_payload(ev: Any) -> Dict[str, Any]:
    if isinstance(ev, dict):
        p = ev.get("payload")
        return p if isinstance(p, dict) else {}
    p = getattr(ev, "payload", None)
    return p if isinstance(p, dict) else {}


def _call_process_tick(
    process_tick: Callable[..., Any],
    *,
    pos: Any,
    spec: Any,
    tick: Any,
    tp_ratios: List[float],
) -> Tuple[List[Any], Optional[Any]]:
    """
    В проекте встречались разные сигнатуры process_tick на эволюции кода.
    Этот хелпер пробует несколько устойчивых вариантов вызова.
    """
    attempts: List[Callable[[], Any]] = [
        # Корректная сигнатура: process_tick(pos, tick, spec, tp_ratios, fill_policy)
        lambda: process_tick(pos, tick, spec, tp_ratios, "level"),
        lambda: process_tick(pos, tick, spec, tp_ratios),
        # keyword-варианты:
        lambda: process_tick(pos=pos, tick=tick, spec=spec, tp_ratios=tp_ratios, fill_policy="level"),
        lambda: process_tick(pos=pos, tick=tick, spec=spec, tp_ratios=tp_ratios),
        # Старые варианты на случай если сигнатура изменилась:
        lambda: process_tick(pos, spec, tick, tp_ratios, "level"),
        lambda: process_tick(pos, spec, tick, tp_ratios),
        lambda: process_tick(pos=pos, spec=spec, tick=tick, tp_ratios=tp_ratios, fill_policy="level"),
        lambda: process_tick(pos=pos, spec=spec, tick=tick, tp_ratios=tp_ratios)
    ]
    last_err: Optional[Exception] = None
    for fn in attempts:
        try:
            res = fn()
            # В вашем фрагменте: return events, closed
            if isinstance(res, tuple) and len(res) == 2:
                evs, closed = res
                return list(evs or []), closed
            # Если вдруг функция возвращает только events
            return list(res or []), None
        except TypeError as e:
            last_err = e
            continue
    raise AssertionError(f"Unable to call process_tick() with known signatures. Last TypeError: {last_err}")


def test_tp1_failsafe_marks_tp_hit_arms_trailing_and_rocket_enters_trailing_only(monkeypatch):
    """
    Закрывающий интеграционный тест для фикса:

    Сценарий:
      - rocket_v1 профиль
      - TP1 достигнут, но close_qty <= EPS_QTY (fail-safe ветка)

    Ожидания:
      1) TP1 всё равно фиксируется как TP_HIT (closed_qty=0) + tp_fill_times[1] установлен
      2) После TP1 вызывается arm трейлинга (раньше там мог быть pass/пропуск)
      3) Для rocket_v1 после арминга TP2/TP3 идут в trailing-only режиме (TP_HIT с trailing_only=1)
    """
    import domain.handlers as handlers
    from domain.models import PositionState

    # Гарантируем, что policy разрешает трейлинг (чтобы тест не зависел от дефолтов ENV).
    monkeypatch.setenv("TRAIL_FORCE_ALWAYS_AFTER_TP1", "1")
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")

    # 1) Делаем так, чтобы ВСЕ TP уровни считались достигнутыми в этом тике,
    #     но SL не срабатывал.
    def fake_trigger_prices(tick: Any, direction: str):
        tp_px = 10_000.0  # выше любых TP для LONG
        sl_px = 10_000.0  # SL не достигнут (для LONG SL должен быть ниже)
        mid = float(getattr(tick, "price", 0.0) or 0.0) or 100.0
        return tp_px, sl_px, mid

    monkeypatch.setattr(handlers, "trigger_prices", fake_trigger_prices)

    # 2) Арминг трейлинга делаем детерминированным и наблюдаемым.
    #    Это оставляет test "интеграционным" относительно process_tick TP-loop,
    #    но убирает зависимость от возможных деталей реализации maybe_arm_trailing_after_tp1().
    def fake_maybe_arm_trailing_after_tp1(pos: Any, spec: Any, ts_ms: int):
        pos.trailing_started = True
        pos.trailing_active = True
        pos.trailing_armed_ts_ms = int(ts_ms)
        pos.trailing_start_reason = "TEST_ARM"
        # Возвращаем либо настоящий TradeEvent, либо dict (оба формата поддерживаем в ассерт-хелперах).
        try:
            TradeEvent = handlers.TradeEvent
            return TradeEvent(
                event_type="TRAILING_SYNC",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts_ms),
                payload={"reason": "TEST_ARM"},
            )
        except Exception:
            return {"event_type": "TRAILING_SYNC", "payload": {"reason": "TEST_ARM"}}

    monkeypatch.setattr(handlers, "maybe_arm_trailing_after_tp1", fake_maybe_arm_trailing_after_tp1)

    # 3) Минимальный spec с нужной сигнатурой pnl_money (используется для MFE/MAE и для TP частичного PnL).
    class FakeSpec:
        def pnl_money(self, entry_price: float, price: float, lot: float, direction: str, symbol="") -> float:
            sign = 1.0 if str(direction).upper() == "LONG" else -1.0
            return (float(price) - float(entry_price)) * sign * float(lot)

    spec = FakeSpec()

    # 4) Готовим позицию:
    #    - remaining_qty > EPS_QTY (иначе while не зайдёт)
    #    - но tp_ratios[0] такой, чтобы close_qty = lot*ratio <= EPS_QTY => fail-safe ветка
    eps = float(getattr(handlers, "EPS_QTY", 1e-12))
    lot = 1.0
    ratio_tp1 = max(eps / 2.0, 1e-18)  # close_qty = lot*ratio = eps/2 <= eps

    pos = PositionState(
        id="pos1",
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",  # type: ignore
        entry_price=100.0,
        entry_ts_ms=1_700_000_000_000,
        lot=lot,
        remaining_qty=eps * 2.0,
        sl=90.0,
        tp_levels=[101.0, 102.0, 103.0],
        # Важно: rocket_v1
        trail_profile="rocket_v1",
        # Fail-open флаг (если TRAIL_COND_ENABLED=1) — разрешаем трейл
        trail_after_tp1=True,
        trail_after_tp1_reason="TEST",
    )

    # excursions init (как делает create_position)
    pos.max_price_seen = pos.entry_price
    pos.min_price_seen = pos.entry_price
    pos.max_favorable_price = pos.entry_price
    pos.max_favorable_ts = pos.entry_ts_ms

    tick = DummyTick(ts_ms=pos.entry_ts_ms + 30_000, price=100.0, last=100.0)

    tp_ratios = [ratio_tp1, 0.5, 1.0]

    events, closed = _call_process_tick(handlers.process_tick, pos=pos, spec=spec, tick=tick, tp_ratios=tp_ratios)

    # --- 1) TP1 должен быть зафиксирован, даже если close_qty был слишком мал ---
    assert pos.tp1_hit is True
    assert isinstance(pos.tp_fill_times, dict)
    assert int(pos.tp_fill_times.get(1) or 0) == int(tick.ts_ms)

    tp_events = [ev for ev in events if _ev_type(ev) == "TP_HIT"]
    assert any((_ev_payload(ev).get("tp_level") == 1 and float(_ev_payload(ev).get("closed_qty") or 0.0) == 0.0) for ev in tp_events)

    # --- 2) Арминг трейлинга должен сработать после TP1 ---
    assert pos.trailing_started is True
    assert any(_ev_type(ev) == "TRAILING_SYNC" for ev in events)

    # --- 3) rocket_v1 после арминга: TP2/TP3 должны стать trailing-only hits ---
    assert any((_ev_payload(ev).get("tp_level") == 2 and int(_ev_payload(ev).get("trailing_only") or 0) == 1) for ev in tp_events)
    assert any((_ev_payload(ev).get("tp_level") == 3 and int(_ev_payload(ev).get("trailing_only") or 0) == 1) for ev in tp_events)

    assert pos.tp2_hit is True
    assert pos.tp3_hit is True
    assert int(pos.tp_hits) == 3

    # В rocket_v1 "trailing-only" режиме позиция НЕ обязана закрываться на TP3 в этом loop.
    assert pos.closed is False
    assert closed is None
