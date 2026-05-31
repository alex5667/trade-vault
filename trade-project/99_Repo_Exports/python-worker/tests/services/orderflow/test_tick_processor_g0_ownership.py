"""G0 ownership tests: tick_processor must not pre-write runtime.last_ts_ms,
so that G0 (strategy.process_tick) can detect backward / clamp / quarantine
ticks. Also covers metric wiring for bad_ts → tick_ts_missing_total and
unknown_side → ticks_side_unknown_total.
"""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.orderflow.components.tick_processor import TickProcessor
from services.orderflow.components.unknown_side_policy import UnknownSidePolicyHandler


def _make_processor(*, dq_policy, strategy, side_policy: str = "ignore_delta") -> TickProcessor:
    main = AsyncMock()
    ticks = AsyncMock()
    gate = AsyncMock()
    gate.allows.return_value = True
    flusher = MagicMock()
    flusher.process = AsyncMock(return_value=None)
    return TickProcessor(
        tick_dq_policy=dq_policy,
        strategy_fn=lambda: strategy,
        gate=gate,
        flusher=flusher,
        health_metrics=None,
        main_redis=main,
        ticks_redis=ticks,
        drop_on_lag=False,
        max_lag_ms=60_000,
        max_ts_skew_ms=5_000,
        unknown_side_policy=side_policy,
        unknown_side_quarantine_stream="stream:tick_dq:quarantine",
        unknown_side_quarantine_sample=0.0,
        unknown_side_quarantine_maxlen=1000,
        exec_quarantine_enable=False,
        quarantine_stream="stream:tick_dq:quarantine",
        lag_trackers={},
        lag_export_counters={},
    )


@pytest.mark.asyncio
async def test_tick_processor_does_not_pre_write_runtime_last_ts_ms(monkeypatch):
    """G0 contract: at the moment strat.process_tick is invoked, runtime.last_ts_ms
    must still hold the previous tick's ts (or 0), NOT the current tick's ts.
    If this regresses, the backward/clamp/quarantine branch in tick_decision_engine
    becomes unreachable."""

    # Bypass dedup so we don't need a real runtime impl.
    monkeypatch.setattr(
        "services.orderflow.components.tick_processor.is_duplicate_tick",
        lambda *a, **kw: False,
    )

    dq_policy = MagicMock()
    dq_policy.validate.return_value = (True, "pass")

    observed: dict = {}
    strategy = MagicMock()

    async def fake_process_tick(runtime, tick, **kw):
        observed["last_ts_ms_at_strat"] = runtime.last_ts_ms
        observed["tick_ts_ms"] = tick.get("ts_ms")
        return None

    strategy.process_tick = fake_process_tick

    tp = _make_processor(dq_policy=dq_policy, strategy=strategy)

    now_ms = int(time.time() * 1000)
    prev_ts = now_ms - 100  # set BEFORE call so coerce_event_ts_ms uses payload ts
    new_ts = now_ms - 50

    runtime = MagicMock()
    runtime.last_ts_ms = prev_ts  # previous tick (G0 should still see this, not new_ts)
    runtime.is_duplicate_tick_uid = lambda uid: False

    fields = {
        "symbol": "BTCUSDT",
        "ts_ms": str(new_ts),
        "price": "50000",
        "qty": "0.1",
        "side": "BUY",
    }

    ok = await tp.process_tick(runtime, msg_id="0-0", fields=fields, symbol="BTCUSDT")
    assert ok is True

    # Crucial assertion: G0 sees the PREVIOUS last_ts_ms, not the current tick's ts.
    assert observed["last_ts_ms_at_strat"] == prev_ts, (
        "tick_processor pre-wrote runtime.last_ts_ms — G0 monotonicity branch will be dead. "
        f"Observed: {observed}"
    )
    assert observed["tick_ts_ms"] == new_ts


@pytest.mark.asyncio
async def test_dq_bad_ts_increments_tick_ts_missing_metric(monkeypatch):
    """When DQ rejects with bad_ts / bad_ts_unit we must bump tick_ts_missing_total
    (the G0-spec metric for missing timestamps)."""
    from services.orderflow import metrics as m

    dq_policy = MagicMock()
    dq_policy.validate.return_value = (False, "bad_ts")

    strategy = MagicMock()
    tp = _make_processor(dq_policy=dq_policy, strategy=strategy)

    runtime = MagicMock()
    runtime.last_ts_ms = 0
    runtime.is_duplicate_tick_uid = lambda uid: False

    sym = "G0TESTSYM1"

    def _value() -> float:
        try:
            return m.tick_ts_missing_total.labels(symbol=sym)._value.get()  # type: ignore[attr-defined]
        except Exception:
            return 0.0

    before = _value()

    fields = {
        "symbol": sym,
        "ts_ms": "0",
        "price": "1",
        "qty": "1",
        "side": "BUY",
    }
    await tp.process_tick(runtime, msg_id="0-0", fields=fields, symbol=sym)

    assert _value() == before + 1.0


@pytest.mark.asyncio
async def test_unknown_side_increments_side_unknown_metric():
    """Unknown-side ticks must increment ticks_side_unknown_total regardless of policy."""
    from services.orderflow import metrics as m

    handler = UnknownSidePolicyHandler(side_policy="ignore_delta")
    sym = "G0TESTSYM2"

    def _value() -> float:
        try:
            return m.ticks_side_unknown_total.labels(symbol=sym)._value.get()  # type: ignore[attr-defined]
        except Exception:
            return 0.0

    before = _value()

    tick = {"side": "UNKNOWN"}
    skip = await handler.apply_policy(tick, unknown_side=True, symbol=sym, msg_id="0-0", raw={})

    # ignore_delta policy does NOT skip — it marks fields. We still want the metric counted.
    assert skip is False
    assert _value() == before + 1.0
