"""Integration tests for ConfirmationBarrier wiring in SignalPipeline.

Verifies that publish_signal:
  * publishes inline when barrier mode == "off"
  * defers publish when barrier mode == "enforce" and a follow-through is
    required
  * re-publishes through barrier_poll_and_publish when the deadline is met
  * drops the signal when no follow-through

The full SignalPipeline.__init__ pulls a huge dependency graph (Redis, ML
calibrators, etc). Rather than instantiate that, we test the four barrier
helpers as small isolated units bound to a stub instance.
"""
from __future__ import annotations

import asyncio
import importlib

import pytest

from core.confirmation_barrier import BarrierConfig, ConfirmationBarrier

sp_mod = importlib.import_module("services.orderflow.signal_pipeline")


# ---------------------------------------------------------------------------
# Stub harness
# ---------------------------------------------------------------------------

class _StubRuntime:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_ts_ms = 1_000


def _make_stub_pipeline(*, barrier_mode: str = "enforce", **cfg_kw):
    """Build a minimal object exposing the barrier helpers under test.

    We bind the four methods (`_barrier_submit`, `barrier_observe_tick`,
    `barrier_poll_and_publish`) onto a fresh stub instance so the heavy
    SignalPipeline.__init__ isn't invoked.
    """
    class _Stub:
        pass

    stub = _Stub()
    cfg = BarrierConfig(**{
        "timeout_ms": 1_000,
        "min_progress_bps": 5.0,
        "max_adverse_bps": 10.0,
        "min_observations": 1,
        **cfg_kw,
    })
    stub._barrier = ConfirmationBarrier(config=cfg, mode=barrier_mode)
    stub._BARRIER_RESOLVED_KEY = "_barrier_resolved"
    stub.published: list[tuple[str, dict]] = []

    async def _publish_signal(self, runtime, signal):
        # Mimic the wired barrier guard from real publish_signal.
        if not signal.get(self._BARRIER_RESOLVED_KEY):
            dec = self._barrier_submit(runtime, signal)
            if dec is None:
                return
        self.published.append((runtime.symbol, signal))

    stub._barrier_submit = sp_mod.SignalPipeline._barrier_submit.__get__(stub)
    stub.barrier_observe_tick = sp_mod.SignalPipeline.barrier_observe_tick.__get__(stub)
    stub.barrier_poll_and_publish = sp_mod.SignalPipeline.barrier_poll_and_publish.__get__(stub)
    stub.publish_signal = _publish_signal.__get__(stub)
    return stub


def _make_signal(*, sid="s1", side="LONG", entry=100.0, ts=1_000):
    return {
        "sid": sid,
        "side": side,
        "entry": entry,
        "tick_ts": ts,
        "symbol": "BTCUSDT",
        "indicators": {},
    }


# ---------------------------------------------------------------------------
# off mode — passthrough
# ---------------------------------------------------------------------------

def test_off_mode_publishes_inline():
    p = _make_stub_pipeline(barrier_mode="off")
    runtime = _StubRuntime("BTCUSDT")
    asyncio.run(p.publish_signal(runtime, _make_signal()))
    assert len(p.published) == 1
    assert len(p._barrier) == 0  # nothing queued


# ---------------------------------------------------------------------------
# enforce mode — defers, polls, republishes
# ---------------------------------------------------------------------------

def test_enforce_defers_then_publishes_on_progress():
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")

    asyncio.run(p.publish_signal(runtime, _make_signal(sid="s1", entry=100.0, ts=1_000)))
    assert p.published == []  # deferred
    assert len(p._barrier) == 1

    # Favourable tick
    p.barrier_observe_tick("BTCUSDT", 1_500, 100.10)  # +10 bp

    # Poll past deadline (1000+1000)
    resolver = lambda sym: runtime if sym == "BTCUSDT" else None
    processed = asyncio.run(p.barrier_poll_and_publish(2_001, resolver))
    assert processed == 1
    assert len(p.published) == 1
    sym, sig = p.published[0]
    assert sym == "BTCUSDT"
    # The republished signal carries the resolution marker.
    assert sig[p._BARRIER_RESOLVED_KEY] is True
    assert "confirmed_progress" in sig["indicators"]["barrier_resolution"]


def test_enforce_drops_when_no_progress():
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")

    asyncio.run(p.publish_signal(runtime, _make_signal(sid="s1", entry=100.0, ts=1_000)))
    # No observation → DROP path
    resolver = lambda sym: runtime
    asyncio.run(p.barrier_poll_and_publish(2_001, resolver))
    assert p.published == []
    assert len(p._barrier) == 0


def test_enforce_drops_on_early_flip():
    p = _make_stub_pipeline(barrier_mode="enforce", max_adverse_bps=10.0)
    runtime = _StubRuntime("BTCUSDT")

    asyncio.run(p.publish_signal(runtime, _make_signal(sid="s1", entry=100.0, ts=1_000)))
    p.barrier_observe_tick("BTCUSDT", 1_100, 99.80)  # -20 bp adverse → flip

    resolver = lambda sym: runtime
    asyncio.run(p.barrier_poll_and_publish(1_200, resolver))  # before deadline
    assert p.published == []


# ---------------------------------------------------------------------------
# Re-entry guard
# ---------------------------------------------------------------------------

def test_resolved_marker_bypasses_barrier():
    """Once barrier_poll_and_publish marks a signal _barrier_resolved, a
    second pass through publish_signal must NOT re-submit it."""
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")
    sig = _make_signal(sid="s1")
    sig["_barrier_resolved"] = True
    asyncio.run(p.publish_signal(runtime, sig))
    assert len(p.published) == 1
    assert len(p._barrier) == 0  # not queued


# ---------------------------------------------------------------------------
# Submit edge cases
# ---------------------------------------------------------------------------

def test_submit_missing_sid_fails_open():
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")
    sig = _make_signal(sid="")  # no sid
    asyncio.run(p.publish_signal(runtime, sig))
    assert len(p.published) == 1  # published inline


def test_submit_unknown_side_fails_open():
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")
    sig = _make_signal(side="???")
    asyncio.run(p.publish_signal(runtime, sig))
    # publish_signal in real code would reject unknown direction earlier;
    # the barrier submit path itself fails open and the stub publishes.
    assert len(p.published) == 1


# ---------------------------------------------------------------------------
# Shadow mode — publishes inline + logs telemetry
# ---------------------------------------------------------------------------

def test_shadow_mode_publishes_inline_and_records():
    p = _make_stub_pipeline(barrier_mode="shadow")
    runtime = _StubRuntime("BTCUSDT")
    asyncio.run(p.publish_signal(runtime, _make_signal()))
    # In shadow mode publish_signal proceeds inline AND queues for telemetry.
    assert len(p.published) == 1
    assert len(p._barrier) == 1  # tracked for shadow telemetry

    # Telemetry runs on poll: DROP path → SHADOW_DROP — no republish.
    resolver = lambda sym: runtime
    asyncio.run(p.barrier_poll_and_publish(2_001, resolver))
    assert len(p.published) == 1  # not republished
    assert len(p._barrier) == 0


# ---------------------------------------------------------------------------
# Multi-signal interleaving
# ---------------------------------------------------------------------------

def test_multiple_signals_resolve_independently():
    p = _make_stub_pipeline(barrier_mode="enforce")
    runtime = _StubRuntime("BTCUSDT")

    asyncio.run(p.publish_signal(runtime, _make_signal(sid="good", entry=100.0, ts=1_000)))
    asyncio.run(p.publish_signal(runtime, _make_signal(sid="bad", entry=200.0, ts=1_000)))
    assert p.published == []

    # Only "good" gets confirmation
    p.barrier_observe_tick("BTCUSDT", 1_500, 100.10)  # good: +10 bp
    p.barrier_observe_tick("BTCUSDT", 1_500, 200.00)  # bad: 0 bp

    resolver = lambda sym: runtime
    processed = asyncio.run(p.barrier_poll_and_publish(2_001, resolver))
    assert processed == 2
    published_sids = {sig["sid"] for _, sig in p.published}
    assert published_sids == {"good"}
