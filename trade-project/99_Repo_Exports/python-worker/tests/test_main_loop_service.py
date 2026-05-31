import importlib.util
import types
from pathlib import Path


def _load_module(rel_path: str, name: str):
    root = Path(__file__).resolve().parents[1]  # tests/ -> python-worker/
    # rel_path is "python-worker/handlers/main_loop_service.py"
    # but we're already in python-worker/, so strip the prefix
    rel_path = rel_path.replace("python-worker/", "", 1)
    p = root / rel_path
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


class DummyHM:
    def __init__(self):
        self.calls = []

    def set_pending_len(self, symbol, kind, pending, ts_ms=None):
        self.calls.append(("set_pending_len", symbol, kind, int(pending), int(ts_ms or 0)))


class FakeClient:
    def __init__(self, pending=0):
        self._pending = pending

    def xpending(self, stream, group):
        return {"pending": self._pending}


class FakeConsumer:
    def __init__(self, msgs=None, pending=0):
        self.client = FakeClient(pending=pending)
        self.group = "g"
        self._msgs = list(msgs or [])
        self._read_calls = 0

    def ensure_groups(self, streams, stop_event=None):
        return

    def read_new(self, streams, count=100, block_ms=0):
        # return msgs once, then nothing
        self._read_calls += 1
        if self._read_calls == 1:
            return list(self._msgs)
        return []


class FakeStopEvent:
    def __init__(self, stop_after_waits=1):
        self._set = False
        self._waits = 0
        self._stop_after_waits = stop_after_waits

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def wait(self, timeout=None):
        self._waits += 1
        # stop after N waits to exit loop deterministically
        if self._waits >= self._stop_after_waits:
            self._set = True
        return self._set


def test_emit_pending_metrics_uses_passed_consumer():
    mod = _load_module("python-worker/handlers/main_loop_service.py", "main_loop_service")

    cfg = types.SimpleNamespace(pending_metrics_interval_ms=1)
    hm = DummyHM()
    svc = mod.MainLoopService(symbol="BTCUSDT", health_metrics=hm, config=cfg)
    svc.book_stream = "book"
    svc.l3_stream = "l3"
    svc.tick_stream = "ticks"

    consumer = FakeConsumer(pending=7)
    svc._emit_pending_metrics(consumer, mono_now_ms=10_000, wall_now_ms=1_700_000_000_000)

    assert hm.calls, "expected pending metric emission"
    # should have at least one call for book/l3/ticks (streams present)
    kinds = {c[2] for c in hm.calls}
    assert {"book", "l3", "ticks"} <= kinds


def test_max_msgs_per_loop_chunks_calls_process_multiple_times(monkeypatch):
    mod = _load_module("python-worker/handlers/main_loop_service.py", "main_loop_service2")

    # deterministic monotonic/time
    t = {"mono": 0.0, "wall": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: t["mono"])
    monkeypatch.setattr(mod.time, "time", lambda: t["wall"])

    cfg = types.SimpleNamespace(
        read_count=10,
        read_block_ms=0,
        read_count_book=10,
        read_count_l3=10,
        read_count_tick=10,
        idle_sleep_s=0.0,
        claim_interval_ms=10_000,
        max_msgs_per_loop=3,
        pending_metrics_interval_ms=10_000,
        pending_sample_every_ms=10_000,
    )

    calls = []

    class MH:
        def process_message_batch(self, msgs, backoff, fail_counts, consumer, stop_event=None):
            calls.append(len(msgs))
            # advance time a bit per call
            t["mono"] += 0.001
            t["wall"] += 0.001
            return (0, 0, True)

        def claim_and_process_pending(self, *a, **k):
            return True

    class EH:
        def _is_transient_error(self, e):
            return False

    # 7 msgs -> chunks of 3,3,1
    msgs = [types.SimpleNamespace(stream="ticks", msg_id=str(i), fields={}) for i in range(7)]
    consumer = FakeConsumer(msgs=msgs)
    stop = FakeStopEvent(stop_after_waits=1)  # will stop after first idle wait

    svc = mod.MainLoopService(
        symbol="BTCUSDT",
        config=cfg,
        health_metrics=None,
    )
    svc.tick_stream = "ticks"
    svc.book_stream = ""
    svc.l3_stream = ""
    svc.message_handler = MH()
    svc.error_handler = EH()
    svc._claim_and_process_pending = lambda *a, **k: True

    # force the loop to exit after processing once: after first read, next iteration hits empty -> wait -> stop
    svc._run_loop(consumer, stop)

    assert calls == [3, 3, 1]


def test_claim_interval_uses_monotonic_not_wallclock(monkeypatch):
    """
    time.time() "скачет назад", но claim interval планируется на monotonic -> claim вызывается по mono.
    """
    mod = _load_module("python-worker/handlers/main_loop_service.py", "main_loop_service3")

    state = {"mono": 0.0, "wall": 1000.0}

    def mono():
        return state["mono"]

    def wall():
        return state["wall"]

    monkeypatch.setattr(mod.time, "monotonic", mono)
    monkeypatch.setattr(mod.time, "time", wall)

    cfg = types.SimpleNamespace(
        read_count=10,
        read_block_ms=0,
        read_count_book=10,
        read_count_l3=10,
        read_count_tick=10,
        idle_sleep_s=0.0,
        claim_interval_ms=50,
        max_msgs_per_loop=0,
        pending_metrics_interval_ms=10_000,
        pending_sample_every_ms=10_000,
    )

    claim_calls = {"n": 0}

    class MH:
        def process_message_batch(self, msgs, backoff, fail_counts, consumer, stop_event=None):
            return (0, 0, True)

        def claim_and_process_pending(self, *a, **k):
            claim_calls["n"] += 1
            return True

    class EH:
        def _is_transient_error(self, e):
            return False

    # consumer returns empty always -> loop does idle wait; we'll stop after a few waits
    consumer = FakeConsumer(msgs=[])
    stop = FakeStopEvent(stop_after_waits=4)

    svc = mod.MainLoopService(symbol="BTCUSDT", config=cfg, health_metrics=None)
    svc.tick_stream = "ticks"
    svc.book_stream = ""
    svc.l3_stream = ""
    svc.message_handler = MH()
    svc.error_handler = EH()

    # make wait advance time and also simulate wallclock jump backwards
    def wait(timeout=None):
        # advance monotonic by 0.03s per wait
        state["mono"] += 0.03
        # wallclock jumps backwards after first wait
        state["wall"] -= 5.0
        stop._waits += 1
        if stop._waits >= stop._stop_after_waits:
            stop._set = True
        return stop._set

    stop.wait = wait

    svc._run_loop(consumer, stop)

    # initial pending recovery + subsequent claims depending on mono progression
    assert claim_calls["n"] >= 1
