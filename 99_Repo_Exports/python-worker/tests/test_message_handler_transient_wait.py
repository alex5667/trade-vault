import types
from pathlib import Path
import importlib.util


def _load_module(rel_path: str, name: str):
    root = Path(__file__).resolve().parents[1]  # tests/ -> python-worker/
    # rel_path is "python-worker/handlers/message_handler.py"
    # but we're already in python-worker/, so strip the prefix
    rel_path = rel_path.replace("python-worker/", "", 1)
    p = root / rel_path
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


class FakeStopEvent:
    def __init__(self):
        self.wait_calls = []

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        return False


class FakeBackoff:
    def next_sleep(self):
        return 0.123

    def reset(self):
        return


class FakeConsumer:
    def ack(self, *a, **k):
        raise AssertionError("ACK must NOT happen on transient error path")


def test_transient_error_uses_stop_event_wait(monkeypatch):
    mod = _load_module("python-worker/handlers/message_handler.py", "message_handler_mod")

    # ensure time.sleep is NOT called when stop_event provided
    import time as _time
    monkeypatch.setattr(mod, "time", _time)
    monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("time.sleep must not be called")))

    # Build a minimal self with required attributes/methods
    class Self:
        def __init__(self):
            import logging
            self.logger = logging.getLogger("t")
            self.symbol = "BTCUSDT"
            self.tick_stream = "ticks"
            self.book_stream = "book"
            self.l3_stream = "l3"
            self.max_fail_retries = 3
            self.config = None
            self.health_metrics = None

            self.data_parser = types.SimpleNamespace(
                _parse_tick=lambda _fields: (_ for _ in ()).throw(RuntimeError("boom"))
            )
            self.data_processor = types.SimpleNamespace(_process_tick=lambda _t: None, _process_book=lambda _b: None)

        def _priority(self, stream):
            return 0

        def _is_transient_error(self, e):
            return True

        def _try_add_dlq_or_backoff(self, *a, **k):
            return True

    s = Self()
    stop = FakeStopEvent()
    backoff = FakeBackoff()
    fail_counts = {}
    consumer = FakeConsumer()

    m = types.SimpleNamespace(stream="ticks", msg_id="1-0", fields={"x": "y"})
    tick_cnt, book_cnt, all_success = mod.process_message_batch(s, [m], backoff, fail_counts, consumer, stop_event=stop)

    assert all_success is False
    assert stop.wait_calls, "expected stop_event.wait to be used"
    assert abs(stop.wait_calls[0] - 0.123) < 1e-9
