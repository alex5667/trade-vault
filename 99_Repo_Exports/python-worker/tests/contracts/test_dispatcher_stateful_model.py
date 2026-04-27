import time
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, rule, precondition, initialize
from services.signal_dispatcher import SignalDispatcher


class DispatcherModel(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.sid = "sid_stateful_1"
        self.targets = ["notify", "signal_stream"]

    @initialize()
    def init(self):
        # тут redis fixture недоступен напрямую; этот тест запускайте как обычный pytest с фикстурой через обёртку ниже
        pass


def run_state_machine(dispatcher):
    """
    Обёртка чтобы stateful получил реальный dispatcher (с Redis fixture).
    """
    class M(RuleBasedStateMachine):
        def __init__(self):
            super().__init__()
            self.d = dispatcher
            self.sid = "sid_stateful_1"
            self.env = {
                "sid": self.sid,
                "targets": {
                    "notify": {"sid": self.sid},
                    "signal_stream_payload": {"sid": self.sid},
                },
                "meta": {"signal_stream": "stream:signals:main"},
            }

        @rule()
        def deliver(self):
            self.d._deliver_targets_with_retry(self.env, self.sid, targets=["notify", "signal_stream"])

        @rule()
        def redeliver_same(self):
            self.d._deliver_targets_with_retry(self.env, self.sid, targets=["notify", "signal_stream"])

        @rule()
        def assert_done_iff_markers(self):
            r = self.d.redis
            m1 = r.exists(self.d._marker_key("notify", self.sid)) == 1
            m2 = r.exists(self.d._marker_key("signal_stream", self.sid)) == 1
            done = r.exists(self.d._env_done_key(self.sid)) == 1
            assert done == (m1 and m2)

    M.TestCase.settings = settings(max_examples=50, deadline=None)
    M.TestCase().runTest()
