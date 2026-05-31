import pytest
from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, rule, invariant

from services.dispatch.dispatcher_app import SignalDispatcher
from utils.time_utils import get_ny_time_millis


class DoneInvariantMachine(RuleBasedStateMachine):
    def __init__(self, r):
        super().__init__()
        # Use __new__ to avoid side-effects in __init__
        self.d = SignalDispatcher.__new__(SignalDispatcher)
        self.r = r
        self.sid = "sid_hyp_done_1"
        self.env = {
            "sid": self.sid,
            "trace_id": "t1",
            "targets": {
                "notify": {"a": "1"},
                "signal_stream_payload": {"b": "2"},
                "audit_payload": {"c": "3"},
            },
            "meta": {
                "signal_stream": "stream:test:signal",
                "audit_stream": "stream:test:audit",
                "req_targets": ["notify", "signal_stream", "audit"],
            },
            "attempts": {},
        }
        self.delivered = set()
        
        # Initialize dispatcher manually
        self.d.redis = r
        self.d.simple_redis = r
        self.d.dual_redis = r
        self.d.delivery_marker_ttl_sec = 120
        self.d.marker_prefix = "marker"
        self.d.env_done_prefix = "done:env"
        
        # Mock evalsha_or_eval to actually SET the marker in redis
        def fake_eval(client, sha, tag, script, nkeys, *argv):
            if not argv:
                return "OK"
            marker_key = argv[0]
            ttl = int(argv[3]) if len(argv) > 3 else 120
            client.set(marker_key, str(get_ny_time_millis()), ex=ttl)
            return "OK"
        self.d._evalsha_or_eval = fake_eval
        
        # Mock other needed methods
        self.d._targets_list = lambda env: ["notify", "signal_stream", "audit"]
        self.d._env_done_key = lambda sid: f"done:env:{sid}"
        # SignalDispatcher proxies these to router, but we can also mock them here if needed
        # However, we want to test the REAL logic in deliver_targets_with_retry

    @initialize()
    def setup(self):
        # Clear redis for this sid
        self.r.delete(f"done:env:{self.sid}")
        for t in ["notify", "signal_stream", "audit"]:
            self.r.delete(f"marker:{t}:{self.sid}")

    def _done_key(self):
        return f"done:env:{self.sid}"

    def _marker_exists(self, t: str) -> bool:
        m_key = f"marker:{t}:{self.sid}"
        return bool(self.r.exists(m_key))

    @rule()
    def deliver_notify_only(self):
        self.d._deliver_targets_with_retry(self.env, self.sid, targets=["notify"])
        if self._marker_exists("notify"):
            self.delivered.add("notify")

    @rule()
    def deliver_signal_only(self):
        self.d._deliver_targets_with_retry(self.env, self.sid, targets=["signal_stream"])
        if self._marker_exists("signal_stream"):
            self.delivered.add("signal_stream")

    @rule()
    def deliver_audit_only(self):
        self.d._deliver_targets_with_retry(self.env, self.sid, targets=["audit"])
        if self._marker_exists("audit"):
            self.delivered.add("audit")

    @invariant()
    def invariant_done_only_when_all_markers_present(self):
        done = bool(self.r.exists(self._done_key()))
        all_markers = all(self._marker_exists(t) for t in ["notify", "signal_stream", "audit"])
        if done:
            assert all_markers, f"Env marked done but markers missing. Delivered set: {self.delivered}"


@pytest.mark.usefixtures("r")
def test_done_invariant_stateful(r):
    from hypothesis.stateful import run_state_machine_as_test
    # We define the machine inside so it's fresh and has access to 'r'
    class Machine(DoneInvariantMachine):
        def __init__(self):
            super().__init__(r)
            
    run_state_machine_as_test(Machine)
