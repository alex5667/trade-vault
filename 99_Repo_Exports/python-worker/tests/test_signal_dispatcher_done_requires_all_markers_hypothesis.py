
import pytest
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, initialize, rule

from services.dispatch.dispatcher_app import SignalDispatcher
from utils.time_utils import get_ny_time_millis


class DoneInvariantMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.d = SignalDispatcher()
        # test will inject redis later via initialize(r=...)
        self.r = None
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
                # required targets MUST persist across per-target retries
                "req_targets": ["notify", "signal_stream", "audit"],
            },
            "attempts": {},
        }
        self.delivered = set()

    @initialize()
    def init_redis(self, r=None):
        # pytest injects fixture through wrapper below
        self.r = r
        self.d.redis = r
        self.d.simple_redis = r
        self.d.dual_redis = r
        self.d.delivery_marker_ttl_sec = 120

        def fake_eval(client, sha, tag, script, nkeys, *argv):
            marker_key = argv[0]
            ttl = int(argv[3]) if len(argv) > 3 else 120
            client.set(marker_key, str(get_ny_time_millis()), ex=ttl)
            return "OK"

        self.d._evalsha_or_eval = fake_eval

    def _done_key(self):
        return self.d._env_done_key(self.sid)

    def _marker_exists(self, t: str) -> bool:
        cli = self.r
        return bool(cli.exists(self.d._delivery_key(t, self.sid)))

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

    @rule()
    def invariant_done_only_when_all_markers_present(self):
        done = bool(self.r.exists(self._done_key()))
        all_markers = all(self._marker_exists(t) for t in ["notify", "signal_stream", "audit"])
        assert (not done) or all_markers


@pytest.mark.usefixtures("r")
def test_done_invariant_stateful(r):
    # wrap machine so it receives fixture r
    class M(DoneInvariantMachine):
        @initialize()
        def init_redis(self):
            super().init_redis(r=r)

    settings(max_examples=30, stateful_step_count=20, deadline=None)(M.TestCase)()
