
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from services.signal_dispatcher import SignalDispatcher
from tests.fake_redis import FakeRedis


class RetryDedupMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.r = FakeRedis()
        self.d = SignalDispatcher.__new__(SignalDispatcher)
        self.d.redis = self.r
        self.d._ctr = {}
        self.d.retry_zset = "z:retry"
        self.d.dlq_stream = "stream:dlq"
        self.d.dlq_notify = "stream:dlq:notify"
        self.d.dlq_signal_stream = "stream:dlq:signal_stream"
        self.d.dlq_audit = "stream:dlq:audit"
        self.d.dlq_manual = "stream:dlq:manual"
        self.d.dlq_snapshot = "stream:dlq:snapshot"
        self.d.max_attempts = 3

        # ensure required methods exist (real class methods)
        # _retry_delay_ms / _retry_dedup_key / _send_target_dlq / _schedule_target_retry should exist on class

        self.sids = ["s1", "s2", "s3"]
        self.targets = ["notify", "signal_stream", "audit", "manual"]

    @initialize()
    def init(self):
        # baseline
        assert self.r.zcard(self.d.retry_zset) == 0

    @rule(target_name=st.sampled_from(["notify", "signal_stream", "audit", "manual"]),
          sid=st.sampled_from(["s1", "s2", "s3"]),
          attempt=st.integers(min_value=0, max_value=5))
    def schedule(self, target_name, sid, attempt):
        target = target_name
        env = {"sid": sid, "trace_id": "t1", "targets": {}, "meta": {}}
        self.d._schedule_target_retry(target=target, sid=sid, env=env, attempt=int(attempt), last_error="x")

    @invariant()
    def retry_zset_not_exploding(self):
        # With dedup, repeated schedule calls for same (target,sid) should not grow unbounded.
        # This invariant is intentionally loose because FakeRedis TTL may not tick.
        assert self.r.zcard(self.d.retry_zset) <= 64


TestRetryDedupMachine = RetryDedupMachine.TestCase
settings.register_profile("ci", max_examples=50, stateful_step_count=100)
settings.load_profile("ci")
