from __future__ import annotations

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from services.dispatch.dispatcher_app import SignalDispatcher


def _mk_dispatcher_for_state(monkeypatch):
    d = SignalDispatcher.__new__(SignalDispatcher)

    d.marker_prefix = "marker"
    d.delivery_marker_ttl_sec = 60

    d.notify_stream = "notify:stream"
    d.notify_signal_counter_key = "notify:counter"
    d.marker_gc_zset = "marker:gc"
    d.notify_signal_every_n = 5
    d._sha_dual = "sha_dual"
    d._sha_main = "sha_main"
    d.redis = object()

    d._adapt_notify_payload = lambda **kwargs: None
    d._flatten_notify_fields = lambda payload: ["k1", "v1"]

    # Mock trace functions on the dispatcher instance
    d.ensure_env_trace = lambda *a, **k: None
    d.append_env_trace_event = lambda *a, **k: None

    calls = []

    def _evalsha_or_eval(client, sha, op_name, lua_src, numkeys, *args):
        calls.append({"client": client, "op_name": op_name, "numkeys": int(numkeys), "args": list(args)})

    d._evalsha_or_eval = _evalsha_or_eval
    return d, calls


class DeliverOneTargetSM(RuleBasedStateMachine):
    def __init__(self, monkeypatch):
        super().__init__()
        self.d, self.calls = _mk_dispatcher_for_state(monkeypatch)

    @rule(
        target_name=st.sampled_from(["notify", "signal_stream", "audit", "manual"]),
        has_payload=st.booleans(),
        has_stream=st.booleans(),
        has_dual=st.booleans(),
        has_simple=st.booleans(),
    )
    def deliver(self, target_name, has_payload, has_stream, has_dual, has_simple):
        target = target_name
        self.calls.clear()

        sid = "sid_sm"
        env = {}
        meta = {}
        targets_obj = {}

        dual_client = object() if has_dual else None
        simple_client = object() if has_simple else None

        if target == "notify":
            targets_obj["notify"] = {"text": "hi"} if has_payload else None
            # stream/meta не нужен
        elif target == "signal_stream":
            if has_stream:
                meta["signal_stream"] = "sig:stream"
            targets_obj["signal_stream_payload"] = {"x": 1} if has_payload else None
        elif target == "audit":
            if has_stream:
                meta["audit_stream"] = "audit:stream"
            targets_obj["audit_payload"] = {"x": 1} if has_payload else None
        elif target == "manual":
            if has_stream:
                meta["manual_stream"] = "manual:stream"
            targets_obj["manual_payload"] = {"x": 1} if has_payload else None

        should_succeed = False
        if target == "notify":
            should_succeed = bool(has_payload and has_dual)
        elif target == "signal_stream":
            should_succeed = bool(has_stream and has_payload and has_simple)
        elif target == "audit":
            should_succeed = bool(has_stream and has_payload)  # self.redis уже задан
        elif target == "manual":
            should_succeed = bool(has_stream and has_payload and has_dual)

        if should_succeed:
            self.d._deliver_one_target(
                env=env,
                sid=sid,
                target=target,
                targets_obj=targets_obj,
                meta=meta,
                dual_client=dual_client,
                simple_client=simple_client,
            )
            assert env.get("trace_id") == sid
            assert len(self.calls) == 1
        else:
            try:
                self.d._deliver_one_target(
                    env=env,
                    sid=sid,
                    target=target,
                    targets_obj=targets_obj,
                    meta=meta,
                    dual_client=dual_client,
                    simple_client=simple_client,
                )
            except Exception:
                pass
            else:
                raise AssertionError("Expected exception for missing prerequisites, but call succeeded")
            assert len(self.calls) == 0

    @invariant()
    def marker_key_format_is_stable(self):
        d = self.d
        assert d._delivery_key("notify", "X") == f"{d.marker_prefix}:notify:X"
        assert d._delivery_key("signal_stream", "X") == f"{d.marker_prefix}:signal_stream:X"


def test_state_machine(monkeypatch):
    from hypothesis.stateful import run_state_machine_as_test
    run_state_machine_as_test(lambda: DeliverOneTargetSM(monkeypatch))
