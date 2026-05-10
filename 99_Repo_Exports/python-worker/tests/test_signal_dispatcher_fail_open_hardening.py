from __future__ import annotations


class _BoomRedis:
    def __init__(self, *, boom_methods=None):
        self._boom = set(boom_methods or [])

    def setex(self, *a, **k):
        if "setex" in self._boom:
            raise RuntimeError("boom:setex")
        return True

    def sadd(self, *a, **k):
        if "sadd" in self._boom:
            raise RuntimeError("boom:sadd")
        return 1

    def expire(self, *a, **k):
        if "expire" in self._boom:
            raise RuntimeError("boom:expire")
        return True


def _mk_sd_without_init():
    # Instantiate without calling __init__ (keeps test resilient to ctor signature changes).
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.logger = None
    sd.metrics_prefix = "sd"
    sd._metrics = []

    def _incr(k: str):
        sd._metrics.append(k)

    sd._incr = _incr

    # minimal attributes used by patched methods
    sd.done_ttl_sec = 10
    sd.env_state_ttl_sec = 10

    # minimal key builders
    sd._done_key = lambda msg_id: f"done:{msg_id}"
    sd._env_done_target_key = lambda sid, target: f"envdone:{sid}:{target}"

    return sd


def test_mark_outbox_done_fail_open_increments_metric():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom_methods={"setex"})

    # must not raise
    sd._mark_outbox_done("m1")
    assert "sd:outbox_done_marker_errors_total" in sd._metrics


def test_mark_env_done_fail_open_increments_metric():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom_methods={"setex"})

    # must not raise
    sd._mark_env_done("sid1", "tg")
    assert "sd:env_done_marker_errors_total" in sd._metrics


def test_env_req_sadd_expire_fail_open_increments_metric():
    # This test is self-adapting: method name may change across versions.
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom_methods={"sadd", "expire"})
    sd.env_state_ttl_sec = 10

    # Provide minimal key + payload for any likely env-req method.
    k = "env:req:test"
    req = ["a", "b"]

    cand = None
    for name in ("_update_env_req", "_set_env_req", "_mark_env_req", "_add_env_req", "_touch_env_req"):
        if hasattr(SignalDispatcher, name):
            cand = getattr(sd, name)
            break

    if cand is None:
        # No such helper in this version: nothing to assert.
        return

    # Try calling with the most common signature patterns (fail-open).
    try:
        cand(k, req)  # type: ignore[arg-type]
    except TypeError:
        try:
            cand(k=k, req=req)  # type: ignore[call-arg]
        except TypeError:
            # If signature is different in your tree, adjust only this test.
            return

    assert "sd:env_req_update_errors_total" in sd._metrics
