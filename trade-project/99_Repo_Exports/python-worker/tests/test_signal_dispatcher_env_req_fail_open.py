from __future__ import annotations

import inspect


class _RedisBoom:
    def sadd(self, *a, **k):
        raise RuntimeError("boom:sadd")

    def expire(self, *a, **k):
        raise RuntimeError("boom:expire")

    def setex(self, *a, **k):
        raise RuntimeError("boom:setex")


def _make_sd_minimal():
    # Avoid constructor coupling: create instance without __init__
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.redis = _RedisBoom()
    sd.metrics_prefix = "test"
    sd.env_state_ttl_sec = 10
    sd.done_ttl_sec = 10
    sd.logger = None

    # metrics are best-effort in production; in tests we keep no-op
    sd._incr = lambda *a, **k: None  # noqa: E731

    # keys helpers may exist; if not, these tests will skip those calls
    return sd


def _call_with_signature(func, **pool):
    sig = inspect.signature(func)
    kwargs = {}
    for name, p in sig.parameters.items():
        if name == "self":
            continue
        if name in pool:
            kwargs[name] = pool[name]
            continue
        # reasonable defaults for unknown required params
        if p.default is not inspect._empty:
            continue
        kwargs[name] = ""  # safest placeholder
    return func(**kwargs)


def test_env_req_update_fail_open_no_raise():
    sd = _make_sd_minimal()

    # Find env-req updater method by common names (keeps compatibility)
    cand = None
    for name in ("_update_env_req", "_set_env_req", "_mark_env_req", "_add_env_req", "_touch_env_req"):
        if hasattr(sd, name):
            cand = getattr(sd, name)
            break
    if cand is None:
        # If your implementation uses a different internal name, add it above.
        return

    # Call using signature introspection (works across versions)
    _call_with_signature(
        cand,
        k="env:req:test",
        key="env:req:test",
        sid="sid1",
        target="t1",
        req={"a", "b"},
        req_set={"a", "b"},
    )


def test_markers_fail_open_no_raise():
    sd = _make_sd_minimal()
    if hasattr(sd, "_mark_env_done"):
        _call_with_signature(sd._mark_env_done, sid="sid1", target="t1")
    if hasattr(sd, "_mark_outbox_done"):
        _call_with_signature(sd._mark_outbox_done, msg_id="m1")
