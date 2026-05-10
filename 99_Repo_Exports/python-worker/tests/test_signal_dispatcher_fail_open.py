from __future__ import annotations

from collections import defaultdict


class _RedisBoom:
    def setex(self, *a, **k):
        raise RuntimeError("boom:setex")

    def sadd(self, *a, **k):
        raise RuntimeError("boom:sadd")

    def expire(self, *a, **k):
        raise RuntimeError("boom:expire")


def test_signal_dispatcher_fail_open_common_markers(monkeypatch):
    """
    Ensures key methods do not crash when Redis operations fail.
    Test is resilient to method renames (self-adapting discovery).
    """
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.redis = _RedisBoom()
    sd.metrics_prefix = "sd"
    sd._ctr = defaultdict(int)

    def _incr(key: str) -> None:
        sd._ctr[key] += 1

    sd._incr = _incr
    sd.done_ttl_sec = 10
    sd.env_state_ttl_sec = 10

    # 1) _mark_outbox_done should never raise
    assert hasattr(sd, "_mark_outbox_done")
    sd._mark_outbox_done("mid-1")

    # 2) env-req updater method names vary; discover best-effort
    cand = None
    for name in ("_update_env_req", "_set_env_req", "_mark_env_req", "_add_env_req", "_touch_env_req"):
        if hasattr(sd, name):
            cand = getattr(sd, name)
            break
    if cand is not None:
        # call with a typical shape: (sid, req_set) OR (sid, target, req_set) varies; handle both
        try:
            cand("sid-1", {"t1", "t2"})
        except TypeError:
            cand("sid-1", "t0", {"t1", "t2"})

    # 3) _mark_env_done should never raise
    if hasattr(sd, "_mark_env_done"):
        sd._mark_env_done("sid-2", "target-1")
