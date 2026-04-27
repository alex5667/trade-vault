from __future__ import annotations


class _RedisBoom:
    def set(self, *a, **k):
        raise RuntimeError("boom:set")

    def setex(self, *a, **k):
        raise RuntimeError("boom:setex")

    def sadd(self, *a, **k):
        raise RuntimeError("boom:sadd")

    def expire(self, *a, **k):
        raise RuntimeError("boom:expire")


def test_done_marker_set_fail_open_increments_metric():
    from services.signal_dispatcher import SignalDispatcher

    sd = SignalDispatcher.__new__(SignalDispatcher)  # bypass heavy __init__
    sd.redis = _RedisBoom()
    sd.logger = None
    sd.metrics_prefix = "test"
    sd.delivery_marker_ttl_sec = 10
    sd.env_state_ttl_sec = 10
    sd.done_ttl_sec = 10
    sd.outbox_stream = "test:stream"
    sd.group = "test:group"

    seen = []

    def _incr(key: str) -> None:
        seen.append(key)

    sd._incr = _incr

    # The done marker set block is inside _handle_one method.
    # We need to set up minimal state and call the method that triggers the done marker path.
    # Since _handle_one is complex, we'll mock the necessary dependencies.

    # Mock required methods/keys
    def _done_key(sid):
        return f"done:{sid}"

    def _sid_lease_key(sid):
        return f"sid_lease:{sid}"

    def _try_acquire_sid_lease(sid):
        return "token123"  # success

    def _deliver_all(env, sid=None, lease_token=None, last_extend_ms_ref=None):
        pass  # success

    sd._done_key = _done_key
    sd._sid_lease_key = _sid_lease_key
    sd._try_acquire_sid_lease = _try_acquire_sid_lease
    sd._deliver_all = _deliver_all

    # Create minimal env
    env = {"sid": "sid1", "targets": {}}

    # Call _handle_one which will hit the done marker set block
    try:
        result = sd._handle_one("msg123", {"data": '{"sid": "sid1", "targets": {}}'})
    except Exception:
        # Expected due to Redis boom, but we check if metric was incremented
        pass

    assert "test:done_marker_set_errors_total" in seen
