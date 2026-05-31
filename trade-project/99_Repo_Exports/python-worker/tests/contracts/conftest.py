import types

import pytest

from services.dispatch.dispatcher_app import SignalDispatcher
from utils.time_utils import get_ny_time_millis


@pytest.fixture()
def dispatcher(r, monkeypatch):
    """Shared dispatcher fixture for contract tests that don't define their own."""
    d = SignalDispatcher()
    d.redis = r
    d.simple_redis = r
    d.dual_redis = r

    d.marker_prefix = "signal:delivery:marker"
    d.done_prefix = "signal:done"
    d.marker_gc_zset = "signal:delivery:gc"
    d.notify_stream = "stream:signals:notify"
    d.notify_signal_counter_key = "signal:notify:ctr"
    d.delivery_marker_ttl_sec = 120

    calls = {"deliver": 0}

    def fake_eval(client, sha, tag, script, nkeys, *argv):
        calls["deliver"] += 1
        marker_key = argv[0]
        client.set(marker_key, str(get_ny_time_millis()), ex=int(d.delivery_marker_ttl_sec))
        return "OK"

    monkeypatch.setattr(d, "_evalsha_or_eval", fake_eval, raising=True)

    if not hasattr(d, "_flatten_notify_fields"):
        monkeypatch.setattr(d, "_flatten_notify_fields", lambda payload: ["sid", (payload.get("sid", ""))], raising=False)

    d._scheduled = []
    d._dlq = []

    monkeypatch.setattr(d, "_schedule_target_retry",
                        lambda target, sid, env, attempt, last_error: d._scheduled.append((target, sid, attempt, last_error)),
                        raising=False)
    monkeypatch.setattr(d, "_send_target_dlq",
                        lambda t, sid, env, reason, err: d._dlq.append((t, sid, reason, err)),
                        raising=False)

    monkeypatch.setattr("services.dispatch.dispatcher_app.is_transient_error", lambda e: False, raising=False)

    return d
