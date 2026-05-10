from __future__ import annotations

import pytest

from services.dispatch.dispatcher_app import SignalDispatcher


def test_notify_missing_payload_is_not_silent():
    d = SignalDispatcher.__new__(SignalDispatcher)
    d.marker_prefix = "marker"
    d.delivery_marker_ttl_sec = 60

    # Mock trace functions on the dispatcher instance
    d.ensure_env_trace = lambda *a, **k: None
    d.append_env_trace_event = lambda *a, **k: None

    env = {}
    sid = "sid_silent"
    targets_obj = {"notify": None}
    meta = {}

    with pytest.raises(Exception) as e:
        d._deliver_one_target(
            env=env,
            sid=sid,
            target="notify",
            targets_obj=targets_obj,
            meta=meta,
            dual_client=object(),
            simple_client=object(),
        )

    assert "notify missing targets.notify payload" in str(e.value)
