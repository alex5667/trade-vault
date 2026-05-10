from __future__ import annotations

"""
Integration tests for SignalDispatcher atomicity guarantees.

These tests require a real Redis instance and verify that Lua scripts maintain
atomicity: either both delivery AND marker succeed, or neither does.

Run with: pytest -m integration tests/test_signal_dispatcher_atomicity_integration.py
"""


import json
import time

import pytest

from services.dispatch.dispatcher_app import SignalDispatcher


@pytest.mark.integration
class TestDispatcherAtomicity:
    """
    Atomicity layer verification: no marker without delivery, no delivery without marker.

    Uses real Redis + real Lua scripts to verify transactional semantics.
    """

    def test_notify_atomic_delivery_marker(self, redis_client) -> None:
        """
        Verify that notify delivery is atomic: marker is set IFF stream entry exists.

        Uses real Lua script _LUA_NOTIFY_GATE_XADD_THEN_MARK.
        """
        # Setup dispatcher with real Redis
        d = SignalDispatcher()
        d.redis = redis_client
        d.lua_scripts.redis = redis_client  # Ensure scripts run on test client
        d.dual_redis = redis_client  # for notify
        d.simple_redis = redis_client

        sid = f"atomic_test_{int(time.time())}"
        target = "notify"
        stream = f"notify:test:{sid}"
        marker_key = d._marker_key(target, sid)
        d.notify_stream = stream

        # Ensure clean state
        redis_client.delete(stream, marker_key)

        # Prepare env with valid notify payload
        env = {
            "targets": {"notify": {"text": f"test message {sid}"}},
            "meta": {},
            "attempts": {},
            "trace_id": sid,
        }

        # Call real delivery
        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        # Verify atomicity: both marker AND stream entry exist
        marker_exists = bool(redis_client.exists(marker_key))
        stream_entries = redis_client.xlen(stream)

        assert marker_exists, "Marker must exist after successful delivery"
        assert stream_entries == 1, "Stream must have exactly one entry after delivery"

        # Verify stream content contains our payload
        entries = redis_client.xrange(stream, "-", "+")
        assert len(entries) == 1
        _, entry_data = entries[0]
        assert "text" in entry_data["data"] or "test message" in entry_data["data"]

    def test_signal_stream_atomic_delivery_marker(self, redis_client) -> None:
        """
        Verify that signal_stream delivery is atomic: marker is set IFF stream entry exists.

        Uses real Lua script _LUA_XADD_OR_SETEX_THEN_MARK.
        """
        d = SignalDispatcher()
        d.redis = redis_client
        d.lua_scripts.redis = redis_client
        d.dual_redis = redis_client
        d.simple_redis = redis_client  # for signal_stream

        sid = f"atomic_signal_{int(time.time())}"
        target = "signal_stream"
        stream = f"signals:test:{sid}"
        marker_key = d._marker_key(target, sid)
        d.signal_stream = stream

        # Clean state
        redis_client.delete(stream, marker_key)

        env = {
            "targets": {"signal_stream_payload": {"signal": "test", "price": 100.0}},
            "meta": {"signal_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        # Real delivery
        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        # Atomicity check
        marker_exists = bool(redis_client.exists(marker_key))
        stream_entries = redis_client.xlen(stream)

        assert marker_exists, "Marker must exist after successful delivery"
        assert stream_entries == 1, "Stream must have exactly one entry"

        # Verify payload in stream
        entries = redis_client.xrange(stream, "-", "+")
        assert len(entries) == 1
        _, entry_data = entries[0]
        entry_json = json.loads(entry_data["data"])
        assert entry_json["sid"] == sid
        assert "signal" in entry_json

    def test_audit_atomic_delivery_marker(self, redis_client) -> None:
        """
        Verify audit delivery atomicity.
        """
        d = SignalDispatcher()
        d.redis = redis_client  # for audit
        d.lua_scripts.redis = redis_client

        sid = f"atomic_audit_{int(time.time())}"
        target = "audit"
        stream = f"audit:test:{sid}"
        marker_key = d._marker_key(target, sid)
        d.audit_stream = stream

        redis_client.delete(stream, marker_key)

        env = {
            "targets": {"audit_payload": {"action": "trade", "symbol": "BTC"}},
            "meta": {"audit_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        marker_exists = bool(redis_client.exists(marker_key))
        stream_entries = redis_client.xlen(stream)

        assert marker_exists
        assert stream_entries == 1

    def test_manual_atomic_delivery_marker(self, redis_client) -> None:
        """
        Verify manual delivery atomicity.
        """
        d = SignalDispatcher()
        d.redis = redis_client
        d.lua_scripts.redis = redis_client
        d.dual_redis = redis_client  # for manual

        sid = f"atomic_manual_{int(time.time())}"
        target = "manual"
        stream = f"manual:test:{sid}"
        marker_key = d._marker_key(target, sid)
        d.manual_stream = stream

        redis_client.delete(stream, marker_key)

        env = {
            "targets": {"manual_payload": {"cmd": "execute", "params": {"amount": 100}}},
            "meta": {"manual_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        marker_exists = bool(redis_client.exists(marker_key))
        stream_entries = redis_client.xlen(stream)

        assert marker_exists
        assert stream_entries == 1

    def test_delivery_idempotency_no_duplicates(self, redis_client) -> None:
        """
        Verify that repeated delivery attempts don't create duplicates.
        Marker check should prevent re-delivery.
        """
        d = SignalDispatcher()
        d.redis = redis_client
        d.lua_scripts.redis = redis_client
        d.dual_redis = redis_client

        sid = f"idempotent_{int(time.time())}"
        target = "notify"
        stream = f"notify:idem:{sid}"
        d.notify_stream = stream

        # Clean state
        redis_client.delete(stream, d._marker_key(target, sid))

        env = {
            "targets": {"notify": {"text": f"idempotent test {sid}"}},
            "meta": {},
            "attempts": {},
            "trace_id": sid,
        }

        # First delivery
        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        initial_entries = redis_client.xlen(stream)
        assert initial_entries == 1

        # Attempt second delivery (should be skipped due to marker)
        d._deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        # Still exactly one entry (no duplicates)
        final_entries = redis_client.xlen(stream)
        assert final_entries == 1, "Idempotent delivery should not create duplicates"

    def test_missing_prerequisites_raise_permanent_error(self, redis_client) -> None:
        """
        Verify that missing prerequisites cause PermanentDeliveryError (no silent loss).
        """
        from services.dispatch.dispatcher_app import PermanentDeliveryError

        d = SignalDispatcher()
        d.redis = redis_client
        d.lua_scripts.redis = redis_client
        d.dual_redis = redis_client
        d.simple_redis = redis_client

        # Test missing notify payload
        env_missing_payload = {
            "targets": {"notify": None},  # missing payload
            "meta": {},
            "attempts": {},
            "trace_id": "test_missing_payload",
        }

        with pytest.raises(PermanentDeliveryError):
            d._deliver_one_target(
                env=env_missing_payload,
                sid="test_missing_payload",
                target="notify",
                targets_obj=env_missing_payload["targets"],
                meta=env_missing_payload["meta"],
                dual_client=d.dual_redis,
                simple_client=d.simple_redis,
            )

