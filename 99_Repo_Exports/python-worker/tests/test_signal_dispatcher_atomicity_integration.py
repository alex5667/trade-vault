from __future__ import annotations

"""
Integration tests for SignalDispatcher atomicity guarantees (Async).

These tests require a real Redis instance and verify that Lua scripts maintain
atomicity: either both delivery AND marker succeed, or neither does.

Run with: pytest -m integration tests/test_signal_dispatcher_atomicity_integration.py
"""


import json
import time
import asyncio
import pytest

from services.dispatch.dispatcher_app import SignalDispatcher


@pytest.mark.integration
@pytest.mark.asyncio
class TestDispatcherAtomicity:
    """
    Atomicity layer verification: no marker without delivery, no delivery without marker.

    Uses real Redis + real Lua scripts to verify transactional semantics.
    """

    def _override_redis_for_test(self, d: SignalDispatcher, async_redis_client: Any):
        d.redis = async_redis_client
        d.lua_scripts.redis = async_redis_client
        d.dual_redis = async_redis_client
        d.simple_redis = async_redis_client
        if d.idempotency_store:
            d.idempotency_store.redis = async_redis_client
            d.idempotency_store.lua_scripts.redis = async_redis_client
        if d.lease_manager:
            d.lease_manager.redis = async_redis_client
        if d.retry_scheduler:
            d.retry_scheduler.redis = async_redis_client
        if d.target_router:
            d.target_router.redis_client = async_redis_client
            d.target_router.dual_client = async_redis_client
            d.target_router.simple_client = async_redis_client
        if d.marker_repair:
            d.marker_repair.redis = async_redis_client
        if d.dlq_writer:
            d.dlq_writer.redis = async_redis_client
        if d.dispatch_metrics:
            d.dispatch_metrics.redis = async_redis_client

    async def test_notify_atomic_delivery_marker(self, async_redis_client) -> None:
        """
        Verify that notify delivery is atomic: marker is set IFF stream entry exists.
        """
        # Setup dispatcher with real Redis
        d = SignalDispatcher()
        await d.initialize()
        self._override_redis_for_test(d, async_redis_client)

        sid = f"atomic_test_{int(time.time())}"
        target = "notify"
        stream = f"notify:test:{sid}"
        marker_key = f"{d.config.marker_prefix}:{target}:{sid}"
        d.config.notify_stream = stream

        # Ensure clean state
        await async_redis_client.delete(stream, marker_key)

        # Prepare env with valid notify payload
        env = {
            "targets": {"notify": {"text": f"test message {sid}"}},
            "meta": {},
            "attempts": {},
            "trace_id": sid,
        }

        # Call real delivery
        await d.target_router.deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        # Verify atomicity: both marker AND stream entry exist
        marker_exists = bool(await async_redis_client.exists(marker_key))
        stream_entries = await async_redis_client.xlen(stream)

        assert marker_exists, "Marker must exist after successful delivery"
        assert stream_entries == 1, "Stream must have exactly one entry after delivery"

        # Verify stream content contains our payload
        entries = await async_redis_client.xrange(stream, "-", "+")
        assert len(entries) == 1
        _, entry_data = entries[0]
        assert "text" in entry_data["data"] or "test message" in entry_data["data"]

    async def test_signal_stream_atomic_delivery_marker(self, async_redis_client) -> None:
        """
        Verify that signal_stream delivery is atomic.
        """
        d = SignalDispatcher()
        await d.initialize()
        self._override_redis_for_test(d, async_redis_client)

        sid = f"atomic_signal_{int(time.time())}"
        target = "signal_stream"
        stream = f"signals:test:{sid}"
        marker_key = f"{d.config.marker_prefix}:{target}:{sid}"
        d.config.signal_stream = stream

        # Clean state
        await async_redis_client.delete(stream, marker_key)

        env = {
            "targets": {"signal_stream_payload": {"signal": "test", "price": 100.0}},
            "meta": {"signal_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        # Real delivery
        await d.target_router.deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        # Atomicity check
        marker_exists = bool(await async_redis_client.exists(marker_key))
        stream_entries = await async_redis_client.xlen(stream)

        assert marker_exists, "Marker must exist after successful delivery"
        assert stream_entries == 1, "Stream must have exactly one entry"

        # Verify payload in stream
        entries = await async_redis_client.xrange(stream, "-", "+")
        assert len(entries) == 1
        _, entry_data = entries[0]
        entry_json = json.loads(entry_data["data"])
        assert entry_json["sid"] == sid
        assert "signal" in entry_json

    async def test_audit_atomic_delivery_marker(self, async_redis_client) -> None:
        """
        Verify audit delivery atomicity.
        """
        d = SignalDispatcher()
        await d.initialize()
        self._override_redis_for_test(d, async_redis_client)

        sid = f"atomic_audit_{int(time.time())}"
        target = "audit"
        stream = f"audit:test:{sid}"
        marker_key = f"{d.config.marker_prefix}:{target}:{sid}"
        d.config.audit_stream = stream

        await async_redis_client.delete(stream, marker_key)

        env = {
            "targets": {"audit_payload": {"action": "trade", "symbol": "BTC"}},
            "meta": {"audit_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        await d.target_router.deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        marker_exists = bool(await async_redis_client.exists(marker_key))
        stream_entries = await async_redis_client.xlen(stream)

        assert marker_exists
        assert stream_entries == 1

    async def test_manual_atomic_delivery_marker(self, async_redis_client) -> None:
        """
        Verify manual delivery atomicity.
        """
        d = SignalDispatcher()
        await d.initialize()
        self._override_redis_for_test(d, async_redis_client)

        sid = f"atomic_manual_{int(time.time())}"
        target = "manual"
        stream = f"manual:test:{sid}"
        marker_key = f"{d.config.marker_prefix}:{target}:{sid}"
        d.config.manual_stream = stream

        await async_redis_client.delete(stream, marker_key)

        env = {
            "targets": {"manual_payload": {"cmd": "execute", "params": {"amount": 100}}},
            "meta": {"manual_stream": stream},
            "attempts": {},
            "trace_id": sid,
        }

        await d.target_router.deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        marker_exists = bool(await async_redis_client.exists(marker_key))
        stream_entries = await async_redis_client.xlen(stream)

        assert marker_exists
        assert stream_entries == 1

    async def test_delivery_idempotency_no_duplicates(self, async_redis_client) -> None:
        """
        Verify that repeated delivery attempts don't create duplicates.
        """
        d = SignalDispatcher()
        await d.initialize()
        self._override_redis_for_test(d, async_redis_client)

        sid = f"idempotent_{int(time.time())}"
        target = "notify"
        stream = f"notify:idem:{sid}"
        d.config.notify_stream = stream

        # Clean state
        marker_key = f"{d.config.marker_prefix}:{target}:{sid}"
        await async_redis_client.delete(stream, marker_key)

        env = {
            "targets": {"notify": {"text": f"idempotent test {sid}"}},
            "meta": {},
            "attempts": {},
            "trace_id": sid,
        }

        # First delivery
        await d.target_router.deliver_one_target(
            env=env,
            sid=sid,
            target=target,
            targets_obj=env["targets"],
            meta=env["meta"],
            dual_client=d.dual_redis,
            simple_client=d.simple_redis,
        )

        initial_entries = await async_redis_client.xlen(stream)
        assert initial_entries == 1

        # Attempt second delivery (should be skipped due to marker)
        # deliver_targets_with_retry checks marker
        await d.target_router.deliver_targets_with_retry(
            env=env,
            sid=sid,
            targets=[target],
        )

        # Still exactly one entry (no duplicates)
        final_entries = await async_redis_client.xlen(stream)
        assert final_entries == 1, "Idempotent delivery should not create duplicates"
