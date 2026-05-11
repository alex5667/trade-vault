import json
import time
import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import redis.asyncio as redis

from common.decision_trace import DecisionTrace
from common.log import setup_logger
from common.transient import is_transient_error
from core.redis_client import get_async_redis_client
from core.redis_stream_consumer import AsyncRedisStreamHelper

from services.dispatcher.config import SignalDispatcherConfig
from services.dispatcher.error_handler import ErrorHandler
from services.dispatcher.lua_scripts import LuaScriptManager
from services.dispatcher.trace_writer import TraceWriter
from services.orderflow.of_inputs_v3_circuit import record_downgrade_and_maybe_trip, call_with_timeout, refresh_disabled_state
from services.orderflow.metrics import of_inputs_v3_circuit_disabled, of_inputs_v3_circuit_disabled_until_ms

from services.dispatch.envelope_parser import EnvelopeParser
from services.dispatch.target_router import TargetRouter, PermanentDeliveryError
from services.dispatch.idempotency_store import IdempotencyStore
from services.dispatch.retry_scheduler import RetryScheduler
from services.dispatch.dlq_writer import DlqWriter
from services.dispatch.lease_manager import LeaseManager
from services.dispatch.marker_repair import MarkerRepair
from services.dispatch.dispatch_metrics import DispatchMetrics

logger = setup_logger("SignalDispatcher")


class TransientError(RuntimeError): pass


@dataclass(frozen=True)
class PendingMsg:
    msg_id: str
    fields: dict[str, Any]


@dataclass(frozen=True)
class DispatchDecision:
    ack_now: bool
    reason: str = ""


async def _try_restore_pending_cursor(redis_client: Any, key: str) -> str | None:
    if redis_client is None:
        return None
    try:
        v = await redis_client.get(key)
        if isinstance(v, bytes):
            return v.decode("utf-8", "ignore")
        if isinstance(v, str):
            return v
        return None
    except Exception:
        return None


class SignalDispatcher:
    def __init__(self, loop=None):
        self.logger = logger
        self.loop = loop or asyncio.get_event_loop()
        
        self.redis = None
        self.dual_redis = None
        self.simple_redis = None
        
        self.config = SignalDispatcherConfig.from_env()
        self.ctr: defaultdict[str, int] = defaultdict(int)
        
        self.lua_scripts = None
        self.trace_writer = None
        self.error_handler = None
        self.dlq_writer = None
        self.envelope_parser = None
        self.idempotency_store = None
        self.lease_manager = None
        self.target_router = None
        self.retry_scheduler = None
        self.marker_repair = None
        self.dispatch_metrics = None
        
        self.of_inputs_v3_cb_states: dict[str, Any] = {}
        self._pending_start_id = "0-0"
        self._last_claim_mono = 0.0
        self._pending_claimed = 0
        self._lease_contention = 0
        self._ack_retry: dict[tuple[str, str], float] = {}
        self._last_ack_cleanup_mono = time.monotonic()
        self._last_consumer_cleanup = 0.0

    async def initialize(self):
        try:
            self.redis = await get_async_redis_client(
                url=self.config.redis_url,
                max_connections=self.config.redis_max_connections,
                socket_timeout=self.config.redis_socket_timeout,
                socket_connect_timeout=self.config.redis_socket_connect_timeout
            )
            self.simple_redis = self.redis
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis: {e}")
            raise

        self.lua_scripts = LuaScriptManager(self.redis, logger=self.logger)
        await self.lua_scripts.preload_all()

        self.trace_writer = TraceWriter(self.redis, self.config, self.logger)
        self.error_handler = ErrorHandler(self.logger, self.ctr)

        self.dlq_writer = DlqWriter(self.config, self.redis, self.logger)
        self.envelope_parser = EnvelopeParser(self.redis, self.config.dlq_stream, self.logger)
        self.idempotency_store = IdempotencyStore(self.config, self.redis, self.lua_scripts, self.logger, self.ctr)
        self.lease_manager = LeaseManager(self.config, self.redis, self.lua_scripts, self.ctr)
        
        self.target_router = TargetRouter(
            config=self.config,
            redis_client=self.redis,
            dual_client=self.dual_redis,
            simple_client=self.simple_redis,
            idempotency_store=self.idempotency_store,
            retry_scheduler=None,
            dlq_writer=self.dlq_writer,
            logger=self.logger
        )
        self.retry_scheduler = RetryScheduler(
            config=self.config, 
            redis_client=self.redis, 
            lua_scripts=self.lua_scripts, 
            dlq_writer=self.dlq_writer,
            target_router=self.target_router,
            ctr=self.ctr
        )
        self.target_router.set_retry_scheduler(self.retry_scheduler)
        
        self.marker_repair = MarkerRepair(self.config, self.redis, self.logger, self.ctr)
        self.dispatch_metrics = DispatchMetrics(self.config, self.redis, self.logger, self.ctr)

    async def _ack_fail_open(self, helper: AsyncRedisStreamHelper, stream: str, msg_id: str, *, ctr_ok: str, ctr_fail: str, where: str) -> bool:
        try:
            await helper.ack(stream, msg_id)
            self.ctr[ctr_ok] += 1
            return True
        except Exception as exc:
            self.ctr[ctr_fail] += 1
            if is_transient_error(exc):
                self._remember_ack_retry(stream, msg_id)
                self.logger.warning("Transient ACK failed where=%s msg=%s err=%r (will retry ack)", where, msg_id, exc)
            else:
                self.logger.warning("ACK failed where=%s msg=%s err=%r", where, msg_id, exc)
            return False

    async def _finalize_ack(self, helper: AsyncRedisStreamHelper, stream: str, msg_id: str, *, ctr_ok: str, ctr_fail: str, where: str) -> bool:
        return await self._ack_fail_open(helper, stream, msg_id, ctr_ok=ctr_ok, ctr_fail=ctr_fail, where=where)

    async def _handle_env(self, *, msg_id: str, env: dict[str, Any], sid: str) -> bool:
        lease = await self.lease_manager.try_acquire_sid_lease(sid)  # type: ignore
        if not lease:  # type: ignore
            self._lease_contention += 1
            try:
                await self.redis.xadd(  # type: ignore
                    self.config.outbox_stream,  # type: ignore
                    {"data": json.dumps(env, ensure_ascii=False)},
                    maxlen=20000,
                    approximate=True,
                )
                return True
            except Exception:
                return False

        dtrace = DecisionTrace.from_env(env)
        try:
            mt5_payload = env.pop("__mt5_payload__", None)
            if mt5_payload is not None and isinstance(mt5_payload, dict):
                if "targets" not in env or not isinstance(env["targets"], dict):
                    env["targets"] = {}
                env["targets"]["mt5_plan"] = mt5_payload

            await self.target_router.deliver_targets_with_retry(env, sid, _trace=dtrace)  # type: ignore
  # type: ignore
            meta = env.get("meta") or {}
            downgrade_reason = meta.get("downgrade_reason")
            if downgrade_reason and self.config.cb_enabled:
                # In async mode, we can await it or use create_task. 
                # Better await to ensure P4.1 latency is tracked correctly if needed, 
                # but CB is usually side-channel.
                asyncio.create_task(self._check_upstream_circuit(env, sid, str(downgrade_reason)))

            await self.trace_writer.emit_diag(dtrace, stage="dispatch_ok")  # type: ignore
            await self.trace_writer.persist_trace_meta(sid=sid, trace=dtrace)  # type: ignore
            return True  # type: ignore
        except Exception as exc:
            self.logger.error("Unexpected error sid=%s msg=%s err=%s", sid, msg_id, exc, exc_info=True)
            try:
                await self.retry_scheduler.schedule_target_retry(  # type: ignore
                    target="__env__",  # type: ignore
                    sid=sid,
                    env=env,
                    attempt=(env.get("attempt", 0) or 0) + 1,
                    last_error=str(exc),
                )
                dtrace.add(where="dispatch", name="dispatch_unexpected", ok=False, veto=False, reason_code="DISPATCH_ERROR", etype="gate", extra={"err": str(exc)})
                await self.trace_writer.emit_diag(dtrace, stage="dispatch_error", extra={"outcome": "scheduled"})  # type: ignore
                return True  # type: ignore
            except Exception:
                return True
        finally:
            if lease:
                await self.lease_manager.release_sid_lease(sid, lease)  # type: ignore
  # type: ignore
    async def _check_upstream_circuit(self, env: dict[str, Any], sid: str, downgrade_reason: str) -> None:
        if not self.config.cb_enabled or not downgrade_reason:
            return
        if downgrade_reason == "circuit_disabled":
            return

        try:
            now_ms = int(time.time() * 1000)
            meta = env.get("meta") or {}
            sym = str(meta.get("symbol") or sid.split(":")[0] if ":" in sid else "unknown")

            await call_with_timeout(
                record_downgrade_and_maybe_trip(
                    self.redis,
                    sym=sym,
                    now_ms=now_ms,
                    downgrade_reason=downgrade_reason,
                    window_ms=self.config.cb_window_ms,
                    max_downgrades_in_window=self.config.cb_max_downgrades,
                    disable_ms=self.config.cb_disable_ms,
                    block_auto_apply=self.config.cb_block_auto_apply,
                    auto_apply_reason=self.config.cb_auto_apply_reason,
                ),
                timeout_ms=self.config.cb_timeout_ms,
            )

            if sym not in self.of_inputs_v3_cb_states:
                self.of_inputs_v3_cb_states[sym] = SimpleNamespace(
                    symbol=sym,
                    of_inputs_v3_cb_last_refresh_ts_ms=0,
                    of_inputs_v3_disabled_until_ms=0,
                    of_inputs_v3_disabled_reason="",
                    of_inputs_v3_disabled_hard_until_ms=0,
                    of_inputs_v3_disabled_phase="",
                )

            cb_state = self.of_inputs_v3_cb_states[sym]
            disabled, until_ms, reason = await refresh_disabled_state(self.redis, cb_state, now_ms)

            if of_inputs_v3_circuit_disabled:
                of_inputs_v3_circuit_disabled.labels(symbol=sym).set(1 if disabled else 0)
            if of_inputs_v3_circuit_disabled_until_ms:
                of_inputs_v3_circuit_disabled_until_ms.labels(symbol=sym).set(until_ms)
        except Exception as e:
            self.logger.warning(f"Failed to record upstream downgrade sid={sid} reason={downgrade_reason}: {e}")

    async def _process_outbox_message(self, helper: AsyncRedisStreamHelper, *, stream: str, msg_id: str, fields: dict[str, Any], where: str, ack_ctr_ok: str, ack_ctr_fail: str, handle_transient_ctr: str, handle_failed_ctr: str) -> None:
        if not msg_id:
            return

        try:
            if await self._try_ack_retry_only(helper, stream, msg_id):
                return
        except Exception:
            pass

        if not await self.lease_manager.try_acquire_lease(msg_id):  # type: ignore
            return  # type: ignore

        try:
            if await self.idempotency_store.is_outbox_done(msg_id):  # type: ignore
                await self._finalize_ack(helper, stream, msg_id, ctr_ok=ack_ctr_ok, ctr_fail=ack_ctr_fail, where=f"{where}_done_fastpath")  # type: ignore
                return

            env = await self.envelope_parser.parse_envelope(fields)  # type: ignore
            if not env:  # type: ignore
                ok = await self.dlq_writer.send_dlq_and_ack(msg_id, fields, helper=helper, stream=stream, reason="bad_envelope")  # type: ignore
                return  # type: ignore

            sid = (env.get("sid") or "")
            if not sid:
                await self.dlq_writer.send_dlq_and_ack(msg_id, env, helper=helper, stream=stream, reason="missing_sid")  # type: ignore
                return  # type: ignore

            ack_now = False
            try:
                ack_now = await self._handle_env(msg_id=msg_id, env=env, sid=sid)
            except Exception as exc:
                self.error_handler.handle(  # type: ignore
                    exc,  # type: ignore
                    context=where,
                    msg_id=msg_id,
                    ctr_transient=handle_transient_ctr,
                    ctr_fatal=handle_failed_ctr,
                    log_transient=False
                )
                return

            if ack_now:
                await self._finalize_ack(helper, stream, msg_id, ctr_ok=ack_ctr_ok, ctr_fail=ack_ctr_fail, where=where)
        finally:
            await self.lease_manager.release_lease(msg_id)  # type: ignore
  # type: ignore
    async def _handle_one(self, msg_id: str, fields: dict[str, Any]) -> bool:
        env = await self.envelope_parser.parse_envelope(fields)  # type: ignore
        if not env:  # type: ignore
            await self.dlq_writer.send_target_dlq("__env__", "unknown", fields, reason="bad_envelope", err="Parse failed")  # type: ignore
            return True  # type: ignore

        sid = (env.get("sid") or "")
        if not sid:
            await self.dlq_writer.send_target_dlq("__env__", "missing", env, reason="missing_sid", err="SID missing")  # type: ignore
            return True  # type: ignore

        return await self._handle_env(msg_id=msg_id, env=env, sid=sid)

    async def _process_new_batch(self, helper: AsyncRedisStreamHelper, messages: Sequence[Any]) -> None:
        for stream, items in messages:
            for m in items:
                msg_id = getattr(m, "msg_id", "") or ""
                fields = getattr(m, "fields", {}) or {}
                await self._process_outbox_message(
                    helper,
                    stream=stream,
                    msg_id=msg_id,
                    fields=fields,
                    where="new",
                    ack_ctr_ok="acked",
                    ack_ctr_fail="ack_failed",
                    handle_transient_ctr="handle_transient",
                    handle_failed_ctr="handle_failed",
                )

    def _remember_ack_retry(self, stream: str, msg_id: str) -> None:
        key = (stream, msg_id)
        self._ack_retry[key] = time.monotonic()
        if len(self._ack_retry) > self.config.ack_retry_max:
            sorted_keys = sorted(self._ack_retry.keys(), key=lambda k: self._ack_retry[k])
            for k in sorted_keys[:max(1, self.config.ack_retry_max // 10)]:
                self._ack_retry.pop(k, None)

    async def _try_ack_retry_only(self, helper: AsyncRedisStreamHelper, stream: str, msg_id: str) -> bool:
        key = (stream, msg_id)
        if key not in self._ack_retry:
            return False
        try:
            await helper.ack(stream, msg_id)
            self._ack_retry.pop(key, None)
            self.ctr["acked_retry_ok"] += 1
            return True
        except Exception as e:
            if is_transient_error(e):
                self.ctr["acked_retry_transient"] += 1
                return True
            self._ack_retry.pop(key, None)
            self.ctr["acked_retry_drop"] += 1
            return False

    async def _maybe_claim_pending(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if (now - self._last_claim_mono) * 1000.0 < float(self.config.claim_every_ms):
            return
        self._last_claim_mono = now

        claimed_total = 0
        claimed_msgs: list[Any] = []
        last_next_id: str | None = None

        while claimed_total < self.config.claim_budget_per_tick:
            try:
                next_id, msgs = await helper.claim_pending(
                    self.config.outbox_stream,
                    min_idle_ms=self.config.claim_min_idle_ms,
                    start_id=self._pending_start_id,
                    count=min(self.config.claim_count, self.config.claim_budget_per_tick - claimed_total),
                )
            except Exception as e:
                if is_transient_error(e):
                    self.ctr["claim_transient"] += 1
                    return
                raise

            if (not msgs) and ((next_id or "") == "0-0"):
                self.ctr["claim_wrap_empty"] += 1
                break

            if msgs:
                self.ctr["claimed"] += len(msgs)
                claimed_total += len(msgs)
                claimed_msgs.extend(list(msgs))

            last_next_id = next_id if next_id else last_next_id
            if last_next_id:
                self._pending_start_id = last_next_id

        self._pending_claimed += claimed_total

        for m in claimed_msgs:
            msg_id = getattr(m, "msg_id", "") or ""
            fields = getattr(m, "fields", None) or {}

            if await self._try_ack_retry_only(helper, self.config.outbox_stream, msg_id):
                continue

            if await self.idempotency_store.is_outbox_done(msg_id):  # type: ignore
                try:  # type: ignore
                    await helper.ack(self.config.outbox_stream, msg_id)
                    self.ctr["acked_claimed_done_only"] += 1
                except Exception as e:
                    self.ctr["ack_failed_claimed_done_only"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.config.outbox_stream, msg_id)
                continue

            if not await self.lease_manager.try_acquire_lease(msg_id):  # type: ignore
                continue  # type: ignore

            ok = False
            try:
                ok = await self._handle_one(msg_id, fields)
            except Exception as exc:
                self.logger.error("Failed to handle claimed pending msg %s: %s", msg_id, exc, exc_info=True)
                ok = False
            if ok:
                await self.idempotency_store.mark_outbox_done(msg_id)  # type: ignore
                try:  # type: ignore
                    await helper.ack(self.config.outbox_stream, msg_id)
                    self.ctr["acked_claimed"] += 1
                except Exception as e:
                    self.ctr["ack_failed_claimed"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.config.outbox_stream, msg_id)
                        self.logger.warning("Transient ACK failed (claimed) %s: %s (will retry ack)", msg_id, e)
                    else:
                        self.logger.warning("ACK failed (claimed) %s: %s", msg_id, e)
            await self.lease_manager.release_lease(msg_id)  # type: ignore
  # type: ignore
    async def _cleanup_dead_consumers(self, helper: AsyncRedisStreamHelper) -> None:
        if not self.config.cleanup_dead_consumers:
            return
        now = time.monotonic()
        if now - self._last_consumer_cleanup < 60.0:
            return
        self._last_consumer_cleanup = now

        try:
            cs = await helper.consumers_info(self.config.outbox_stream)  # type: ignore
        except Exception:  # type: ignore
            return

        for c in cs or []:
            name = (c.get("name") or "")
            pending = c.get("pending") or 0
            idle = c.get("idle") or 0

            if not name or pending <= 0:
                continue
            if idle < self.config.dead_consumer_idle_ms:
                continue

            try:
                await self.redis.xgroup_delconsumer(self.config.outbox_stream, self.config.group, name)  # type: ignore
                self.ctr["delconsumer"] += 1  # type: ignore
                self.logger.warning("xgroup_delconsumer: %s (pending=%d idle_ms=%d)", name, pending, idle)
            except Exception:
                continue

    async def _tick_housekeeping(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_ack_cleanup_mono > 60.0:
            self._last_ack_cleanup_mono = now
            ttl = self.config.ack_retry_ttl_s
            self._ack_retry = {k: v for k, v in self._ack_retry.items() if now - v < ttl}

        await self.dispatch_metrics.tick_metrics(helper)  # type: ignore
        await self.marker_repair.repair_orphan_markers_best_effort()  # type: ignore
  # type: ignore
    async def run(self) -> None:
        await self.initialize()
        
        helper = AsyncRedisStreamHelper(self.redis, self.config.group, self.config.consumer)
        await helper.ensure_groups([self.config.outbox_stream], start_id="0")
        self.logger.info("SignalDispatcher started (ASYNC). stream=%s group=%s consumer=%s", self.config.outbox_stream, self.config.group, self.config.consumer)
        
        try:
            while True:
                await self.retry_scheduler.drain_retries_best_effort()  # type: ignore
                await self._tick_housekeeping(helper)  # type: ignore
                await self._maybe_claim_pending(helper)

                messages = await helper.read(
                    {self.config.outbox_stream: ">"},
                    count=self.config.read_count,
                    block=self.config.read_block_ms,
                    recover_start_id="0",  # type: ignore
                )  # type: ignore

                if not messages:
                    await self._maybe_claim_pending(helper)
                    await self.dispatch_metrics.maybe_log_diagnostics(helper)  # type: ignore
                    self._lease_contention = 0  # type: ignore
                    self._pending_claimed = 0
                    await self.marker_repair.maybe_maintenance()  # type: ignore
                    continue  # type: ignore

                for stream, items in messages:
                    for m in items:
                        msg_id = getattr(m, "msg_id", "") or ""
                        fields = getattr(m, "fields", {}) or {}
                        if not msg_id:
                            continue

                        if await self.idempotency_store.is_outbox_done(msg_id):  # type: ignore
                            try:  # type: ignore
                                await helper.ack(stream, msg_id)
                                self.ctr["acked_done_fastpath"] += 1
                            except Exception as exc:
                                self.ctr["ack_failed_done_fastpath"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, msg_id)
                            continue

                        if not await self.lease_manager.try_acquire_lease(msg_id):  # type: ignore
                            continue  # type: ignore

                        ack_now = False
                        try:
                            ack_now = await self._handle_one(msg_id, fields)
                        except Exception as exc:
                            self.ctr["handle_one_ex"] += 1
                            if not is_transient_error(exc):
                                self.logger.error("Failed msg %s: %s", msg_id, exc, exc_info=True)
                            ack_now = False

                        if ack_now:
                            await self.idempotency_store.mark_outbox_done(msg_id)  # type: ignore
                            try:  # type: ignore
                                await helper.ack(stream, msg_id)
                                self.ctr["acked"] += 1
                            except Exception as exc:
                                self.ctr["ack_failed"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, msg_id)
                                    self.logger.warning("Transient ACK failed %s: %s (will retry ack)", msg_id, exc)
                                else:
                                    self.logger.warning("ACK failed %s: %s", msg_id, exc)
                        await self.lease_manager.release_lease(msg_id)  # type: ignore
  # type: ignore
                await self.dispatch_metrics.maybe_log_diagnostics(helper)  # type: ignore
                self._lease_contention = 0  # type: ignore
                self._pending_claimed = 0
                await self.marker_repair.maybe_maintenance()  # type: ignore
  # type: ignore
        except KeyboardInterrupt:
            self.logger.info("SignalDispatcher stopped")
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            self.logger.warning("Redis connection lost in dispatcher loop. Retrying...")
            await asyncio.sleep(1)
        except Exception as exc:
            self.logger.error("Dispatcher loop error: %s", exc, exc_info=True)
            await asyncio.sleep(1)


async def main():
    dispatcher = SignalDispatcher()
    await dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
