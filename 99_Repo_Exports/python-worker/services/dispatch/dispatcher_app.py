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
from common.payload_fingerprint import fingerprint_tradeable_payload
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


def get_redis() -> Any:
    """Module-level stub — tests patch this to inject FakeRedis."""
    return None


def trace_enabled() -> bool:
    """Module-level stub — tests patch this to disable tracing."""
    return False


class Span:
    """No-op span stub. Tests replace this with their own implementation."""
    def ms(self) -> float:
        return 0.0


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
    def __init__(self, loop=None, *, redis_client: Any = None):
        self.logger = logger
        self.loop = loop or asyncio.get_event_loop()

        self.redis = redis_client
        self.dual_redis = None
        self.simple_redis = None

        self.config = SignalDispatcherConfig.from_env()
        self.ctr: defaultdict[str, int] = defaultdict(int)
        self._ctr: defaultdict[str, int] = defaultdict(int)

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

    async def _async_handle_env(self, *, msg_id: str, env: dict[str, Any], sid: str) -> bool:
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

    async def _async_handle_one(self, msg_id: str, fields: dict[str, Any]) -> bool:
        env = await self.envelope_parser.parse_envelope(fields)  # type: ignore
        if not env:  # type: ignore
            await self.dlq_writer.send_target_dlq("__env__", "unknown", fields, reason="bad_envelope", err="Parse failed")  # type: ignore
            return True  # type: ignore

        sid = (env.get("sid") or "")
        if not sid:
            await self.dlq_writer.send_target_dlq("__env__", "missing", env, reason="missing_sid", err="SID missing")  # type: ignore
            return True  # type: ignore

        return await self._async_handle_env(msg_id=msg_id, env=env, sid=sid)

    async def _async_process_new_batch(self, helper: AsyncRedisStreamHelper, messages: Sequence[Any]) -> None:
        for stream, items in messages:
            for m in items:
                msg_id = getattr(m, "msg_id", "") or ""
                fields = getattr(m, "fields", {}) or {}
                self._process_outbox_message(
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
        ack_retry_max = int(getattr(self, "_ack_retry_max", None) or (self.config.ack_retry_max if hasattr(self, "config") and self.config is not None else 20000))
        if len(self._ack_retry) > ack_retry_max:
            sorted_keys = sorted(self._ack_retry.keys(), key=lambda k: self._ack_retry[k])
            for k in sorted_keys[:max(1, ack_retry_max // 10)]:
                self._ack_retry.pop(k, None)

    async def _async_try_ack_retry_only(self, helper: AsyncRedisStreamHelper, stream: str, msg_id: str) -> bool:
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

    # ------------------------------------------------------------------ #
    # Sync helpers (used by tests and ACK-only recovery path)            #
    # ------------------------------------------------------------------ #

    _TARGET_PAYLOAD_KEYS: dict[str, list[str]] = {
        "notify": ["notify"],
        "signal_stream": ["signal_stream_payload", "signal_stream"],
        "audit": ["audit_payload", "audit"],
        "manual": ["manual_payload", "manual"],
    }

    def _evalsha_or_eval(
        self, client: Any, sha: str | None, tag: str, script: str, nkeys: int, *argv: Any
    ) -> Any:
        """Stub: overridden in tests. Production path uses Lua scripts via LuaScriptManager."""
        return None

    def _marker_key(self, target: str, sid: str) -> str:
        prefix = getattr(self, "marker_prefix", "signal:delivery:marker")
        return f"{prefix}:{target}:{sid}"

    def _env_done_key(self, sid: str) -> str:
        prefix = getattr(self, "done_prefix", "signal:done")
        return f"{prefix}:{sid}"

    def _schedule_target_retry(self, target: str, sid: str, env: dict[str, Any], attempt: int, last_error: str) -> None:
        pass  # overridden in tests

    def _send_target_dlq(self, target: str, sid: str, env: dict[str, Any], reason: str, err: str) -> None:
        pass  # overridden in tests

    def _marker_exists(self, client: Any, target: str, sid: str) -> bool:
        marker_key = self._marker_key(target, sid)
        try:
            return bool(client.exists(marker_key))
        except Exception:
            return False

    def _deliver_targets_with_retry(
        self, env: dict[str, Any], sid: str, *, targets: list[str] | None = None, **kwargs: Any
    ) -> None:
        env_targets = env.get("targets") or {}
        meta = env.get("meta") or {}
        targets_list = list(targets) if targets is not None else list(env_targets.keys())
        base_attempts: dict[str, int] | None = kwargs.get("base_attempts")
        all_ok = True
        attempts = env.setdefault("attempts", {})
        dual_client = getattr(self, "dual_redis", None) or self.redis
        simple_client = getattr(self, "simple_redis", None) or self.redis

        for i, target in enumerate(targets_list):
            r = self.redis
            check_marker = getattr(self, "_marker_exists", None)
            if check_marker is None:
                check_marker = self._marker_exists
            if check_marker(r, target, sid):
                continue

            if base_attempts is not None and i == 0 and "__forced__" in base_attempts:
                attempt = int(base_attempts["__forced__"])
            else:
                attempt = int(attempts.get(target) or 0) + 1

            deliver_fn = getattr(self, "_deliver_one_target", None)
            if deliver_fn is None:
                deliver_fn = self._deliver_one_target
            _exc: Exception | None = None
            try:
                deliver_fn(
                    env=env, sid=sid, target=target,
                    targets_obj=env_targets, meta=meta,
                    dual_client=dual_client, simple_client=simple_client,
                )
            except Exception as _e:
                _exc = _e
                all_ok = False
            attempts[target] = attempt
            if _exc is not None:
                try:
                    self._schedule_target_retry(target=target, sid=sid, env=env, attempt=attempt, last_error=str(_exc))
                except Exception:
                    pass
                if not is_transient_error(_exc):
                    try:
                        self._send_target_dlq(target, sid, env, reason="permanent_error", err=str(_exc))
                    except Exception:
                        pass

        if all_ok and targets_list:
            done_key = self._env_done_key(sid)
            ttl = int(getattr(self, "delivery_marker_ttl_sec", 3600))
            r = self._r()
            if r is not None:
                try:
                    r.set(done_key, "1", ex=ttl)
                except Exception:
                    pass

    def _msg_done_key(self, msg_id: str) -> str:
        prefix = getattr(self, "msg_done_prefix", "signal:outbox:msg_done")
        return f"{prefix}:{msg_id}"

    def _mark_msg_done(self, msg_id: str) -> None:
        ttl = getattr(self, "done_ttl_sec", 3600)
        self.redis.set(self._msg_done_key(msg_id), "1", ex=ttl)

    def _xack_only(self, *, msg_id: str) -> None:
        stream = getattr(self, "outbox_stream", None) or self.config.outbox_stream
        group = getattr(self, "group", None) or self.config.group
        self.redis.xack(stream, group, msg_id)

    def _process_one_outbox_message(self, *, msg_id: str, env: dict[str, Any], sid: str) -> None:
        if self.redis.exists(self._msg_done_key(msg_id)):
            self._xack_only(msg_id=msg_id)
            return
        self._deliver_targets_with_retry(env, sid)
        self._mark_msg_done(msg_id)
        self._xack_only(msg_id=msg_id)

    async def _async_maybe_claim_pending(self, helper: AsyncRedisStreamHelper) -> None:
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

            if await self._async_try_ack_retry_only(helper, self.config.outbox_stream, msg_id):
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
                ok = await self._async_handle_one(msg_id, fields)
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

    async def _async_tick_housekeeping(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_ack_cleanup_mono > 60.0:
            self._last_ack_cleanup_mono = now
            ttl = self.config.ack_retry_ttl_s
            self._ack_retry = {k: v for k, v in self._ack_retry.items() if now - v < ttl}

        await self.dispatch_metrics.tick_metrics(helper)  # type: ignore
        await self.marker_repair.repair_orphan_markers_best_effort()  # type: ignore
  # type: ignore

    # ================================================================== #
    # Sync backward-compat API (used by unit tests)                      #
    # ================================================================== #

    def _r(self) -> Any:
        return getattr(self, "redis", None) or getattr(self, "simple_redis", None)

    def _done_key(self, sid: str) -> str:
        prefix = getattr(self, "done_prefix", "signal:done")
        return f"{prefix}:{sid}"

    def _sid_done_key(self, sid: str) -> str:
        prefix = getattr(self, "metrics_prefix", "signal_dispatcher") or "signal_dispatcher"
        return f"{prefix}:sid_done:{sid}"

    def _mark_outbox_done(self, msg_id: str) -> None:
        r = self._r()
        if r is None:
            return
        ttl = int(getattr(self, "done_ttl_sec", 3600))
        try:
            r.setex(self._msg_done_key(str(msg_id)), ttl, "1")
        except Exception:
            pass

    def _is_outbox_done(self, msg_id: str) -> bool:
        r = self._r()
        if r is None:
            return False
        try:
            v = r.get(self._msg_done_key(str(msg_id)))
            if v in (None, "", b""):
                v = r.get(self._done_key(str(msg_id)))
            if v in (None, "", b""):
                return False
            if isinstance(v, bytes):
                v = v.decode("utf-8", "ignore")
            return str(v).strip() == "1"
        except Exception:
            return False

    def _env_req_key(self, sid: str) -> str:
        return f"signal_dispatcher:env_req:{sid}"

    def _update_env_req(self, sid: str, required: set) -> None:
        if not required:
            return
        r = self._r()
        if r is None:
            return
        ttl = int(getattr(self, "env_state_ttl_sec", 3600))
        k = self._env_req_key(sid)
        try:
            pipe = r.pipeline(transaction=False)
            pipe.sadd(k, *required)
            pipe.expire(k, ttl)
            pipe.execute()
        except Exception:
            pass

    def _parse_envelope(self, fields: dict) -> dict | None:
        for key in ("data", "envelope_json", "payload_json", "payload"):
            val = fields.get(key)
            if val:
                try:
                    if isinstance(val, bytes):
                        val = val.decode("utf-8", "ignore")
                    if isinstance(val, str):
                        env = json.loads(val)
                    elif isinstance(val, dict):
                        env = dict(val)
                    else:
                        continue
                    # Fingerprint integrity check
                    stored_sha = (env.get("meta") or {}).get("payload_sha1")
                    if stored_sha:
                        computed, _ = fingerprint_tradeable_payload(env)
                        if computed and computed != stored_sha:
                            r = self._r()
                            dlq = getattr(self, "dlq_stream", None)
                            if r is not None and dlq:
                                with contextlib.suppress(Exception):
                                    r.xadd(dlq, {"reason": "fingerprint_mismatch", "sid": str(env.get("sid", ""))})
                            return None
                    return env
                except Exception:
                    pass
        return None

    def _send_dlq(self, msg_id: str, fields: Any, reason: str) -> None:
        pass

    def _send_dlq_and_ack(self, msg_id: str, data: Any, reason: str) -> bool:
        return True

    def _try_acquire_sid_lease(self, sid: str) -> Any:
        return "stub-lease"

    def _release_sid_lease(self, sid: str, lease: Any) -> None:
        pass

    def _try_acquire_lease(self, msg_id: str) -> bool:
        return True

    def _release_lease(self, msg_id: str) -> None:
        pass

    def _try_ack_retry_only(self, helper: Any, stream: str, msg_id: str) -> bool:
        key = (stream, msg_id)
        ack_retry = getattr(self, "_ack_retry", {})
        if key not in ack_retry:
            return False
        try:
            helper.ack(stream, msg_id)
            ack_retry.pop(key, None)
            return True
        except Exception:
            ack_retry.pop(key, None)
            return False

    def _tick_housekeeping(self, helper: Any) -> None:
        now = time.monotonic()
        last = getattr(self, "_last_ack_cleanup_mono", 0.0)
        if now - last > 60.0:
            self._last_ack_cleanup_mono = now
            ttl = float(getattr(self, "_ack_retry_ttl_s", None) or (
                self.config.ack_retry_ttl_s if hasattr(self, "config") and self.config is not None else 600.0
            ))
            self._ack_retry = {k: v for k, v in getattr(self, "_ack_retry", {}).items() if now - v < ttl}

    def _pending_by_consumer(self, limit: int = 100) -> dict[str, int]:
        r = self._r()
        if r is None:
            return {}
        try:
            stream = getattr(self, "outbox_stream", None) or (
                self.config.outbox_stream if hasattr(self, "config") and self.config is not None else "signals:outbox"
            )
            group = getattr(self, "group", None) or (
                self.config.group if hasattr(self, "config") and self.config is not None else "dispatcher"
            )
            raw = r.execute_command("XPENDING", stream, group, "-", "+", limit)
            counts: dict[str, int] = {}
            for entry in (raw or []):
                consumer = entry[1]
                if isinstance(consumer, bytes):
                    consumer = consumer.decode()
                else:
                    consumer = str(consumer)
                counts[consumer] = counts.get(consumer, 0) + 1
            return counts
        except Exception:
            return {}

    def _handle_env(self, msg_id: str = "", env: dict | None = None, sid: str = "", **_kw: Any) -> bool:
        return True

    def _handle_one(self, msg_id: str, fields: dict) -> bool:
        env = None
        try:
            env = self._parse_envelope(fields)
        except Exception:
            env = None
        if not env:
            with contextlib.suppress(Exception):
                self._send_dlq(str(msg_id), fields, "bad_envelope")
            return True
        sid = (env.get("sid") or "")
        if not sid:
            with contextlib.suppress(Exception):
                self._send_dlq(str(msg_id), env, "missing_sid")
            return True
        lease = None
        acquire_fn = getattr(self, "_try_acquire_sid_lease", None)
        if acquire_fn is not None:
            lease = acquire_fn(sid)
            if lease is None:
                try:
                    outbox_stream = getattr(self, "outbox_stream", None) or (
                        self.config.outbox_stream if hasattr(self, "config") and self.config is not None else "signals:outbox"
                    )
                    self.redis.xadd(outbox_stream, fields)
                except Exception:
                    return False
                return True
        try:
            self._deliver_targets_with_retry(env, sid)
        except Exception:
            pass
        if lease is not None:
            release_fn = getattr(self, "_release_sid_lease", None)
            if release_fn is not None:
                with contextlib.suppress(Exception):
                    release_fn(sid, lease)
        return True

    def _process_outbox_message(
        self,
        helper: Any,
        *,
        stream: str,
        msg_id: str,
        fields: dict,
        where: str = "new",
        ack_ctr_ok: str = "acked",
        ack_ctr_fail: str = "ack_failed",
        handle_transient_ctr: str = "handle_transient",
        handle_failed_ctr: str = "handle_failed",
    ) -> None:
        if not msg_id:
            return
        if self._is_outbox_done(str(msg_id)):
            try:
                helper.ack(stream, str(msg_id))
            except Exception:
                pass
            return
        env = self._parse_envelope(fields)
        if not env:
            self._send_dlq_and_ack(str(msg_id), fields, "bad_envelope")
            return
        sid = (env.get("sid") or "")
        if not sid:
            self._send_dlq_and_ack(str(msg_id), env, "missing_sid")
            return
        ack_now = self._handle_env(msg_id=str(msg_id), env=env, sid=sid)
        if ack_now:
            self._mark_outbox_done(str(msg_id))
            try:
                helper.ack(stream, str(msg_id))
            except Exception:
                pass

    def _process_new_batch(self, helper: Any, messages: Any) -> None:
        ctr = getattr(self, "_ctr", {})
        for stream, items in (messages or []):
            for m in (items or []):
                msg_id = str(getattr(m, "msg_id", "") or "")
                fields = dict(getattr(m, "fields", {}) or {})
                if not msg_id:
                    continue
                if self._is_outbox_done(msg_id):
                    try:
                        helper.ack(stream, msg_id)
                        ctr["acked"] = ctr.get("acked", 0) + 1
                    except Exception:
                        ctr["ack_failed"] = ctr.get("ack_failed", 0) + 1
                    continue
                if not self._try_acquire_lease(msg_id):
                    continue
                ack_now = False
                try:
                    ack_now = self._handle_one(msg_id, fields)
                except Exception as exc:
                    if is_transient_error(exc):
                        ctr["handle_transient"] = ctr.get("handle_transient", 0) + 1
                    else:
                        ctr["handle_failed"] = ctr.get("handle_failed", 0) + 1
                finally:
                    self._release_lease(msg_id)
                if ack_now:
                    self._mark_outbox_done(msg_id)
                    try:
                        helper.ack(stream, msg_id)
                        ctr["acked"] = ctr.get("acked", 0) + 1
                    except Exception:
                        ctr["ack_failed"] = ctr.get("ack_failed", 0) + 1

    def _process_pending_batch(self, helper: Any, messages: Any) -> None:
        outbox_stream = getattr(self, "outbox_stream", None) or (
            self.config.outbox_stream if hasattr(self, "config") and self.config is not None else "signals:outbox"
        )
        for m in (messages or []):
            msg_id = str(getattr(m, "msg_id", "") or "")
            fields = dict(getattr(m, "fields", {}) or {})
            if not msg_id:
                continue
            if self._is_outbox_done(msg_id):
                try:
                    helper.ack(outbox_stream, msg_id)
                except Exception:
                    pass
                continue
            if not self._try_acquire_lease(msg_id):
                continue
            ack_now = False
            try:
                ack_now = self._handle_one(msg_id, fields)
            except Exception:
                pass
            finally:
                self._release_lease(msg_id)
            if ack_now:
                self._mark_outbox_done(msg_id)
                try:
                    helper.ack(outbox_stream, msg_id)
                except Exception:
                    pass

    def _handle_read_messages(self, helper: Any, messages: Any) -> None:
        outbox_stream = getattr(self, "outbox_stream", None) or (
            self.config.outbox_stream if hasattr(self, "config") and self.config is not None else "signals:outbox"
        )
        for stream, items in (messages or []):
            for m in (items or []):
                msg_id = str(getattr(m, "msg_id", "") or "")
                fields = dict(getattr(m, "fields", {}) or {})
                if not msg_id:
                    continue
                if self._try_ack_retry_only(helper, stream, msg_id):
                    continue
                if self._is_outbox_done(msg_id):
                    try:
                        helper.ack(stream, msg_id)
                    except Exception:
                        pass
                    continue
                if not self._try_acquire_lease(msg_id):
                    continue
                ack_now = False
                try:
                    env = self._parse_envelope(fields)
                    if env:
                        sid = env.get("sid") or ""
                        if sid:
                            ack_now = self._handle_env(msg_id, env, sid)
                except Exception:
                    pass
                finally:
                    self._release_lease(msg_id)
                if ack_now:
                    self._mark_outbox_done(msg_id)
                    try:
                        helper.ack(stream, msg_id)
                    except Exception:
                        pass

    def _maybe_claim_pending(self, helper: Any) -> None:
        now = time.monotonic()
        claim_every_ms = float(getattr(self, "claim_every_ms", None) or (
            self.config.claim_every_ms if hasattr(self, "config") and self.config is not None else 10000.0
        ))
        if (now - self._last_claim_mono) * 1000.0 < claim_every_ms:
            return
        self._last_claim_mono = now
        claim_budget = int(getattr(self, "claim_budget_per_tick", None) or (
            self.config.claim_budget_per_tick if hasattr(self, "config") and self.config is not None else 100
        ))
        claim_count = int(getattr(self, "claim_count", None) or (
            self.config.claim_count if hasattr(self, "config") and self.config is not None else 10
        ))
        claim_min_idle_ms = int(getattr(self, "claim_min_idle_ms", None) or (
            self.config.claim_min_idle_ms if hasattr(self, "config") and self.config is not None else 60000
        ))
        outbox_stream = getattr(self, "outbox_stream", None) or (
            self.config.outbox_stream if hasattr(self, "config") and self.config is not None else "signals:outbox"
        )
        ctr = getattr(self, "_ctr", {})
        claimed_total = 0
        while claimed_total < claim_budget:
            try:
                next_id, msgs = helper.claim_pending(
                    outbox_stream,
                    min_idle_ms=claim_min_idle_ms,
                    start_id=self._pending_start_id,
                    count=min(claim_count, claim_budget - claimed_total),
                )
            except Exception:
                break
            if (not msgs) and ((next_id or "") == "0-0"):
                break
            if msgs:
                n = len(msgs)
                ctr["claimed"] = ctr.get("claimed", 0) + n
                claimed_total += n
            if next_id:
                self._pending_start_id = next_id
            for m in (msgs or []):
                if isinstance(m, str):
                    continue
                msg_id = str(getattr(m, "msg_id", "") or "")
                fields = dict(getattr(m, "fields", {}) or {})
                if not msg_id:
                    continue
                if self._try_ack_retry_only(helper, outbox_stream, msg_id):
                    continue
                if self._is_outbox_done(msg_id):
                    try:
                        helper.ack(outbox_stream, msg_id)
                    except Exception:
                        pass
                    continue
                if not self._try_acquire_lease(msg_id):
                    continue
                ok = False
                try:
                    env = self._parse_envelope(fields)
                    if env:
                        sid = env.get("sid") or ""
                        if sid:
                            ok = self._handle_env(msg_id, env, sid)
                except Exception:
                    pass
                finally:
                    self._release_lease(msg_id)
                if ok:
                    self._mark_outbox_done(msg_id)
                    try:
                        helper.ack(outbox_stream, msg_id)
                    except Exception:
                        pass

    def _deliver_one_target(
        self,
        *,
        env: dict,
        sid: str,
        target: str,
        targets_obj: dict,
        meta: dict,
        dual_client: Any,
        simple_client: Any,
    ) -> None:
        payload_keys = self._TARGET_PAYLOAD_KEYS.get(target, [target])
        payload = None
        for pk in payload_keys:
            payload = (targets_obj or {}).get(pk)
            if payload is not None:
                break
        if payload is None:
            raise RuntimeError(f"{target} missing targets.{target} payload")

    def _write_trace_sidecar_best_effort(self, sid: str, env: dict, patch: list) -> None:
        try:
            meta = env.get("meta") or {}
            trace_key = meta.get("trace_meta_key") or getattr(self, "outbox_meta_prefix", "signal:meta:") + sid
            r = self._r()
            if r is None:
                return
            existing_raw = r.get(trace_key)
            if existing_raw:
                if isinstance(existing_raw, bytes):
                    existing_raw = existing_raw.decode("utf-8", "ignore")
                try:
                    obj = json.loads(existing_raw)
                except Exception:
                    obj = {}
            else:
                obj = {}
            trace = obj.setdefault("trace", {})
            events = trace.setdefault("events", [])
            for item in (patch or []):
                if isinstance(item, dict):
                    events.append(dict(item))
            r.set(trace_key, json.dumps(obj, ensure_ascii=False))
        except Exception:
            pass

    async def run(self) -> None:
        await self.initialize()

        helper = AsyncRedisStreamHelper(self.redis, self.config.group, self.config.consumer)
        await helper.ensure_groups([self.config.outbox_stream], start_id="0")
        self.logger.info("SignalDispatcher started (ASYNC). stream=%s group=%s consumer=%s", self.config.outbox_stream, self.config.group, self.config.consumer)

        try:
            while True:
                await self.retry_scheduler.drain_retries_best_effort()  # type: ignore
                await self._async_tick_housekeeping(helper)  # type: ignore
                await self._async_maybe_claim_pending(helper)

                messages = await helper.read(
                    {self.config.outbox_stream: ">"},
                    count=self.config.read_count,
                    block=self.config.read_block_ms,
                    recover_start_id="0",  # type: ignore
                )  # type: ignore

                if not messages:
                    await self._async_maybe_claim_pending(helper)
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
                            ack_now = await self._async_handle_one(msg_id, fields)
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
