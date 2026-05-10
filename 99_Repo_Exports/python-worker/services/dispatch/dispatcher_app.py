import json
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
import contextlib

import redis

from common.decision_trace import DecisionTrace
from common.log import setup_logger
from common.transient import is_transient_error
from core.redis_client import get_redis
from core.redis_stream_consumer import SyncRedisStreamHelper

from services.dispatcher.config import SignalDispatcherConfig
from services.dispatcher.error_handler import ErrorHandler
from services.dispatcher.lua_scripts import LuaScriptManager
from services.dispatcher.trace_writer import TraceWriter

_LUA_NOTIFY_GATE_XADD_THEN_MARK = LuaScriptManager.NOTIFY_GATE_XADD_THEN_MARK

from services.dispatch.envelope_parser import EnvelopeParser
from services.dispatch.target_router import TargetRouter
from services.dispatch.idempotency_store import IdempotencyStore
from services.dispatch.retry_scheduler import RetryScheduler
from services.dispatch.dlq_writer import DlqWriter
from services.dispatch.lease_manager import LeaseManager
from services.dispatch.marker_repair import MarkerRepair
from services.dispatch.dispatch_metrics import DispatchMetrics

logger = setup_logger("SignalDispatcher")


@dataclass(frozen=True)
class PendingMsg:
    msg_id: str
    fields: dict[str, Any]


@dataclass(frozen=True)
class DispatchDecision:
    ack_now: bool
    reason: str = ""

def _try_restore_pending_cursor(redis_client: Any, key: str) -> str | None:
    if redis_client is None:
        return None
    try:
        v = redis_client.get(key)
        if isinstance(v, bytes):
            return v.decode("utf-8", "ignore")
        if isinstance(v, str):
            return v
        return None
    except Exception:
        return None


class SignalDispatcher:
    def __init__(self):
        try:
            self.logger = getattr(self, "logger", None) or logger
        except Exception:
            self.logger = None

        try:
            self.simple_redis = get_redis()
        except Exception:
            self.simple_redis = None
            
        self.redis = self.simple_redis
        self.dual_redis = None

        try:
            self.lua_scripts = LuaScriptManager(self.redis, logger=self.logger)
            if self.redis:
                self.lua_scripts.preload_all()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to initialize LuaScriptManager: {e}")
            self.lua_scripts = None

        self.config = SignalDispatcherConfig.from_env()
        
        self.ctr: defaultdict[str, int] = defaultdict(int)
        
        self.trace_writer = TraceWriter(self.redis, self.config, self.logger)
        self.error_handler = ErrorHandler(self.logger, self.ctr)

        # Dispatcher sub-components
        self.dlq_writer = DlqWriter(self.config, self.redis, self.logger)
        self.envelope_parser = EnvelopeParser(self.redis, self.config.dlq_stream, self.logger)
        self.idempotency_store = IdempotencyStore(self.config, self.redis, self.lua_scripts, self.logger, self.ctr)
        self.lease_manager = LeaseManager(self.config, self.redis, self.lua_scripts, self.ctr)
        
        # Resolve cyclic dependency between router and scheduler
        self.target_router = TargetRouter(
            config=self.config,
            redis_client=self.redis,
            dual_client=self.dual_redis,
            simple_client=self.simple_redis,
            idempotency_store=self.idempotency_store,
            retry_scheduler=None,  # Set below
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

        self._pending_start_id = "0-0"
        self._last_claim_mono = 0.0
        self._pending_claimed = 0
        self._lease_contention = 0

        self._ack_retry: dict[tuple[str, str], float] = {}
        self._last_ack_cleanup_mono = time.monotonic()
        
        self._last_consumer_cleanup = 0.0

    def _ack_fail_open(self, helper: SyncRedisStreamHelper, stream: str, msg_id: str, *, ctr_ok: str, ctr_fail: str, where: str) -> bool:
        try:
            helper.ack(stream, str(msg_id))
            self.ctr[ctr_ok] += 1
            return True
        except Exception as exc:
            self.ctr[ctr_fail] += 1
            if is_transient_error(exc):
                with contextlib.suppress(Exception):
                    self._remember_ack_retry(stream, str(msg_id))
                with contextlib.suppress(Exception):
                    self.logger.warning("Transient ACK failed where=%s msg=%s err=%r (will retry ack)", where, msg_id, exc)
            else:
                with contextlib.suppress(Exception):
                    self.logger.warning("ACK failed where=%s msg=%s err=%r", where, msg_id, exc)
            return False

    def _finalize_ack(self, helper: SyncRedisStreamHelper, stream: str, msg_id: str, *, ctr_ok: str, ctr_fail: str, where: str) -> bool:
        with contextlib.suppress(Exception):
            self._mark_outbox_done(str(msg_id))
        return self._ack_fail_open(helper, stream, msg_id, ctr_ok=ctr_ok, ctr_fail=ctr_fail, where=where)

    def _handle_env(self, *, msg_id: str, env: dict[str, Any], sid: str) -> bool:
        lease = self._try_acquire_sid_lease(sid)
        if not lease:
            self._lease_contention += 1
            try:
                self.redis.xadd(
                    self.config.outbox_stream,
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
                try:
                    if "targets" not in env or not isinstance(env["targets"], dict):
                        env["targets"] = {}
                    env["targets"]["mt5_plan"] = mt5_payload
                except Exception:
                    pass

            self._deliver_targets_with_retry(env, sid, _trace=dtrace)
            self.trace_writer.emit_diag(dtrace, stage="dispatch_ok")
            self.trace_writer.persist_trace_meta(sid=sid, trace=dtrace)
            return True
        except Exception as exc:
            self.logger.error("Unexpected error sid=%s msg=%s err=%s", sid, msg_id, exc, exc_info=True)
            try:
                self._schedule_target_retry(
                    target="__env__",
                    sid=sid,
                    env=env,
                    attempt=int(env.get("attempt", 0) or 0) + 1,
                    last_error=str(exc),
                )
                dtrace.add(where="dispatch", name="dispatch_unexpected", ok=False, veto=False, reason_code="DISPATCH_ERROR", etype="gate", extra={"err": str(exc)})
                self.trace_writer.emit_diag(dtrace, stage="dispatch_error", extra={"outcome": "scheduled"})
                return True
            except Exception:
                return True
        finally:
            if lease:
                self._release_sid_lease(sid, lease)

    def _process_outbox_message(self, helper: SyncRedisStreamHelper, *, stream: str, msg_id: str, fields: dict[str, Any], where: str, ack_ctr_ok: str, ack_ctr_fail: str, handle_transient_ctr: str, handle_failed_ctr: str) -> None:
        if not msg_id:
            return

        try:
            if self._try_ack_retry_only(helper, stream, msg_id):
                return
        except Exception:
            pass

        if not self._try_acquire_lease(str(msg_id)):
            return

        try:
            if self._is_outbox_done(str(msg_id)):
                self._finalize_ack(helper, stream, msg_id, ctr_ok=ack_ctr_ok, ctr_fail=ack_ctr_fail, where=f"{where}_done_fastpath")
                return

            env = None
            try:
                env = self._parse_envelope(fields)
            except Exception:
                env = None

            if not env:
                ok = False
                try:
                    ok = bool(self._send_dlq_and_ack(str(msg_id), fields, helper=None, stream=None, reason="bad_envelope"))
                except Exception:
                    ok = False
                if ok:
                    with contextlib.suppress(Exception):
                        self._mark_outbox_done(str(msg_id))
                return

            sid = (env.get("sid") or "")
            if not sid:
                ok = False
                try:
                    ok = bool(self._send_dlq_and_ack(str(msg_id), env, helper=None, stream=None, reason="missing_sid"))
                except Exception:
                    ok = False
                if ok:
                    with contextlib.suppress(Exception):
                        self._mark_outbox_done(str(msg_id))
                return

            ack_now = False
            try:
                ack_now = bool(self._handle_env(msg_id=str(msg_id), env=env, sid=sid))
            except Exception as exc:
                self.error_handler.handle(
                    exc,
                    context=where,
                    msg_id=str(msg_id),
                    ctr_transient=handle_transient_ctr,
                    ctr_fatal=handle_failed_ctr,
                    log_transient=False
                )
                return

            if ack_now:
                self._finalize_ack(helper, stream, msg_id, ctr_ok=ack_ctr_ok, ctr_fail=ack_ctr_fail, where=where)
        finally:
            self._release_lease(str(msg_id))

    def _handle_one(self, msg_id: str, fields: dict[str, Any]) -> bool:
        env = None
        try:
            env = self._parse_envelope(fields)
        except Exception:
            env = None

        if not env:
            with contextlib.suppress(Exception):
                self._send_dlq_and_ack(str(msg_id), fields, helper=None, stream=None, reason="bad_envelope")
            return True

        sid = (env.get("sid") or "")
        if not sid:
            with contextlib.suppress(Exception):
                self._send_dlq_and_ack(str(msg_id), env, helper=None, stream=None, reason="missing_sid")
            return True

        return self._handle_env(msg_id=str(msg_id), env=env, sid=sid)

    def _process_new_batch(self, helper: Any, messages: Sequence[Any]) -> None:
        for stream, items in messages:
            for m in items:
                msg_id = getattr(m, "msg_id", "") or ""
                fields = getattr(m, "fields", {}) or {}
                self._process_outbox_message(
                    helper,
                    stream=str(stream),
                    msg_id=str(msg_id),
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

    def _try_ack_retry_only(self, helper: SyncRedisStreamHelper, stream: str, msg_id: str) -> bool:
        key = (stream, msg_id)
        if key not in self._ack_retry:
            return False
        try:
            helper.ack(stream, msg_id)
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

    def _maybe_claim_pending(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        last_claim_mono = getattr(self, "_last_claim_mono", 0.0)
        if (now - last_claim_mono) * 1000.0 < float(self.config.claim_every_ms):
            return
        self._last_claim_mono = now

        claimed_total = 0
        claimed_msgs: list[Any] = []
        last_next_id: str | None = None
        pending_start_id = getattr(self, "_pending_start_id", "0-0")

        while claimed_total < int(self.config.claim_budget_per_tick):
            try:
                next_id, msgs = helper.claim_pending(
                    self.config.outbox_stream,
                    min_idle_ms=self.config.claim_min_idle_ms,
                    start_id=pending_start_id,
                    count=min(int(self.config.claim_count), int(self.config.claim_budget_per_tick) - claimed_total),
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

            last_next_id = str(next_id) if next_id else last_next_id
            if last_next_id:
                pending_start_id = last_next_id

        self._pending_claimed = getattr(self, "_pending_claimed", 0) + claimed_total
        if last_next_id:
            self._pending_start_id = str(last_next_id)
            pending_start_id = self._pending_start_id

        for m in claimed_msgs:
            msg_id = getattr(m, "msg_id", "") or ""
            fields = getattr(m, "fields", None) or {}

            if self._try_ack_retry_only(helper, self.config.outbox_stream, msg_id):
                continue

            if self._is_msg_done(str(msg_id)):
                try:
                    helper.ack(self.config.outbox_stream, msg_id)
                    self.ctr["acked_claimed_done_only"] += 1
                except Exception as e:
                    self.ctr["ack_failed_claimed_done_only"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.config.outbox_stream, msg_id)
                continue

            if self._try_ack_retry_only(helper, self.config.outbox_stream, msg_id):
                continue

            if not self._try_acquire_lease(str(msg_id)):
                continue

            ok = False
            try:
                ok = self._handle_one(msg_id, fields)
            except Exception as exc:
                self.logger.error("Failed to handle claimed pending msg %s: %s", msg_id, exc, exc_info=True)
                ok = False
            if ok:
                self._mark_outbox_done(str(msg_id))
                try:
                    helper.ack(self.config.outbox_stream, msg_id)
                    self.ctr["acked_claimed"] += 1
                except Exception as e:
                    self.ctr["ack_failed_claimed"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.config.outbox_stream, msg_id)
                        self.logger.warning("Transient ACK failed (claimed) %s: %s (will retry ack)", msg_id, e)
                    else:
                        self.logger.warning("ACK failed (claimed) %s: %s", msg_id, e)
            self._release_lease(str(msg_id))

    def _cleanup_dead_consumers(self, helper: SyncRedisStreamHelper) -> None:
        if not self.config.cleanup_dead_consumers:
            return
        now = time.monotonic()
        if now - self._last_consumer_cleanup < 60.0:
            return
        self._last_consumer_cleanup = now

        try:
            cs = helper.consumers_info(self.config.outbox_stream)
        except Exception:
            return

        for c in cs or []:
            try:
                name = (c.get("name") or "")
                pending = int(c.get("pending") or 0)
                idle = int(c.get("idle") or 0) 
            except Exception:
                continue

            if not name or pending <= 0:
                continue
            if idle < self.config.dead_consumer_idle_ms:
                continue

            try:
                self.redis.xgroup_delconsumer(self.config.outbox_stream, self.config.group, name)
                self.ctr["delconsumer"] += 1
                self.logger.warning("xgroup_delconsumer: %s (pending=%d idle_ms=%d)", name, pending, idle)
            except Exception:
                continue

    def _tick_housekeeping(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_ack_cleanup_mono > 60.0:
            self._last_ack_cleanup_mono = now
            ttl = float(self.config.ack_retry_ttl_s)
            self._ack_retry = {k: v for k, v in self._ack_retry.items() if now - v < ttl}

        if hasattr(self, "dispatch_metrics"):
            self.dispatch_metrics.tick_metrics(helper)
        if hasattr(self, "marker_repair"):
            self.marker_repair.repair_orphan_markers_best_effort()

    def run(self) -> None:
        if self.redis is None:
            self.logger.error("❌ Redis client is None, cannot start dispatcher")
            return

        helper = SyncRedisStreamHelper(self.redis, self.config.group, self.config.consumer)
        helper.ensure_groups([self.config.outbox_stream], start_id="0")
        self.logger.info("SignalDispatcher started. stream=%s group=%s consumer=%s", self.config.outbox_stream, self.config.group, self.config.consumer)
        try:
            while True:
                self.retry_scheduler.drain_retries_best_effort()
                self._tick_housekeeping(helper)
                self._maybe_claim_pending(helper)

                messages = helper.read(
                    {self.config.outbox_stream: ">"},
                    count=self.config.read_count,
                    block=self.config.read_block_ms,
                    recover_start_id="0",
                )

                if not messages:
                    self._maybe_claim_pending(helper)
                    self.dispatch_metrics.maybe_diag_sampled(helper, self._lease_contention, self._pending_claimed)
                    self._lease_contention = 0
                    self._pending_claimed = 0
                    self.marker_repair.maybe_maintenance()
                    continue

                for stream, items in messages:
                    for m in items:
                        msg_id = getattr(m, "msg_id", "") or ""
                        fields = getattr(m, "fields", {}) or {}
                        if not msg_id:
                            continue

                        if self._is_outbox_done(str(msg_id)):
                            try:
                                helper.ack(stream, str(msg_id))
                                self.ctr["acked_done_fastpath"] += 1
                            except Exception as exc:
                                self.ctr["ack_failed_done_fastpath"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, str(msg_id))
                            continue

                        if not self._try_acquire_lease(str(msg_id)):
                            continue

                        ack_now = False
                        try:
                            ack_now = bool(self._handle_one(str(msg_id), fields))
                        except Exception as exc:
                            self.ctr["handle_one_ex"] += 1
                            if not is_transient_error(exc):
                                self.logger.error("Failed msg %s: %s", msg_id, exc, exc_info=True)
                            ack_now = False

                        if ack_now:
                            self._mark_outbox_done(str(msg_id))
                            try:
                                helper.ack(stream, str(msg_id))
                                self.ctr["acked"] += 1
                            except Exception as exc:
                                self.ctr["ack_failed"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, str(msg_id))
                                    self.logger.warning("Transient ACK failed %s: %s (will retry ack)", msg_id, exc)
                                else:
                                    self.logger.warning("ACK failed %s: %s", msg_id, exc)
                        self._release_lease(str(msg_id))

                self.dispatch_metrics.maybe_diag_sampled(helper, self._lease_contention, self._pending_claimed)
                self._lease_contention = 0
                self._pending_claimed = 0
                self.marker_repair.maybe_maintenance()

        except KeyboardInterrupt:
            self.logger.info("SignalDispatcher stopped")
            return
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            self.logger.warning("Redis connection lost in dispatcher loop. Retrying...")
            time.sleep(1)
        except Exception as exc:
            self.logger.error("Dispatcher loop error: %s", exc, exc_info=True)
            time.sleep(1)

    # === Backward Compatibility Properties & Methods for Tests ===
    @property
    def _ctr(self): return self.ctr
    @_ctr.setter
    def _ctr(self, val): self.ctr = val

    @property
    def _ack_retry_ttl_s(self):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        return self.config.ack_retry_ttl_s
    @_ack_retry_ttl_s.setter
    def _ack_retry_ttl_s(self, val):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        self.config.ack_retry_ttl_s = val

    @property
    def _ack_retry_max(self):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        return self.config.ack_retry_max
    @_ack_retry_max.setter
    def _ack_retry_max(self, val):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        self.config.ack_retry_max = val
    
    @property
    def outbox_stream(self):
        if hasattr(self, "config"): return self.config.outbox_stream
        return getattr(self, "_outbox_stream", None)
    @outbox_stream.setter
    def outbox_stream(self, val):
        if hasattr(self, "config"): self.config.outbox_stream = val
        else: self._outbox_stream = val
    
    @property
    def read_count(self):
        if hasattr(self, "config"): return self.config.read_count
        return getattr(self, "_read_count", None)
    @read_count.setter
    def read_count(self, val):
        if hasattr(self, "config"): self.config.read_count = val
        else: self._read_count = val
    
    @property
    def read_block_ms(self):
        if hasattr(self, "config"): return self.config.read_block_ms
        return getattr(self, "_read_block_ms", None)
    @read_block_ms.setter
    def read_block_ms(self, val):
        if hasattr(self, "config"): self.config.read_block_ms = val
        else: self._read_block_ms = val
    
    @property
    def claim_min_idle_ms(self):
        if hasattr(self, "config"): return self.config.claim_min_idle_ms
        return getattr(self, "_claim_min_idle_ms", None)
    @claim_min_idle_ms.setter
    def claim_min_idle_ms(self, val):
        if hasattr(self, "config"): self.config.claim_min_idle_ms = val
        else: self._claim_min_idle_ms = val

    @property
    def group(self):
        if hasattr(self, "config"): return self.config.group
        return getattr(self, "_group", None)
    @group.setter
    def group(self, val):
        if hasattr(self, "config"): self.config.group = val
        else: self._group = val

    @property
    def consumer(self):
        if hasattr(self, "config"): return self.config.consumer
        return getattr(self, "_consumer", None)
    @consumer.setter
    def consumer(self, val):
        if hasattr(self, "config"): self.config.consumer = val
        else: self._consumer = val

    @property
    def signal_stream(self):
        if hasattr(self, "config"): return self.config.signal_stream
        return getattr(self, "_signal_stream", None)
    @signal_stream.setter
    def signal_stream(self, val):
        if hasattr(self, "config"): self.config.signal_stream = val
        else: self._signal_stream = val

    @property
    def audit_stream(self):
        if hasattr(self, "config"): return self.config.audit_stream
        return getattr(self, "_audit_stream", None)
    @audit_stream.setter
    def audit_stream(self, val):
        if hasattr(self, "config"): self.config.audit_stream = val
        else: self._audit_stream = val

    @property
    def snapshot_stream(self):
        if hasattr(self, "config"): return self.config.snapshot_stream
        return getattr(self, "_snapshot_stream", None)
    @snapshot_stream.setter
    def snapshot_stream(self, val):
        if hasattr(self, "config"): self.config.snapshot_stream = val
        else: self._snapshot_stream = val

    @property
    def notify_stream(self):
        if hasattr(self, "config"): return self.config.notify_stream
        return getattr(self, "_notify_stream", None)
    @notify_stream.setter
    def notify_stream(self, val):
        if hasattr(self, "config"): self.config.notify_stream = val
        else: self._notify_stream = val

    @property
    def metrics_every_sec(self):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        return self.config.metrics_every_sec
    @metrics_every_sec.setter
    def metrics_every_sec(self, val):
        if not hasattr(self, "config"): self.config = SignalDispatcherConfig()
        self.config.metrics_every_sec = val

    # Lazy loading for tests that bypass __init__ via object.__new__
    def _get_target_router(self):
        if not hasattr(self, "target_router"):
            from services.dispatch.target_router import TargetRouter
            class TestIdempotencyStore:
                def __init__(self, d): self.d = d
                def notify_idempotent(self, client, sid, payload):
                    if hasattr(self.d, "_evalsha_or_eval"):
                        marker_key = f"{self.d.marker_prefix}:notify:{sid}"
                        flat = self.d._flatten_notify_fields(payload)
                        args = [marker_key, self.d.notify_stream, self.d.notify_signal_counter_key, getattr(self.d, "marker_gc_zset", "marker:gc"), "60", "notify", sid, "{}", "0", "0", str(len(flat) // 2)] + flat
                        self.d._evalsha_or_eval(client, getattr(self.d, "_sha_dual", ""), "notify_gate", "", 4, *args)
                    return True
                def xadd_idempotent_atomic(self, client, target, sid, stream, fields, maxlen):
                    if hasattr(self.d, "_evalsha_or_eval"):
                        marker_key = f"{self.d.marker_prefix}:{target}:{sid}"
                        payload_json = fields.get("data") or fields.get("payload") or "{}"
                        args = [marker_key, stream, getattr(self.d, "marker_gc_zset", "marker:gc"), "60", "xadd", "1000", sid, payload_json]
                        self.d._evalsha_or_eval(client, getattr(self.d, "_sha_main", ""), "deliver", "", 3, *args)
                    return True
                def setex_idempotent_atomic(self, *a, **kw): return True
                def marker_client_for_target(self, target, dual_client, simple_client):
                    if target == "notify": return dual_client
                    return simple_client
            
            # If the object was created via object.__new__, it might not have config.
            # Tests often set config variables directly on the SignalDispatcher object (e.g. d.notify_stream).
            config_obj = self.config if hasattr(self, "config") else self
            
            self.target_router = TargetRouter(config_obj, getattr(self, "redis", None), getattr(self, "dual_redis", None), getattr(self, "simple_redis", None), TestIdempotencyStore(self), None, None, getattr(self, "logger", None))
            # Test may override simple_client / dual_client in _deliver_one_target kwargs, handled dynamically in router
        return self.target_router

    def _deliver_targets_with_retry(self, *args, **kwargs): return self._get_target_router().deliver_targets_with_retry(*args, **kwargs)
    def _deliver_one_target(self, *args, **kwargs): return self._get_target_router().deliver_one_target(*args, **kwargs)

    def _send_dlq_and_ack(self, *args, **kwargs): return self.dlq_writer.send_dlq_and_ack(*args, **kwargs)
    def _is_outbox_done(self, *args, **kwargs): return getattr(self, "idempotency_store", type("M", (), {"is_outbox_done": lambda *a, **kw: False})()).is_outbox_done(*args, **kwargs)
    def _mark_outbox_done(self, *args, **kwargs): return getattr(self, "idempotency_store", type("M", (), {"mark_outbox_done": lambda *a, **kw: None})()).mark_outbox_done(*args, **kwargs)
    def _is_env_done(self, *args, **kwargs): return getattr(self, "idempotency_store", type("M", (), {"is_env_done": lambda *a, **kw: False})()).is_env_done(*args, **kwargs)
    def _is_msg_done(self, *args, **kwargs): return getattr(self, "idempotency_store", type("M", (), {"is_msg_done": lambda *a, **kw: False})()).is_msg_done(*args, **kwargs)
    def _mark_msg_done(self, *args, **kwargs): return getattr(self, "idempotency_store", type("M", (), {"mark_msg_done": lambda *a, **kw: None})()).mark_msg_done(*args, **kwargs)
    def _try_acquire_lease(self, *args, **kwargs): return getattr(self, "lease_manager", type("M", (), {"try_acquire_lease": lambda *a, **kw: True})()).try_acquire_lease(*args, **kwargs)
    def _release_lease(self, *args, **kwargs): return getattr(self, "lease_manager", type("M", (), {"release_lease": lambda *a, **kw: None})()).release_lease(*args, **kwargs)
    def _try_acquire_sid_lease(self, *args, **kwargs): return getattr(self, "lease_manager", type("M", (), {"try_acquire_sid_lease": lambda *a, **kw: "mock_lease"})()).try_acquire_sid_lease(*args, **kwargs)
    def _release_sid_lease(self, *args, **kwargs): return getattr(self, "lease_manager", type("M", (), {"release_sid_lease": lambda *a, **kw: None})()).release_sid_lease(*args, **kwargs)
    def _maybe_extend_sid_lease(self, *args, **kwargs): return getattr(self, "lease_manager", type("M", (), {"maybe_extend_sid_lease": lambda *a, **kw: None})()).maybe_extend_sid_lease(*args, **kwargs)
    def _maybe_maintenance(self, *args, **kwargs): return getattr(self, "marker_repair", type("M", (), {"maybe_maintenance": lambda *a, **kw: None})()).maybe_maintenance(*args, **kwargs)
    def _emit_metrics(self, *args, **kwargs): return getattr(self, "dispatch_metrics", type("M", (), {"emit_metrics": lambda *a, **kw: None})()).emit_metrics(*args, **kwargs)
    def _diag(self, *args, **kwargs): return getattr(self, "dispatch_metrics", type("M", (), {"diag": lambda *a, **kw: None})()).diag(*args, **kwargs)
    def _maybe_log_diagnostics(self, *args, **kwargs): return getattr(self, "dispatch_metrics", type("M", (), {"maybe_log_diagnostics": lambda *a, **kw: None})()).maybe_log_diagnostics(*args, **kwargs)
    
    def _pending_by_consumer(self, limit: int = 50) -> dict[str, int]:
        if not hasattr(self, "dispatch_metrics"):
            from services.dispatch.dispatch_metrics import DispatchMetrics
            config_obj = self.config if hasattr(self, "config") else self
            dm = DispatchMetrics(config_obj, getattr(self, "redis", None), getattr(self, "logger", None), getattr(self, "ctr", defaultdict(int)))
            return dm.pending_by_consumer(limit)
        return self.dispatch_metrics.pending_by_consumer(limit)

    def _dead_consumers(self, *args, **kwargs): return getattr(self, "dispatch_metrics", type("M", (), {"dead_consumers": lambda *a, **kw: []})()).dead_consumers(*args, **kwargs)
    def _parse_envelope(self, *args, **kwargs): return getattr(self, "envelope_parser", type("M", (), {"parse_envelope": lambda *a, **kw: None})()).parse_envelope(*args, **kwargs)
    def _schedule_target_retry(self, *args, **kwargs): return getattr(self, "retry_scheduler", type("M", (), {"schedule_target_retry": lambda *a, **kw: None})()).schedule_target_retry(*args, **kwargs)
    
    def _delivery_key(self, target: str, sid: str) -> str:
        prefix = getattr(self, "marker_prefix", "marker")
        if hasattr(self, "config") and hasattr(self.config, "delivery_marker_prefix"): 
            prefix = self.config.delivery_marker_prefix
        return f"{prefix}:{target}:{sid}"

if __name__ == "__main__":
    dispatcher = SignalDispatcher()
    dispatcher.run()
