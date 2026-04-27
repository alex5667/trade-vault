from utils.time_utils import get_ny_time_millis
import json
import os
import time
from typing import Any, Dict, Optional, Tuple, List, Sequence, DefaultDict
from dataclasses import dataclass
from collections import defaultdict

import redis

from common.decision_trace import (
    DecisionTrace,
    Span,
    trace_enabled,
)
from services.dispatcher.lua_scripts import LuaScriptManager
from services.dispatcher.delivery_helpers import DeliveryHelpers
from services.dispatcher.config import SignalDispatcherConfig
from services.dispatcher.key_utils import KeyUtils
from services.dispatcher.lua_scripts import LuaScriptManager
from services.dispatcher.trace_writer import TraceWriter
from services.dispatcher.error_handler import ErrorHandler
from services.dispatcher.observability import sd_fail_open

from common.log import setup_logger
from common.transient import is_transient_error
from core.redis_stream_consumer import SyncRedisStreamHelper
from core.redis_client import get_redis


# =============================================================================
# Delivery error taxonomy (next level: avoid "infinite retries" for permanent bugs)
# =============================================================================
class PermanentDeliveryError(RuntimeError):
    """
    A permanent, non-retriable delivery error.

    Examples:
      - envelope missing required meta (stream name)
      - envelope missing target payload
      - required Redis client not configured

    Policy:
      - do NOT schedule retries (retries would be spam/noise)
      - write target DLQ with full context
      - write a TERMINAL marker so future replays skip this target
    """


@dataclass(frozen=True)
class PendingMsg:
    """
    Lightweight adapter for deterministic unit tests.
    Real code uses helper.pending() messages with .msg_id/.fields.
    """
    msg_id: str
    fields: Dict[str, Any]


@dataclass(frozen=True)
class DispatchDecision:
    """
    Explicit contract between business logic (_handle_one) and transport loop (ACK/lease/done).

    Rule: _handle_one MUST NOT ACK messages in Redis.
    Only the outer loop is allowed to ACK (via helper.ack), so we keep one single source of truth.
    """
    ack_now: bool
    reason: str = ""

logger = setup_logger("SignalDispatcher")


class SignalDispatcher:
    def __init__(self):
        # -----------------------------
        # Logger
        # -----------------------------
        try:
            self.logger = getattr(self, "logger", None) or logger
        except Exception:
            self.logger = None

        # -----------------------------
        # Redis (main/simple)
        # -----------------------------
        try:
            self.simple_redis = get_redis()
        except Exception:
            self.simple_redis = None

        # Unify attribute used across the file.
        self.redis = self.simple_redis

        # Dual redis moved elsewhere; keep explicit.
        self.dual_redis = None

        # -----------------------------
        # Lua Script Manager (Phase 1 refactoring)
        # -----------------------------
        try:
            self.lua_scripts = LuaScriptManager(self.redis, logger=self.logger)
            # Preload scripts for better performance
            if self.redis:
                self.lua_scripts.preload_all()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to initialize LuaScriptManager: {e}")
            self.lua_scripts = None

        # -----------------------------
        # Configuration (Phase 4+6 refactoring)
        # -----------------------------
        # -----------------------------
        # Config, Trace, Error
        # -----------------------------
        self.config = SignalDispatcherConfig.from_env()
        self._apply_config()
        self._init_state()  # init _ctr first
        self.trace_writer = TraceWriter(self.redis, self.config, self.logger)
        self.error_handler = ErrorHandler(self.logger, self._ctr)

    def _apply_config(self):
        """Apply configuration from SignalDispatcherConfig to instance attributes."""
        cfg = self.config
        
        # Streams / groups / consumer
        self.outbox_stream = cfg.outbox_stream
        self.dlq_stream = cfg.dlq_stream
        self.dlq_notify = cfg.dlq_notify
        self.dlq_signal_stream = cfg.dlq_signal_stream
        self.dlq_audit = cfg.dlq_audit
        self.dlq_manual = cfg.dlq_manual
        self.dlq_snapshot = cfg.dlq_snapshot
        self.group = cfg.group
        self.consumer = cfg.consumer
        self.read_count = cfg.read_count
        self.read_block_ms = cfg.read_block_ms
        
        # Target Streams
        self.signal_stream = cfg.signal_stream
        self.signal_maxlen = cfg.signal_maxlen
        self.audit_stream = cfg.audit_stream
        self.audit_maxlen = cfg.audit_maxlen
        self.manual_stream = cfg.manual_stream
        self.manual_maxlen = cfg.manual_maxlen
        self.mt5_plans_stream = cfg.mt5_plans_stream
        self.mt5_plans_maxlen = cfg.mt5_plans_maxlen
        self.snapshot_prefix = cfg.snapshot_prefix
        self.snapshot_ttl_sec = cfg.snapshot_ttl_sec
        
        # DONE markers
        self.msg_done_prefix = cfg.msg_done_prefix
        self.done_ttl_sec = cfg.done_ttl_sec
        

        
        # Marker namespaces
        self.marker_prefix = cfg.marker_prefix
        self.env_done_prefix = cfg.env_done_prefix
        self.marker_gc_zset = cfg.marker_gc_zset
        self.done_gc_zset = cfg.done_gc_zset
        self.delivery_marker_prefix = cfg.marker_prefix  # Alias
        
        # Lease settings
        self.sid_lease_prefix = cfg.sid_lease_prefix
        self.sid_lease_ttl_ms = cfg.sid_lease_ttl_ms
        self.sid_lease_extend_every_ms = cfg.sid_lease_extend_every_ms
        self.msg_lease_prefix = cfg.msg_lease_prefix
        self.msg_lease_ttl_ms = cfg.msg_lease_ttl_ms
        self.msg_lease_extend_every_ms = cfg.msg_lease_extend_every_ms
        
        # Retry settings
        self.retry_dedup_prefix = cfg.retry_dedup_prefix
        self.retry_base_ms = cfg.retry_base_ms
        self.retry_max_ms = cfg.retry_max_ms
        self.retry_jitter_ms = cfg.retry_jitter_ms
        self.retry_zset = cfg.retry_zset
        self.retry_pop_limit = cfg.retry_pop_limit
        self.retry_drain_every_ms = cfg.retry_drain_every_ms
        self.retry_sleep_sec = cfg.retry_sleep_sec
        self.retry_sleep_max_sec = cfg.retry_sleep_max_sec
        
        # Pending and reconciliation
        self.pending_interval_sec = cfg.pending_interval_sec
        self.pending_min_idle_ms = cfg.pending_min_idle_ms
        self.pending_claim_count = cfg.pending_claim_count
        self.claim_min_idle_ms = cfg.claim_min_idle_ms
        self.claim_count = cfg.claim_count
        self.claim_every_ms = cfg.claim_every_ms
        self.claim_budget_per_tick = cfg.claim_budget_per_tick
        
        # Maintenance
        self.orphan_repair_every_sec = cfg.orphan_repair_every_sec
        self.marker_ttl_repair_every_sec = cfg.marker_ttl_repair_every_sec
        self.marker_repair_every_sec = cfg.marker_repair_every_sec
        self.marker_repair_scan_count = cfg.marker_repair_scan_count
        self.marker_repair_batch = cfg.marker_repair_batch
        self.maintenance_every_ms = cfg.maintenance_every_ms
        self.maintenance_scan_count = cfg.maintenance_scan_count
        
        # ACK retry
        self.ack_retry_attempts = cfg.ack_retry_attempts
        self.ack_retry_delay_ms = cfg.ack_retry_delay_ms
        self.ack_retry_ttl_s = cfg.ack_retry_ttl_s
        self.ack_retry_max = cfg.ack_retry_max
        
        # Circuit breaker
        self.circuit_breaker_threshold = cfg.circuit_breaker_threshold
        self.circuit_breaker_window_sec = cfg.circuit_breaker_window_sec
        self.circuit_breaker_cooldown_sec = cfg.circuit_breaker_cooldown_sec
        self.cb_fail_threshold = cfg.cb_fail_threshold
        self.cb_open_sec = cfg.cb_open_sec

        # Diag/Full recovery
        self.env_store_prefix = cfg.env_store_prefix
        self.env_store_ttl_sec = cfg.env_store_ttl_sec
        self.maybe_done_zset = cfg.maybe_done_zset
        self.maybe_done_limit = cfg.maybe_done_limit
        self.diag_every_ms = cfg.diag_every_ms
        self.dead_consumer_idle_ms = cfg.dead_consumer_idle_ms
        self.cleanup_dead_consumers = cfg.cleanup_dead_consumers
        
        # Attempts & Metrics
        self.attempt_prefix = cfg.attempt_prefix # Note: was _attempt_prefix but used as attempt_prefix in places, check usage
        self._attempt_prefix = cfg.attempt_prefix # Alias for safe refactoring
        self._attempt_ttl_sec = cfg.attempt_ttl_sec
        self.attempt_ttl_sec = cfg.attempt_ttl_sec

        self.metrics_every_sec = cfg.metrics_every_sec
        self.outbox_diag_every_sec = cfg.outbox_diag_every_sec
        self.janitor_enabled = cfg.janitor_enabled
        self.janitor_every_sec = cfg.janitor_every_sec
        self.janitor_scan_count = cfg.janitor_scan_count
        
        # Notify
        self.notify_stream = cfg.notify_stream
        self.notify_signal_counter_key = cfg.notify_signal_counter_key
        self.notify_signal_every_n = cfg.notify_signal_every_n
        
        # Misc
        self.delivery_marker_ttl_sec = cfg.delivery_marker_ttl_sec
        self.metrics_prefix = cfg.metrics_prefix
        self.done_prefix = cfg.done_prefix
        self.outbox_maxlen = cfg.outbox_maxlen
        self.dlq_maxlen = cfg.dlq_maxlen
        self.env_state_ttl_sec = cfg.env_state_ttl_sec
        self.lock_ttl_ms = cfg.lock_ttl_ms

    def _init_state(self):
        """Initialize internal state variables."""
        self._ctr: DefaultDict[str, int] = defaultdict(int)
        self._last_diag = 0.0
        self._lease_contention = 0
        self._last_retry_drain = 0.0
        
        self._sha_main: Dict[str, str] = {}
        self._sha_dual: Dict[str, str] = {}
        self._sha_cache: Dict[Tuple[int, str], str] = {}
        
        self._last_maint_mono = 0.0
        self._scan_cursor_markers = 0
        self._scan_cursor_done = 0
        
        self._last_diag_mono = 0.0
        self._m = {}
        self._pending_claimed = 0
        
        self._pending_start_id = "0-0"
        self._last_claim_mono = 0.0
        
        self._ack_retry: Dict[Tuple[str, str], float] = {}
        self._last_ack_cleanup_mono = time.monotonic()
        
        self._cb_state: Dict[str, Tuple[int, float]] = {}
        
        self._last_consumer_cleanup = 0.0
        
        self._last_metrics_mono = 0.0
        
        self._last_janitor = 0.0
        
        self._last_repair_mono = 0.0
        self._repair_cursor = 0
        self._last_marker_repair_mono = 0.0

    def _sid_done_key(self, sid: str) -> str:
        """
        Delivery completion marker for SID (NOT msg_id).

        Keep this separated from outbox message done marker:
          - outbox done uses self._done_key(msg_id)
          - sid done uses this function

        Prefix is configurable via metrics_prefix to preserve deployment conventions.
        """
        p = str(getattr(self, "metrics_prefix", "signal_dispatcher") or "signal_dispatcher")
        return f"{p}:sid_done:{sid}"

    # ------------------------------------------------------------------
    # ACK finalization: single source of truth for outbox message lifecycle.
    #
    # Contract (CRITICAL):
    #   - _handle_one() MUST NOT ACK/XACK directly.
    #   - Dispatcher loop decides whether to ack_now based on boolean return.
    #   - Before ACK, we best-effort write "done marker" so that:
    #       * transient ACK failure => message stays pending, but future recovery
    #         will fast-path it as "done" and ACK-only.
    #   - Never raises (fail-open). Any exception is recorded via counters/logs.
    # ------------------------------------------------------------------
    def _ack_fail_open(
        self,
        helper: SyncRedisStreamHelper,
        stream: str,
        msg_id: str,
        *,
        ctr_ok: str,
        ctr_fail: str,
        where: str,
    ) -> bool:
        try:
            helper.ack(stream, str(msg_id))
            self._ctr[ctr_ok] += 1
            return True
        except Exception as exc:
            self._ctr[ctr_fail] += 1
            # Transient ACK failure:
            #   - message remains pending in the group
            #   - we remember (stream,msg_id) in a small local cache, and next ticks
            #     will attempt ACK-only (without reprocessing).
            if is_transient_error(exc):
                try:
                    self._remember_ack_retry(stream, str(msg_id))
                except Exception:
                    pass
                try:
                    logger.warning("Transient ACK failed where=%s msg=%s err=%r (will retry ack)", where, msg_id, exc)
                except Exception:
                    pass
            else:
                try:
                    logger.warning("ACK failed where=%s msg=%s err=%r", where, msg_id, exc)
                except Exception:
                    pass
            return False

    def _finalize_ack(
        self,
        helper: SyncRedisStreamHelper,
        stream: str,
        msg_id: str,
        *,
        ctr_ok: str,
        ctr_fail: str,
        where: str,
    ) -> bool:
        # Done marker is best-effort; it MUST NOT block ACK.
        #
        # Ordering is intentional:
        #   1) mark done
        #   2) ack
        #
        # Why:
        #   If ack() fails transiently, the message stays pending; later recovery
        #   will see "done==1" and do ACK-only (no re-dispatch to targets).
        try:
            self._mark_outbox_done(str(msg_id))
        except Exception:
            pass
        return self._ack_fail_open(helper, stream, msg_id, ctr_ok=ctr_ok, ctr_fail=ctr_fail, where=where)

    def _handle_env(self, *, msg_id: str, env: Dict[str, Any], sid: str) -> bool:
        """
        Process a parsed envelope.

        Contract:
          - MUST NOT ACK/XACK (outer loop owns ACK decision)
          - Returns ack_now:
              True  => outer loop may ACK current msg_id now
              False => keep pending (transient path)
        """
        lease = self._try_acquire_sid_lease(sid)
        if not lease:
            # To avoid stuck pending on this consumer: re-enqueue + let outer ACK current msg.
            # This is safe because per-target markers prevent duplicates across replays.
            self._lease_contention += 1
            try:
                self.redis.xadd(
                    self.outbox_stream,
                    {"data": json.dumps(env, ensure_ascii=False)},
                    maxlen=20000,
                    approximate=True,
                )
                return True
            except Exception:
                # worst-case: keep pending, autoclaim will recover
                return False

        dtrace = DecisionTrace.from_env(env)
        try:
            # ------------------------------------------------------------
            # MT5 Payload Extraction:
            # candidate_emit_pipeline_v2 embeds mt5_payload with special key.
            # Extract it and inject into targets for delivery.
            # ------------------------------------------------------------
            mt5_payload = env.pop("__mt5_payload__", None)
            if mt5_payload is not None and isinstance(mt5_payload, dict):
                try:
                    if "targets" not in env or not isinstance(env["targets"], dict):
                        env["targets"] = {}
                    env["targets"]["mt5_plan"] = mt5_payload
                except Exception:
                    pass
            
            self._deliver_targets_with_retry(env, sid, _trace=dtrace)
            # Emit success diag
            self.trace_writer.emit_diag(dtrace, stage="dispatch_ok")
            # Persist meta for sidecar (if configured)
            self.trace_writer.persist_trace_meta(sid=sid, trace=dtrace)
            return True
        except Exception as exc:
            # Any unexpected error => retry whole env via delayed scheduler (safe default).
            logger.error("Unexpected error sid=%s msg=%s err=%s", sid, msg_id, exc, exc_info=True)
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
            except Exception:
                return False
            finally:
                if lease:
                    self._release_sid_lease(sid, lease)

    # ------------------------------------------------------------------
    # Single message processor (used for BOTH new ">" messages and claimed pending)
    #
    # This is the "ещё выше" step:
    #   - removes duplication between main loop and _maybe_claim_pending()
    #   - makes invariants explicit and testable:
    #       * ack-retry-only happens before any lease/processing
    #       * lease is always released (finally)
    #       * done-fastpath => ACK-only, never re-dispatch
    #       * _handle_one never ACKs; it returns ack_now
    # ------------------------------------------------------------------
    def _process_outbox_message(
        self,
        helper: SyncRedisStreamHelper,
        *,
        stream: str,
        msg_id: str,
        fields: Dict[str, Any],
        where: str,
        ack_ctr_ok: str,
        ack_ctr_fail: str,
        handle_transient_ctr: str,
        handle_failed_ctr: str,
    ) -> None:
        if not msg_id:
            return

        # NOTE: do NOT parse envelope if done-fastpath triggers.
        # This keeps recovery cheap and avoids JSON parsing on hot pending drain.

        # If ACK failed earlier, try ACK-only first (idempotent).
        # IMPORTANT: do NOT re-run _handle_one for such msg_id — it can double-send.
        try:
            if self._try_ack_retry_only(helper, stream, msg_id):
                return
        except Exception:
            # fail-open: proceed with normal path
            pass

        # Prevent concurrent work on the same msg_id across dispatcher instances.
        if not self._try_acquire_lease(str(msg_id)):
            return

        try:
            # done-fastpath: if we already finalized side-effects but ACK failed transiently earlier
            # => ACK-only, never re-dispatch.
            if self._is_outbox_done(str(msg_id)):
                self._finalize_ack(
                    helper,
                    stream,
                    msg_id,
                    ctr_ok=ack_ctr_ok,
                    ctr_fail=ack_ctr_fail,
                    where=f"{where}_done_fastpath",
                )
                return

            # --------------------------------------------------------------
            # Stage 0: parse envelope here (single place) to enforce:
            #   - bad envelope => DLQ+ACK via lua (atomic)
            #   - missing sid  => DLQ+ACK via lua (atomic)
            # This keeps _handle_env pure (no ack / no dlq).
            # --------------------------------------------------------------
            env = None
            try:
                env = self._parse_envelope(fields)
            except Exception:
                env = None

            if not env:
                # Atomic DLQ + ACK (lua). If it fails => keep pending (return).
                ok = False
                try:
                    ok = bool(self._send_dlq_and_ack(str(msg_id), fields, reason="bad_envelope"))
                except Exception:
                    ok = False
                if ok:
                    # mark done marker for pending-recovery safety (best-effort)
                    try:
                        self._mark_outbox_done(str(msg_id))
                    except Exception:
                        pass
                return

            sid = str(env.get("sid") or "")
            if not sid:
                ok = False
                try:
                    ok = bool(self._send_dlq_and_ack(str(msg_id), env, reason="missing_sid"))
                except Exception:
                    ok = False
                if ok:
                    try:
                        self._mark_outbox_done(str(msg_id))
                    except Exception:
                        pass
                return

            ack_now = False
            try:
                # Stage 1+: handle parsed env (no ACK inside)
                ack_now = bool(self._handle_env(msg_id=str(msg_id), env=env, sid=sid))
            except Exception as exc:
                self.error_handler.handle(
                    exc, 
                    context=where, 
                    msg_id=str(msg_id), 
                    ctr_transient=handle_transient_ctr, 
                    ctr_fatal=handle_failed_ctr,
                    log_transient=False # Main loop relies on metrics, not warnings for transient
                )
                return

            if ack_now:
                self._finalize_ack(
                    helper,
                    stream,
                    msg_id,
                    ctr_ok=ack_ctr_ok,
                    ctr_fail=ack_ctr_fail,
                    where=where,
                )
        finally:
            # Lease must always be released; otherwise a transient handler error
            # can "lock" the msg_id and stall progress.
            self._release_lease(str(msg_id))

    # -----------------------------
    # Redis access (single place)
    # -----------------------------
    def _r(self) -> Any:
        """
        Centralized redis accessor:
          - self.redis is the canonical attribute in this file
          - fall back to self.simple_redis if some older init path forgot to set self.redis
        """
        return getattr(self, "redis", None) or getattr(self, "simple_redis", None)

    def _is_msg_done(self, msg_id: str) -> bool:
        """
        MSG-level idempotency marker for ACK-failure recovery.
        Если marker есть => side-effects уже выполнены, можно делать ACK-only.
        FAIL-OPEN: если Redis шалит -> считаем NOT done (пусть остается pending).
        """
        if not msg_id:
            return False
        r = self._r()
        if r is None:
            return False
        try:
            v = r.get(self._msg_done_key(msg_id))
            if v in (None, "", b""):
                return False
            if isinstance(v, bytes):
                v = v.decode("utf-8", "ignore")
            return str(v).strip() == "1"
        except Exception:
            return False

    # Back-compat alias (если где-то в коде осталось старое имя)
    def _is_outbox_done(self, msg_id: str) -> bool:
        return self._is_msg_done(msg_id)

    def _handle_one(self, msg_id: str, fields: Dict[str, Any]) -> bool:
        """
        Parse envelope from fields and delegate to _handle_env.
        
        Returns:
            True if message should be ACKed now, False to keep pending.
        """
        # Parse envelope
        env = None
        try:
            env = self._parse_envelope(fields)
        except Exception:
            env = None
        
        if not env:
            # Bad envelope -> DLQ
            try:
                self._send_dlq_and_ack(str(msg_id), fields, reason="bad_envelope")
            except Exception:
                pass
            return True  # ACK to remove from pending
        
        sid = str(env.get("sid") or "")
        if not sid:
            # Missing SID -> DLQ
            try:
                self._send_dlq_and_ack(str(msg_id), env, reason="missing_sid")
            except Exception:
                pass
            return True  # ACK to remove from pending
        
        # Delegate to _handle_env
        return self._handle_env(msg_id=str(msg_id), env=env, sid=sid)

    # ---------------------------------------------------------------------
    # NEW: unified processing for "new" (">") messages
    # ---------------------------------------------------------------------
    def _process_new_batch(self, helper: Any, messages: Sequence[Any]) -> None:
        """
        Processes *new* messages from XREADGROUP (id=">") safely and deterministically.

        Fixes several production-grade issues observed in the current loop:
          1) unreachable code due to misplaced `continue`
          2) `ack_now` scope bug (ACK only last message / UnboundLocalError)
          3) lease release not guaranteed (must be in finally per msg_id)
          4) idempotency: if done-marker exists -> ACK only, never re-dispatch
          5) mark done BEFORE ACK (idempotent recovery if ACK transiently fails)

        Contract:
          - never raises (fail-open)
          - per-message isolation: one broken message does not break the batch
        """
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

    # ---------------------------------------------------------------------
    # ACK helpers (single source of truth)
    # ---------------------------------------------------------------------
    def _ack_outbox_fail_open(self, helper: Any, stream: str, msg_id: str, *, where: str) -> bool:
        """
        ACK wrapper with consistent transient handling.
        Returns True if ACK succeeded.
        Fail-open: never raises.
        """
        try:
            helper.ack(stream, msg_id)
            return True
        except Exception as exc:
            # keep counters best-effort (must not raise)
            try:
                self._ctr["ack_failed"] += 1
            except Exception:
                pass
            if is_transient_error(exc):
                try:
                    self._remember_ack_retry(stream, msg_id)
                except Exception:
                    pass
                try:
                    self.logger.warning("Transient ACK failed (%s) %s: %s (will retry)", where, msg_id, exc)
                except Exception:
                    pass
            else:
                try:
                    self.logger.warning("ACK failed (%s) %s: %s", where, msg_id, exc)
                except Exception:
                    pass
            return False



    def _attempt_key(self, msg_id: str) -> str:
        return f"{self._attempt_prefix}:{msg_id}"

    def _incr_attempt(self, msg_id: str) -> int:
        # replaced below (sid-based)
        return 1

    def _incr_attempt_sid(self, sid: str) -> int:
        k = f"{self._attempt_prefix}:{sid}"
        try:
            n = int(self.redis.incr(k))
            if n == 1:
                self.redis.expire(k, self._attempt_ttl_sec)
            return n
        except Exception:
            # conservative: treat as transient
            raise

    # ------------------------------------------------------------------
    # Delivery markers (per-target) and "done" markers (msg_id vs sid)
    #
    # IMPORTANT:
    #   - marker == "target delivered" (notify/signal_stream/audit/...)
    #   - done(msg_id) == "this outbox message side-effects finished; safe to ACK-only on recovery"
    #   - done(sid)    == "all targets delivered for this SID" (optional higher-level marker)
    #
    # Do NOT mix keyspaces, otherwise you can:
    #   - skip deliveries due to collisions
    #   - ACK-only wrong messages
    # ------------------------------------------------------------------

    def _marker_key(self, target: str, sid: str) -> str:
        """Per-target delivery marker key (idempotency)."""
        return DeliveryHelpers.marker_key(self.marker_prefix, target, sid)

    # Back-compat: some call sites in the file use _delivery_key(...)
    # Keep it as an alias to prevent AttributeError and to clarify intent.
    def _delivery_key(self, target: str, sid: str) -> str:
        return DeliveryHelpers.delivery_key(self.marker_prefix, target, sid)

    def _lease_key(self, msg_id: str) -> str:
        return KeyUtils.lease_key(self.msg_lease_prefix, msg_id)

    # NOTE: _done_key(...) existed historically and was (incorrectly) used for BOTH:
    #   - outbox msg_id done markers (value "1")
    #   - sid done markers (value timestamp)
    # Keep it for backward compatibility only. New code must NOT write to it.
    def _done_key(self, sid: str) -> str:
        return KeyUtils.done_key(self.done_prefix, sid)

    def _outbox_done_key(self, msg_id: str) -> str:
        """
        Outbox message done marker (msg_id-based).
        Value is "1".
        This key is used ONLY to accelerate pending recovery after transient ACK failure.
        """
        return KeyUtils.outbox_done_key(f"{self.done_prefix}:msg", msg_id)

    def _sid_done_key(self, sid: str) -> str:
        """
        SID done marker (sid-based).
        Value is timestamp (ms).
        This key must never share key-space with _outbox_done_key.
        """
        return KeyUtils.sid_done_key(f"{self.done_prefix}:sid", sid)

    def _msg_done_key(self, msg_id: str) -> str:
        return KeyUtils.outbox_done_key(self.msg_done_prefix, msg_id)

    def _env_done_key(self, sid: str) -> str:
        return KeyUtils.env_done_key(self.env_done_prefix, sid)

    def _retry_dedup_key(self, target: str, sid: str) -> str:
        return f"{self.retry_dedup_prefix}:{target}:{sid}"



    def _deliver_targets_with_retry(
        self,
        env: Dict[str, Any],
        sid: str,
        *,
        targets: Optional[List[str]] = None,
        base_attempts: Optional[Dict[str, int]] = None,
        _trace: Optional[DecisionTrace] = None,
    ) -> None:
        targets_obj = env.get("targets") or {}
        meta = env.get("meta") or {}
        to_process = targets or self._targets_list(env)
        dual_client = self.dual_redis
        simple_client = self.simple_redis
        attempts_obj = env.setdefault("attempts", {})
        if not isinstance(attempts_obj, dict):
            attempts_obj = {}
            env["attempts"] = attempts_obj
        any_failure = False

        def _trace_delivery(*, target: str, ok: bool, reason_code: str = "", err: str = "") -> None:
            if not _trace:
                return
            try:
                _trace.add(
                    where="delivery",
                    name=f"delivery_{target}",
                    ok=bool(ok),
                    veto=False,
                    reason_code=str(reason_code or ("OK" if ok else "DELIVERY_ERROR")),
                    etype="gate",
                    extra={"err": str(err)} if err else None,
                )
            except TypeError:
                try:
                    _trace.add(where="delivery", name=f"delivery_{target}", ok=bool(ok), metrics={"err": str(err)} if err else None)
                except Exception:
                    pass
            except Exception:
                pass

        for idx, t in enumerate(to_process):
            target = str(t)
            marker_client = self._marker_client_for_target(target, dual_client, simple_client) or self.redis
            try:
                if self._marker_exists(marker_client, target, sid):
                    continue
            except Exception:
                pass

            if idx == 0 and isinstance(base_attempts, dict) and "__forced__" in base_attempts:
                attempt = int(base_attempts.get("__forced__") or 0)
            else:
                attempt = int((base_attempts or {}).get(target, attempts_obj.get(target, 0)) or 0) + 1
            attempts_obj[target] = int(attempt)

            try:
                self._deliver_one_target(
                    env=env,
                    sid=sid,
                    target=target,
                    targets_obj=targets_obj,
                    meta=meta,
                    dual_client=dual_client,
                    simple_client=simple_client,
                )
                _trace_delivery(target=target, ok=True)
            except Exception as e:
                any_failure = True
                # schedule retry
                self._schedule_target_retry(
                    target=target,
                    sid=sid,
                    env=env,
                    attempt=int(attempt),
                    last_error=str(e),
                )
                if not is_transient_error(e):
                    try:
                        self._send_target_dlq(target, sid, env, reason="target_delivery_error", err=str(e))
                    except Exception:
                        pass
                _trace_delivery(target=target, ok=False, reason_code="DELIVERY_ERROR", err=str(e))

        if not any_failure:
            try:
                self.redis.set(self._env_done_key(sid), "1", ex=int(self.delivery_marker_ttl_sec), nx=True)
            except Exception:
                pass

    def _deliver_one_target(
        self,
        *,
        env: Dict[str, Any],
        sid: str,
        target: str,
        targets_obj: Dict[str, Any],
        meta: Dict[str, Any],
        dual_client: Any,
        simple_client: Any,
    ) -> None:
        client = self._marker_client_for_target(target, dual_client, simple_client) or self.redis

        # Resolve payload key based on target type
        payload_key = target
        if target == "signal_stream":
            payload_key = "signal_stream_payload"
        elif target == "audit":
            payload_key = "audit_payload"
        elif target == "manual":
            payload_key = "manual_payload"
        elif target == "snapshot":
            payload_key = "snapshot_payload"

        payload = targets_obj.get(payload_key)

        # ✅ FINAL PAYLOAD VALIDATION: Ensure data/payload is properly closed
        if payload and isinstance(payload, dict):
            # Validate delta/z preservation for delta_spike signals
            if payload.get("type") == "delta_spike" or "delta" in payload:
                delta_val = payload.get("delta")
                delta_z_val = payload.get("delta_z") or payload.get("z")

                # CRITICAL: Ensure delta/z are not zeroed out before MIN-CONF check
                if delta_val is not None and delta_z_val is not None:
                    # Log preservation for audit
                    if self.logger:
                        self.logger.debug(
                            f"✅ [{sid}] Payload closed: delta={delta_val:.4f}, z={delta_z_val:.4f}, target={target}"
                        )
                else:
                    # WARN if delta/z missing in delta_spike payload
                    if self.logger:
                        self.logger.warning(
                            f"⚠️ [{sid}] Missing delta/z in payload: delta={delta_val}, z={delta_z_val}, target={target}"
                        )

        if target == "notify":
            if not self.notify_stream:
                raise PermanentDeliveryError("missing_notify_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_notify_payload")
            # Wrap in data for compatibility
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False)}
            
            if not self._notify_idempotent(client, sid=sid, payload=fields):
                raise PermanentDeliveryError("notify_failed")
            return

        if target == "signal_stream":
            stream = meta.get("signal_stream") or self.signal_stream
            if not stream:
                raise PermanentDeliveryError("missing_signal_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_signal_payload")
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False)}
            
            if not self._xadd_idempotent_atomic(
                client, target="signal_stream", sid=sid, stream=stream, fields=fields, maxlen=self.signal_maxlen
            ):
                raise PermanentDeliveryError("signal_stream_failed")
            return

        if target == "audit":
            stream = meta.get("audit_stream") or self.audit_stream
            if not stream:
                raise PermanentDeliveryError("missing_audit_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_audit_payload")
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            # Use 'payload' field for audit to align with AsyncSignalPublisher and tracker conventions
            fields = {"payload": json.dumps(wrapped_payload, ensure_ascii=False)}

            if not self._xadd_idempotent_atomic(
                client, target="audit", sid=sid, stream=stream, fields=fields, maxlen=self.audit_maxlen
            ):
                raise PermanentDeliveryError("audit_failed")
            return

        if target == "manual":
            stream = meta.get("manual_stream") or self.manual_stream
            if not stream:
                raise PermanentDeliveryError("missing_manual_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_manual_payload")
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False)}

            if not self._xadd_idempotent_atomic(
                client, target="manual", sid=sid, stream=stream, fields=fields, maxlen=self.manual_maxlen
            ):
                raise PermanentDeliveryError("manual_failed")
            return

        if target == "mt5_plan":
            if not self.mt5_plans_stream:
                raise PermanentDeliveryError("missing_mt5_plans_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_mt5_plan_payload")
            
            # mt5_bridge expects: {"payload": JSON({"plan": ...})}
            wrapper = {"plan": payload}
            payload_json = json.dumps(wrapper, ensure_ascii=False)
            fields = {"payload": payload_json}
            
            if not self._xadd_idempotent_atomic(
                client, target="mt5_plan", sid=sid, stream=self.mt5_plans_stream, fields=fields, maxlen=self.mt5_plans_maxlen
            ):
                raise PermanentDeliveryError("mt5_plan_failed")
            return

        if target == "snapshot":
            # snapshot -> SET
            if not self.snapshot_prefix:
                return
            key = f"{self.snapshot_prefix}:{sid}"
            val = json.dumps(payload, ensure_ascii=False) if payload is not None else ""
            if not self._setex_idempotent_atomic(
                client, target="snapshot", sid=sid, key=key, ttl_sec=self.snapshot_ttl_sec, value_json=val
            ):
                raise PermanentDeliveryError("snapshot_failed")
            return





    def _marker_client_for_target(self, target: str, dual_client: Any, simple_client: Any) -> Any:
        """
        Marker-location must match _marker_exists() checks to prevent duplicates.
        """
        if target in ("notify", "manual"):
            return dual_client or self.redis
        if target == "signal_stream":
            return simple_client or self.redis
        return self.redis

    def _env_req_key(self, sid: str) -> str:
        return KeyUtils.env_req_key(self.env_store_prefix, sid)

    def _targets_list(self, env: Dict[str, Any]) -> List[str]:
        t = env.get("targets") or {}
        # canonical list for "done"
        out: List[str] = []
        if t.get("notify"): out.append("notify")
        if t.get("signal_stream_payload"): out.append("signal_stream")
        if t.get("audit_payload"): out.append("audit")
        if t.get("manual_payload"): out.append("manual")
        if t.get("mt5_plan"): out.append("mt5_plan")
        if t.get("snapshot_payload") or t.get("snapshot"): out.append("snapshot")
        return out

    def _ensure_script(self, client: Any, cache: Dict[str, str], name: str, script: str) -> str:
        sha = cache.get(name)
        if sha:
            return sha
        sha = client.script_load(script)
        cache[name] = sha
        return sha



    def _marker_exists(self, client: Any, target: str, sid: str) -> bool:
        try:
            return bool(client.get(self._delivery_key(target, sid)))
        except Exception:
            return False


    def _retry_delay_ms(self, attempt: int) -> int:
        # exp backoff with jitter
        return DeliveryHelpers.calculate_retry_delay(
            attempt - 1,  # DeliveryHelpers uses 0-indexed attempts
            base_ms=self.retry_base_ms,
            max_ms=self.retry_max_ms,
            jitter_ms=self.retry_jitter_ms
        )

    def _retry_dedup_key(self, target: str, sid: str) -> str:
        return DeliveryHelpers.retry_dedup_key(self.retry_dedup_prefix, target, sid)

    def _schedule_target_retry(self, *, target: str, sid: str, env: Dict[str, Any], attempt: int, last_error: str) -> None:
        if attempt >= self.max_attempts:
            # Target-specific DLQ is more useful than generic DLQ here:
            #   - preserves target name
            #   - includes env + error for diagnostics
            try:
                self._send_target_dlq(
                    target=str(target),
                    sid=str(sid),
                    env=env if isinstance(env, dict) else {},
                    reason="target_max_attempts",
                    err=str(last_error),
                )
            except Exception:
                pass
            return
        delay = self._retry_delay_ms(attempt)

        # Next level: retry dedup per (target,sid) to prevent ZSET explosions.
        # Typical scenario:
        #   - transient Redis blip throws multiple times in same tick
        #   - without dedup we schedule N identical retries
        #
        # Policy:
        #   - set NX with PX ~ delay window
        #   - if already scheduled, skip silently (fail-open)
        try:
            dk = self._retry_dedup_key(target, sid)
            ok = self.redis.set(dk, "1", nx=True, px=int(delay) + 1000)
            if not ok:
                self._ctr["retry_dedup_hit"] += 1
                return
        except Exception:
            pass

        payload = {
            "sid": sid,
            "target": target,
            "attempt": attempt,
            "ts_ms": get_ny_time_millis(),
            "env": env,
            "last_error": last_error,
        }
        member = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        score = get_ny_time_millis() + delay
        self.redis.zadd(self.retry_zset, {member: score})

    def _drain_retries_best_effort(self) -> None:
        now = time.monotonic()
        if (now - self._last_retry_drain) * 1000 < self.retry_drain_every_ms:
            return
        self._last_retry_drain = now
        try:
            now_ms = get_ny_time_millis()
            items = self.lua_scripts.execute("zpop_due", keys=[self.retry_zset], args=[str(now_ms), str(self.retry_pop_limit)])
        except Exception:
            return
        if not items:
            return
        for raw in items:
            try:
                obj = json.loads(raw)
                sid = str(obj.get("sid") or "")
                target = str(obj.get("target") or "")
                attempt = int(obj.get("attempt") or 0)
                env = obj.get("env") or {}
                if not sid or not target or not isinstance(env, dict):
                    continue
                self._deliver_targets_with_retry(env, sid, targets=[target], base_attempts={"__forced__": attempt})
            except Exception:
                continue


    def _send_target_dlq(self, target: str, sid: str, env: Dict[str, Any], *, reason: str, err: str) -> None:
        stream = DeliveryHelpers.get_dlq_stream_for_target(
            target,
            dlq_notify=self.dlq_notify,
            dlq_signal_stream=self.dlq_signal_stream,
            dlq_audit=self.dlq_audit,
            dlq_manual=self.dlq_manual,
            dlq_snapshot=self.dlq_snapshot,
            dlq_default=self.dlq_stream
        )
        DeliveryHelpers.send_to_dlq(
            redis_client=self.redis,
            dlq_stream=stream,
            target=target,
            sid=sid,
            env=env,
            reason=reason,
            error=err,
            logger=logger
        )

    def _maybe_diag(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if (now - self._last_diag_mono) * 1000 < self.diag_every_ms:
            return
        self._last_diag_mono = now
        try:
            pend = helper.pending_len(self.outbox_stream)
            by_cons = helper.pending_by_consumer(self.outbox_stream)
        except Exception:
            pend, by_cons = 0, {}

        # sampled metrics log (structured)
        payload = {
            "pending": int(pend),
            "pending_by_consumer": dict(by_cons or {}),
            "lease_contention": int(self._lease_contention),
            "claimed_pending": int(self._pending_claimed),
        }
        logger.info("metrics %s", payload)
        self._lease_contention = 0
        self._pending_claimed = 0

    def _maybe_maintenance(self) -> None:
        now = time.monotonic()
        if (now - self._last_maint_mono) * 1000 < self.maintenance_every_ms:
            return
        self._last_maint_mono = now
        # best-effort scan: fix no-ttl keys + remove truly orphaned (no ttl and very old)
        self._scan_cursor_markers = self._maint_scan_prefix(f"{self.marker_prefix}:", self._scan_cursor_markers)
        self._scan_cursor_done = self._maint_scan_prefix(f"{self.done_prefix}:", self._scan_cursor_done)

    def _maint_scan_prefix(self, prefix: str, cursor: int) -> int:
        try:
            cursor2, keys = self.redis.scan(cursor=cursor, match=f"{prefix}*", count=self.maintenance_scan_count)
        except Exception:
            return cursor
        now_ms = get_ny_time_millis()
        ttl_cap = int(self.delivery_marker_ttl_sec)
        for k in keys or []:
            try:
                ttl = int(self.redis.ttl(k))
            except Exception:
                continue
            # -2: missing, -1: no expire
            if ttl == -1:
                # try to delete if value is old, otherwise set expire
                try:
                    v = self.redis.get(k)
                    v_ms = int(v) if v and str(v).isdigit() else 0
                except Exception:
                    v_ms = 0
                if v_ms > 0 and (now_ms - v_ms) > (ttl_cap * 1000 * 2):
                    try:
                        self.redis.delete(k)
                    except Exception:
                        pass
                else:
                    try:
                        self.redis.expire(k, ttl_cap)
                    except Exception:
                        pass
            elif ttl > (ttl_cap * 2):
                # cap overly large TTLs (misconfig)
                try:
                    self.redis.expire(k, ttl_cap)
                except Exception:
                    pass
        return int(cursor2 or 0)

    def _sid_lease_key(self, sid: str) -> str:
        return f"{self.sid_lease_prefix}:{sid}"

    def _try_acquire_sid_lease(self, sid: str) -> Optional[str]:
        """
        Token-based lease. Возвращает token если взяли lease, иначе None.
        """
        import uuid
        token = uuid.uuid4().hex
        key = self._sid_lease_key(sid)
        ok = self.redis.set(key, token, nx=True, px=int(self.sid_lease_ttl_ms))
        if ok:
            self._ctr["sid_lease_acquired"] += 1
            return token
        return None

    def _release_sid_lease(self, sid: str, token: str) -> None:
        try:
            self.lua_scripts.execute("release_lease", keys=[self._sid_lease_key(sid)], args=[token])
        except Exception:

            pass
    def _maybe_extend_sid_lease(self, sid: str, token: str, last_extend_ms: int) -> int:
        """
        Продлеваем lease каждые sid_lease_extend_every_ms (best-effort).
        """
        now_ms = get_ny_time_millis()
        if now_ms - int(last_extend_ms) < int(self.sid_lease_extend_every_ms):
            return last_extend_ms
        try:
            ok = self.lua_scripts.execute(
                "extend_lease",
                keys=[self._sid_lease_key(sid)],
                args=[token, str(int(self.sid_lease_ttl_ms))],
            )
            if int(ok or 0) == 1:
                self._ctr["sid_lease_extended"] += 1
                return now_ms
        except Exception:
            pass
        return last_extend_ms



    def _xadd_idempotent_atomic(self, client: Any, *, target: str, sid: str, stream: str,
                               fields: Dict[str, Any], maxlen: int) -> bool:
        """
        B) FIX (строго): marker и XADD атомарно в Lua. Без loss и без параллельных дублей.
        """
        if not stream:
            return True
        marker = self._marker_key(target, sid)
        # flatten fields
        argv: List[Any] = [str(self.delivery_marker_ttl_sec), str(maxlen)]
        for k, v in fields.items():
            argv.append(str(k))
            argv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = self.lua_scripts.execute("xadd_and_mark", keys=[marker, stream], args=argv, client=client)
        if not res:
            return False
        code = int(res[0])
        if code == 1:
            return True
        if code == 0:
            return True  # already delivered (idempotent skip)
        # -1/-2 => transient-ish delivery infra failure
        raise RuntimeError(f"xadd_and_mark_failed code={code} target={target}")

    def _setex_idempotent_atomic(self, client: Any, *, target: str, sid: str, key: str,
                                ttl_sec: int, value_json: str) -> bool:
        if not key:
            return True
        marker = self._marker_key(target, sid)
        res = self.lua_scripts.execute(
            "setex_and_mark",
            keys=[marker, key],
            args=[str(self.delivery_marker_ttl_sec), str(int(ttl_sec)), value_json],
        )
        if not res:
            return False
        code = int(res[0])
        if code in (0, 1):
            return True
        raise RuntimeError(f"setex_and_mark_failed code={code} target={target}")

    def _try_acquire_lease(self, msg_id: str) -> bool:
        """
        True => можно обрабатывать.
        False => уже кто-то обрабатывает (не ACK, оставить pending).
        """
        try:
            ok = self.redis.set(self._lease_key(msg_id), "1", nx=True, px=self.msg_lease_ttl_ms)
            if ok:
                return True
        except Exception:
            # если Redis совсем плохо — считаем transient и не трогаем message
            return False
        self._ctr["lease_contention"] += 1
        return False

    def _release_lease(self, msg_id: str) -> None:
        try:
            self.redis.delete(self._lease_key(msg_id))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Outbox done marker (msg_id) helpers
    # ------------------------------------------------------------------

    def _mark_outbox_done(self, msg_id: str) -> None:
        """
        Mark outbox message as "done" so pending recovery can do ACK-only.

        IMPORTANT:
          - msg_id-based keyspace: done_prefix:msg:{msg_id}
          - value is "1"
          - TTL uses done_ttl_sec
        """
        try:
            # Message-level done marker (used only for ACK failure recovery).
            self.redis.setex(self._msg_done_key(msg_id), self.done_ttl_sec, "1")
            # Backward-compat write (optional, keep for safe rollout).
            # If you want to stop writing legacy markers, set:
            #   SIGNAL_OUTBOX_WRITE_LEGACY_DONE=0
            if os.getenv("SIGNAL_OUTBOX_WRITE_LEGACY_DONE", "1").lower() not in {"0", "false", "no"}:
                try:
                    self.redis.setex(self._done_key(msg_id), self.done_ttl_sec, "1")
                except Exception:
                    pass
        except Exception as e:
            sd_fail_open(
                getattr(self, "logger", None),
                key="mark_outbox_done_error",
                err=e,
                incr_fn=getattr(self, "_incr", None),
                metric_key=f"{self.metrics_prefix}:mark_outbox_done_errors_total",
            )

    def _is_outbox_done(self, msg_id: str) -> bool:
        """
        Idempotency marker for pending-recovery:
          - if done==1 we must ACK quickly and never re-dispatch to targets.

        FAIL-OPEN:
          - if Redis errors => behave as NOT done (safer; avoids stuck pending).

        Back-compat:
          - also checks legacy _done_key(msg_id) which older versions used for msg_id markers.
            We ONLY treat legacy value "1" as msg-done; timestamps are ignored.
        """
        if not msg_id:
            return False
        r = self._r()
        if r is None:
            return False
        try:
            v = r.get(self._outbox_done_key(msg_id))
            if v in (None, "", b""):
                # legacy fallback
                v2 = r.get(self._done_key(msg_id))
                if v2 in (None, "", b""):
                    return False
                if isinstance(v2, bytes):
                    v2 = v2.decode("utf-8", "ignore")
                return str(v2).strip() == "1"
            if isinstance(v, bytes):
                v = v.decode("utf-8", "ignore")
            return str(v).strip() == "1"
        except Exception:
            return False

    def _is_env_done(self, sid: str) -> bool:
        """
        ENV-level idempotency marker:
          - if env done => this SID has finished dispatch (or terminal-DLQ)
            and we should ACK any repeated outbox messages for this SID quickly.
        """
        if not sid:
            return False
        r = self._r()
        if r is None:
            return False
        try:
            v = r.get(self._env_done_key(sid))
            if v in (None, "", b""):
                # Backward-compat: some versions used done_prefix for SID too.
                v = r.get(self._done_key(sid))
            if v in (None, "", b""):
                return False
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Message batch handling (extracted for correctness + unit tests)
    # ------------------------------------------------------------------
    def _handle_read_messages(self, helper: SyncRedisStreamHelper, messages: Any) -> None:
        """
        Process XREADGROUP ">" batches.

        Guarantees:
          - NO unconditional 'continue' that skips processing
          - ACK is per-message (not "last msg in batch")
          - If _handle_one() succeeded (ack_now=True) we set msg-done marker BEFORE ACK attempt:
              -> if ACK fails transiently and message becomes pending,
                 pending recovery will ACK-only and never re-dispatch side effects.
          - Fail-open on Redis/ACK issues; retry hooks remain in place.
        """
        for stream, items in messages or []:
            for m in items or []:
                msg_id = getattr(m, "msg_id", "") or ""
                fields = getattr(m, "fields", {}) or {}
                if not msg_id:
                    continue

                # If ACK failed earlier for this msg_id, do ACK-only first.
                if self._try_ack_retry_only(helper, stream, str(msg_id)):
                    continue

                # Fast-path: if msg already completed (side effects committed) -> ACK-only.
                # This is critical for recovering transient ACK failures without duplicates.
                if self._is_outbox_done(str(msg_id)):
                    try:
                        helper.ack(stream, str(msg_id))
                        self._ctr["acked_done_fastpath"] += 1
                    except Exception as exc:
                        self._ctr["ack_failed_done_fastpath"] += 1
                        if is_transient_error(exc):
                            self._remember_ack_retry(stream, str(msg_id))
                            logger.warning("Transient ACK failed (done-fastpath) %s: %s (will retry ack)", msg_id, exc)
                        else:
                            logger.warning("ACK failed (done-fastpath) %s: %s", msg_id, exc)
                    continue

                # Lease on msg_id: prevents concurrent processing across dispatcher processes.
                if not self._try_acquire_lease(str(msg_id)):
                    continue

                ack_now = False
                try:
                    ack_now = bool(self._handle_one(str(msg_id), fields))
                except Exception as exc:  # noqa: BLE001
                    # If this is transient, leave it pending (do NOT ack; claim_pending will recover)
                    if is_transient_error(exc):
                        self._ctr["handle_transient"] += 1
                    else:
                        self._ctr["handle_failed"] += 1
                        logger.error("Failed msg %s: %s", msg_id, exc, exc_info=True)
                    ack_now = False
                finally:
                    self._release_lease(str(msg_id))

                if not ack_now:
                    # keep pending
                    continue

                # Mark message done BEFORE ACK attempt (idempotency for pending-recovery).
                try:
                    self._mark_outbox_done(str(msg_id))
                except Exception:
                    pass

                try:
                    helper.ack(stream, str(msg_id))
                    self._ctr["acked"] += 1
                except Exception as exc:  # noqa: BLE001
                    self._ctr["ack_failed"] += 1
                    if is_transient_error(exc):
                        self._remember_ack_retry(stream, str(msg_id))
                        logger.warning("Transient ACK failed %s: %s (will retry ack)", msg_id, exc)
                    else:
                        logger.warning("ACK failed %s: %s", msg_id, exc)

    def _cb_allow(self, target: str) -> bool:
        fails, open_until = self._cb_state.get(target, (0, 0.0))
        now = time.monotonic()
        if open_until and now < open_until:
            self._ctr[f"cb_open:{target}"] += 1
            return False
            return True

    def _cb_on_success(self, target: str) -> None:
        self._cb_state[target] = (0, 0.0)

    def _cb_on_fail(self, target: str) -> None:
        fails, _ = self._cb_state.get(target, (0, 0.0))
        fails += 1
        if fails >= self.cb_fail_threshold:
            self._cb_state[target] = (fails, time.monotonic() + self.cb_open_sec)
        else:
            self._cb_state[target] = (fails, 0.0)

    def _sleep_retry(self, attempt: int) -> None:
        # bounded linear-ish backoff (без рандома, чтобы не плодить зависимости)
        d = min(self.retry_sleep_max_sec, max(self.retry_sleep_sec, self.retry_sleep_sec * float(attempt)))
        time.sleep(d)



    def _diag(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag < self.outbox_diag_every_sec:
            return
        self._last_diag = now
        try:
            info = helper.pending_details(self.outbox_stream)
            pending = int(info.get("pending", 0) or 0)
            cons = info.get("consumers") or []
            oldest_idle = helper.pending_oldest_idle_ms(self.outbox_stream, sample=1)
            logger.info(
                "outbox pending=%d oldest_idle_ms=%d consumers=%s ctr=%s",
                pending,
                oldest_idle,
                cons,
                dict(list(self._ctr.items())[:20]),
            )
        except Exception as e:
            # fail-open: метрики не должны ломать диспатчер,
            # но скрывать исключения нельзя (иначе "немая" телеметрия).
            sd_fail_open(
                getattr(self, "logger", None),
                key="outbox_pending_by_consumer_metrics_error",
                err=e,
                incr_fn=getattr(self, "_incr", None),
                metric_key=f"{self.metrics_prefix}:outbox_pending_by_consumer_metrics_errors_total",
            )

    def _cleanup_dead_consumers(self, helper: SyncRedisStreamHelper) -> None:
        if not self.cleanup_dead_consumers:
            return
        now = time.monotonic()
        if now - self._last_consumer_cleanup < 60.0:
            return
        self._last_consumer_cleanup = now

        try:
            cs = helper.consumers_info(self.outbox_stream)
        except Exception:
            return

        for c in cs or []:
            try:
                name = str(c.get("name") or "")
                pending = int(c.get("pending") or 0)
                idle = int(c.get("idle") or 0)  # ms
            except Exception:
                continue

            if not name or pending <= 0:
                continue
            if idle < self.dead_consumer_idle_ms:
                continue

            # best-effort cleanup: DELCONSUMER только удаляет consumer entry, PEL записи останутся pending
            # и будут XAUTOCLAIM-нуты обычным recovery.
            try:
                self.redis.xgroup_delconsumer(self.outbox_stream, self.group, name)
                self._ctr["delconsumer"] += 1
                logger.warning("xgroup_delconsumer: %s (pending=%d idle_ms=%d)", name, pending, idle)
            except Exception:
                continue

    def _janitor(self) -> None:
        if not self._janitor_enabled:
                return
        now = time.monotonic()
        if now - self._last_janitor < self.janitor_every_sec:
            return
        self._last_janitor = now
        # фиксируем "orphan без TTL": выставляем TTL или удаляем
        try:
            cursor = 0
            scanned = 0
            pattern = f"{self.marker_prefix}:*"
            while scanned < self._janitor_scan_count:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=10000)
                for k in keys or []:
                    scanned += 1
                    try:
                        ttl = int(self.redis.ttl(k))
                        if ttl < 0:
                            # -1 no ttl, -2 missing
                            self.redis.expire(k, self.marker_ttl_sec)
                    except Exception:
                        continue
                if scanned >= self._janitor_scan_count:
                    break
                if cursor == 0:
                    break
        except Exception:
            pass

    def _xadd_idempotent(self, client: Any, *, target: str, sid: str, stream: str, fields: Dict[str, Any], maxlen: int) -> bool:
        """
        Fix: marker set AFTER XADD in single Lua, with rollback on marker-fail.
        Returns True if delivered (or was already delivered).
        """
        fv: List[str] = []
        for k, v in (fields or {}).items():
            fv.append(str(k))
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = self.lua_scripts.execute(
            "xadd_fields_then_mark",
            keys=[self._marker_key(target, sid), stream],
            args=[str(self.delivery_marker_ttl_sec), str(maxlen)] + fv,
        )
        return bool(res and int(res[0]) in (0, 1))

    def _setex_idempotent(self, client: Any, *, target: str, sid: str, key: str, value_json: str, ttl_sec: int) -> bool:
        res = self.lua_scripts.execute(
            "setex_then_mark",
            keys=[self._marker_key(target, sid), key],
            args=[str(self.delivery_marker_ttl_sec), str(int(ttl_sec)), value_json],
            client=client
        )
        return bool(res and int(res[0]) in (0, 1))

    def _notify_idempotent(self, client: Any, *, sid: str, payload: Dict[str, Any]) -> bool:
        fv: List[str] = []
        for k, v in (payload or {}).items():
            fv.append(str(k))
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = self.lua_scripts.execute(
            "notify_gate",
            keys=[self._marker_key("notify", sid), self.notify_stream, self.notify_signal_counter_key],
            args=[str(self.delivery_marker_ttl_sec), str(500000), str(self.notify_signal_every_n)] + fv,
            client=client
        )
        
        # Debug logging for signal gate
        try:
             self.logger.info(f"[SignalGate] SID={sid} N={self.notify_signal_every_n} Result={res} (1=Sent, 0=Skipped)")
        except Exception:
             pass

        return bool(res and int(res[0]) in (0, 1))

    def _cleanup_dead_consumers(self, helper: SyncRedisStreamHelper) -> None:
        if not self.cleanup_dead_consumers:
            return
        now = time.monotonic()
        if now - self._last_consumer_cleanup < 60.0:
            return
        self._last_consumer_cleanup = now

        try:
            cs = helper.consumers_info(self.outbox_stream)
        except Exception:
            return

        for c in cs or []:
            try:
                name = str(c.get("name") or "")
                pending = int(c.get("pending") or 0)
                idle = int(c.get("idle") or 0)  # ms
            except Exception:
                    continue

            if not name or pending <= 0:
                continue
            if idle < self.dead_consumer_idle_ms:
                continue

            # best-effort cleanup: DELCONSUMER только удаляет consumer entry, PEL записи останутся pending
            # и будут XAUTOCLAIM-нуты обычным recovery.
            try:
                self.redis.xgroup_delconsumer(self.outbox_stream, self.group, name)
                self._ctr["delconsumer"] += 1
                logger.warning("xgroup_delconsumer: %s (pending=%d idle_ms=%d)", name, pending, idle)
            except Exception:
                continue

    def _parse_envelope(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Backward compatibility:
          - OutboxWriter may write both: data + payload
          - Some legacy producers wrote only: payload
        """
        from common.payload_fingerprint import fingerprint_tradeable_payload
        from common.json_safe import to_json_safe

        raw = fields.get("data")
        if not raw:
            raw = fields.get("payload")
        if not raw:
            raw = fields.get("payload_json")
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            env = json.loads(raw)
        elif isinstance(raw, dict):
            env = raw
        else:
            return None

        if not isinstance(env, dict):
            return None

        # ✅ AUTO-REPAIR: If envelope is flat (not nested under 'targets'), wrap it.
        # This provides resilience against stale producers or schema mismatches.
        if "targets" not in env:
            try:
                # Check if it looks like an old/flat envelope
                has_audit = "audit_payload" in env
                has_notify = "notify_payload" in env or "notify" in env
                
                if has_audit or has_notify:
                    self.logger.info(f"🔧 Auto-repairing flat envelope for sid={env.get('sid', 'unknown')}")
                    targets = {}
                    
                    # Move audit
                    if "audit_payload" in env:
                        targets["audit_payload"] = env.pop("audit_payload")
                    
                    # Move notify
                    if "notify_payload" in env:
                        targets["notify"] = env.pop("notify_payload")
                    elif "notify" in env:
                        targets["notify"] = env.pop("notify")
                    
                    # Move signal_stream_payload
                    if "signal_stream_payload" in env:
                        targets["signal_stream_payload"] = env.pop("signal_stream_payload")
                    
                    env["targets"] = targets
                    
                    # Ensure meta exists
                    if "meta" not in env:
                        env["meta"] = {}
                    
                    # If we have audit_stream / signal_stream at top, move to meta
                    for key in ["audit_stream", "signal_stream", "manual_stream"]:
                        if key in env and key not in env["meta"]:
                            env["meta"][key] = env.pop(key)
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to auto-repair flat envelope: {e}")

        # ✅ VALIDATION: Ensure envelope structure is correct (audit_payload/meta must not be on top level)
        if "audit_payload" in env or "meta" not in env or "targets" not in env:
            try:
                self.logger.warning(f"⚠️ Malformed envelope structure detected: audit_payload on top level or missing required fields")
                self.logger.warning(f"   env keys: {list(env.keys())}")
                self.logger.warning(f"   sid: {env.get('sid', 'unknown')}")
                # Send to DLQ for malformed envelopes
                payload = {
                    "ts": get_ny_time_millis(),
                    "reason": "malformed_envelope_structure",
                    "sid": str(env.get("sid") or ""),
                    "env_keys": list(env.keys()),
                    "has_audit_payload_top": "audit_payload" in env,
                    "has_meta": "meta" in env,
                    "has_targets": "targets" in env,
                    "raw": raw[:1000] if isinstance(raw, str) else str(raw)[:1000],
                }
                self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
            except Exception:
                pass
            return None

        # FAIL-CLOSED (DLQ) on fingerprint mismatch BEFORE ANY MUTATION.
        try:
            meta = env.get("meta") or {}
            expected = meta.get("payload_sha1") if isinstance(meta, dict) else None
            if isinstance(expected, str) and expected:
                env_safe = to_json_safe(env)
                got, _nbytes = fingerprint_tradeable_payload(env_safe)
                if str(got) != str(expected):
                    # write to generic DLQ (not target-specific, envelope-level corruption)
                    try:
                        payload = {
                            "ts": get_ny_time_millis(),
                            "reason": "payload_fingerprint_mismatch",
                            "sid": str(env.get("sid") or ""),
                            "expected": str(expected),
                            "got": str(got),
                            "env": env_safe,
                        }
                        self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
                    except Exception:
                        pass
                    return None
        except Exception:
            # fail-open: do not block dispatch on fingerprint verification failures
            pass

        # ✅ VALIDATE DELTA/Z PRESERVATION: Check if delta_spike signals maintain delta/z
        try:
            targets = env.get("targets") or {}
            for target_name, target_payload in targets.items():
                if isinstance(target_payload, dict):
                    signal_type = target_payload.get("type")
                    has_delta = "delta" in target_payload
                    has_delta_z = "delta_z" in target_payload or "z" in target_payload

                    if signal_type == "delta_spike" or has_delta:
                        delta_val = target_payload.get("delta")
                        delta_z_val = target_payload.get("delta_z") or target_payload.get("z")

                        # CRITICAL: Detect if delta/z were zeroed out inappropriately
                        if (delta_val is None or delta_z_val is None) and (has_delta or has_delta_z):
                            if self.logger:
                                self.logger.warning(
                                    f"⚠️ [{env.get('sid', 'unknown')}] Potential delta/z zeroing detected in {target_name}: "
                                    f"delta={delta_val}, z={delta_z_val}, signal_type={signal_type}"
                                )
        except Exception as exc:
            # fail-open: log and continue (don't break valid envelopes due to validation bugs)
            if self.logger:
                self.logger.warning(f"⚠️ [{env.get('sid', 'unknown')}] Delta/z validation error: {exc}")
            pass

        return env

    # =============================================================================
    # Crash-consistency: MSG-done (ACK-fail recovery)
    # =============================================================================

    def _mark_msg_done(self, msg_id: str) -> None:
        """
        Ставим ПЕРЕД XACK.
        Если XACK упал transiently — pending recovery должен сделать ACK-only.
        """
        try:
            r = getattr(self, "redis", None)
            if r is None:
                return
            r.set(self._msg_done_key(msg_id), "1", ex=int(self.done_ttl_sec), nx=True)
        except Exception:
            return

    def _process_one_outbox_message(self, *, msg_id: str, env: Dict[str, Any], sid: str) -> None:
        """
        Жёсткий контракт:
          - если msg_done уже стоит -> ACK-only (никаких доставок/маркер-проверок)
          - иначе -> deliver_targets -> mark_msg_done -> XACK
        """
        # ACK-only fast path
        if self._is_msg_done(msg_id):
            self._xack_only(msg_id=msg_id)
            return

        # deliver (ваш реальный путь)
        self._deliver_targets_with_retry(env, sid)

        # IMPORTANT: mark msg done BEFORE XACK
        self._mark_msg_done(msg_id)

        # ACK (если упадёт — msg_done уже есть, recovery пойдёт ACK-only)
        self._xack_only(msg_id=msg_id)

    def _xack_only(self, *, msg_id: str) -> None:
        """
        Единственная функция, делающая XACK.
        Тесты будут monkeypatch'ить её без знания вашего main loop.
        """
        try:
            r = getattr(self, "redis", None)
            if r is None:
                return
            group = str(getattr(self, "group", "") or "")
            consumer = str(getattr(self, "consumer", "") or "")
            stream = str(getattr(self, "outbox_stream", "") or "")
            if group and stream:
                r.xack(stream, group, msg_id)
        except Exception:
            # важно: наружу может подниматься transient, чтобы pending recovered
            raise

    # =============================================================================
    # Runtime-guard: strict validation (опционально, но рекомендую)
    # =============================================================================

    def _strict_validate_env(self, env: Dict[str, Any]) -> None:
        if str(os.getenv("OUTBOX_STRICT_VALIDATE","0")).lower() in {"0","false","no"}:
            return
        # 1) trace/events не должны быть в targets
        t = env.get("targets") or {}
        def _scan(x):
            if isinstance(x, dict):
                if isinstance(x.get("trace"), (dict, list)) or isinstance(x.get("decision_trace"), (dict, list)):
                    raise ValueError("trace leaked into tradeable targets")
                for v in x.values(): _scan(v)
            elif isinstance(x, list):
                for v in x: _scan(v)
        _scan(t)

    def _extract_trace_id(self, env: Dict[str, Any], fields: Dict[str, Any], msg_id: str) -> str:
        # 1) envelope
        try:
            tid = str(env.get("trace_id") or env.get("corr_id") or "").strip()
        except Exception:
            tid = ""
        # 2) stream fields (если outbox_writer добавил trace_id отдельным полем)
        if not tid:
            try:
                v = fields.get("trace_id")
                if isinstance(v, (bytes, bytearray)):
                    v = v.decode("utf-8", "ignore")
                tid = str(v or "").strip()
            except Exception:
                tid = ""
        # 3) fallback: не идеально, но лучше чем пусто (корреляция хотя бы по msg_id)
        if not tid:
            tid = f"msg:{msg_id}"
        return tid

    def run(self) -> None:
        if self.redis is None:
            logger.error("❌ Redis client is None, cannot start dispatcher")
            return

        helper = SyncRedisStreamHelper(self.redis, self.group, self.consumer)
        # IMPORTANT (C): outbox group MUST start from "0" to avoid loss on recovery/recreate
        helper.ensure_groups([self.outbox_stream], start_id="0")
        logger.info("SignalDispatcher started. stream=%s group=%s consumer=%s", self.outbox_stream, self.group, self.consumer)
        try:
            while True:
                self._drain_retries_best_effort()
                self._tick_housekeeping(helper)

                # 1) claim pending periodically (recovers ACK-fail / dead consumer)
                self._maybe_claim_pending(helper)

                # 2) read new (">")
                messages = helper.read(
                    {self.outbox_stream: ">"},
                    count=self.read_count,
                    block=self.read_block_ms,
                    recover_start_id="0",  # Fix C) (group recovery start)
                )

                # IMPORTANT:
                #   `continue` MUST be inside the "no messages" branch.
                #   Otherwise the whole "process new messages" code becomes unreachable.
                if not messages:
                    self._maybe_claim_pending(helper)
                    self._maybe_diag(helper)
                    self._maybe_maintenance()
                    continue

                # Process new messages (per-message ACK; no "ack only last" bug)
                for stream, items in messages:
                    for m in items:
                        msg_id = getattr(m, "msg_id", "") or ""
                        fields = getattr(m, "fields", {}) or {}
                        if not msg_id:
                            continue

                        # If we already committed side-effects earlier but ACK failed:
                        # pending-recovery must become ACK-only.
                        if self._is_outbox_done(str(msg_id)):
                            try:
                                helper.ack(stream, str(msg_id))
                                self._ctr["acked_done_fastpath"] += 1
                            except Exception as exc:
                                self._ctr["ack_failed_done_fastpath"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, str(msg_id))
                            continue

                        if not self._try_acquire_lease(str(msg_id)):
                            continue

                        ack_now = False
                        try:
                            ack_now = bool(self._handle_one(str(msg_id), fields))
                        except Exception as exc:
                            # keep pending on errors (autoclaim will recover)
                            self._ctr["handle_one_ex"] += 1
                            if is_transient_error(exc):
                                # transient: simply retry later
                                pass
                            else:
                                logger.error("Failed msg %s: %s", msg_id, exc, exc_info=True)
                            ack_now = False

                        if ack_now:
                            # CRITICAL: set msg-done marker BEFORE ACK attempt
                            # so pending recovery can safely do ACK-only.
                            self._mark_outbox_done(str(msg_id))
                            try:
                                helper.ack(stream, str(msg_id))
                                self._ctr["acked"] += 1
                            except Exception as exc:
                                self._ctr["ack_failed"] += 1
                                if is_transient_error(exc):
                                    self._remember_ack_retry(stream, str(msg_id))
                                    logger.warning("Transient ACK failed %s: %s (will retry ack)", msg_id, exc)
                                else:
                                    logger.warning("ACK failed %s: %s", msg_id, exc)
                        self._release_lease(str(msg_id))

                self._maybe_diag(helper)
                self._maybe_maintenance()
        except KeyboardInterrupt:
            logger.info("SignalDispatcher stopped")
            return
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning("Redis connection lost in dispatcher loop. Retrying...")
            time.sleep(1)
        except Exception as exc:
            logger.error("Dispatcher loop error: %s", exc, exc_info=True)
            time.sleep(1)

    def _tick_housekeeping(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()

        # cleanup ack-retry cache
        if now - self._last_ack_cleanup_mono > 60.0:
            self._last_ack_cleanup_mono = now
            ttl = float(self.ack_retry_ttl_s)
            self._ack_retry = {k: v for k, v in self._ack_retry.items() if now - v < ttl}

        # periodic metrics log (cheap ones + XPENDING summary/details)
        if now - self._last_metrics_mono >= float(self.metrics_every_sec):
            self._last_metrics_mono = now
            self._emit_metrics(helper)

        if now - self._last_repair_mono >= float(self.orphan_repair_every_sec):
            self._last_repair_mono = now
            self._repair_orphan_markers_best_effort()

    def _emit_metrics(self, helper: SyncRedisStreamHelper) -> None:
        try:
            outbox_len = int(self.redis.xlen(self.outbox_stream))
        except Exception:
            outbox_len = -1
        try:
            pending = int(helper.pending_len(self.outbox_stream))
        except Exception:
            pending = -1

        by_consumer = self._pending_by_consumer(limit=50)
        oldest_idle = self._pending_oldest_idle_ms()

        logger.info(
            "outbox metrics: len=%s pending=%s oldest_idle_ms=%s read_count=%d block_ms=%d claim_idle_ms=%d ctr=%s pending_by_consumer=%s",
            outbox_len,
            pending,
            oldest_idle,
            self.read_count,
            self.read_block_ms,
            self.claim_min_idle_ms,
            dict(self._ctr),
            by_consumer,
        )

    def _pending_oldest_idle_ms(self) -> int:
        """
        XPENDING details 1 entry: берём oldest idle_ms для диагностики залипов.
        """
        try:
            rows = self.redis.execute_command("XPENDING", self.outbox_stream, self.group, "-", "+", 1)
        except Exception:
            return -1
        if not isinstance(rows, list) or not rows:
            return 0
        r = rows[0]
        if not isinstance(r, (list, tuple)) or len(r) < 3:
            return -1
        try:
            return int(r[2])
        except Exception:
            return -1

    def _pending_by_consumer(self, limit: int = 50) -> Dict[str, int]:
        """
        Диагностика конкуренции: XPENDING details -> counts by consumer (best-effort).
        """
        try:
            # XPENDING stream group - + limit
            rows = self.redis.execute_command("XPENDING", self.outbox_stream, self.group, "-", "+", int(limit))
        except Exception:
            return {}
        out: Dict[str, int] = {}
        # rows: [(id, consumer, idle_ms, deliveries), ...]
        if isinstance(rows, list):
            for r in rows:
                if not isinstance(r, (list, tuple)) or len(r) < 2:
                    continue
                consumer = str(r[1])
                out[consumer] = out.get(consumer, 0) + 1
        return out

    def _repair_orphan_markers_best_effort(self) -> None:
        """
        SCAN deliver/done namespaces: if TTL=-1, set EXPIRE.
        Done rarely in small batches.
        """
        prefixes = (self.marker_prefix, self.done_prefix)
        try:
            for pref in prefixes:
                cursor, keys = self.redis.scan(
                    cursor=self._repair_cursor,
                    match=f"{pref}:*",
                    count=int(self.marker_repair_batch),
                )
                self._repair_cursor = int(cursor or 0)
                if not keys:
                    continue
                repaired = 0
                for k in keys:
                    try:
                        ttl = self.redis.ttl(k)
                        if int(ttl) < 0:
                            self.redis.expire(k, int(self.delivery_marker_ttl_sec))
                            repaired += 1
                    except Exception:
                        continue
                if repaired:
                    self._ctr["marker_repaired"] += repaired
        except Exception:
            return

    def _remember_ack_retry(self, stream: str, msg_id: str) -> None:
        key = (stream, msg_id)
        self._ack_retry[key] = time.monotonic()
        if len(self._ack_retry) > self.ack_retry_max:
            # drop oldest ~10%
            sorted_keys = sorted(self._ack_retry.keys(), key=lambda k: self._ack_retry[k])
            for k in sorted_keys[:max(1, self.ack_retry_max // 10)]:
                self._ack_retry.pop(k, None)

    def _try_ack_retry_only(self, helper: SyncRedisStreamHelper, stream: str, msg_id: str) -> bool:
        key = (stream, msg_id)
        if key not in self._ack_retry:
            return False
        try:
            helper.ack(stream, msg_id)
            self._ack_retry.pop(key, None)
            self._ctr["acked_retry_ok"] += 1
            return True
        except Exception as e:
            if is_transient_error(e):
                self._ctr["acked_retry_transient"] += 1
                return True  # do not re-process; keep trying ack in next ticks
            # non-transient: drop from cache and let normal flow continue
            self._ack_retry.pop(key, None)
            self._ctr["acked_retry_drop"] += 1
            return False

    def _maybe_claim_pending(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if (now - self._last_claim_mono) * 1000.0 < float(self.claim_every_ms):
            return
        self._last_claim_mono = now

        claimed_total = 0
        claimed_msgs: List[Any] = []
        last_next_id: Optional[str] = None

        # fair drain: маленькими порциями, чтобы не starve XREADGROUP ">"
        while claimed_total < int(self.claim_budget_per_tick):
            try:
                next_id, msgs = helper.claim_pending(
                    self.outbox_stream,
                    min_idle_ms=self.claim_min_idle_ms,
                    start_id=self._pending_start_id,
                    count=min(int(self.claim_count), int(self.claim_budget_per_tick) - claimed_total),
                )
            except Exception as e:
                if is_transient_error(e):
                    self._ctr["claim_transient"] += 1
                    return
                raise

            # wrap-guard: если скан завершён и пусто — не сбрасываемся в вечный "0-0" цикл
            if (not msgs) and (str(next_id or "") == "0-0"):
                self._ctr["claim_wrap_empty"] += 1
                break

            if msgs:
                self._ctr["claimed"] += len(msgs)
                claimed_total += len(msgs)
                claimed_msgs.extend(list(msgs))

            last_next_id = str(next_id) if next_id else last_next_id

        self._pending_claimed += claimed_total
        if last_next_id:
            self._pending_start_id = str(last_next_id)

        for m in claimed_msgs:
            msg_id = getattr(m, "msg_id", "") or ""
            fields = getattr(m, "fields", None) or {}

            # If ACK failed earlier, try ACK-only first
            if self._try_ack_retry_only(helper, self.outbox_stream, msg_id):
                continue

            # done-only ACK (важно после рестарта/ack-fail)
            if self._is_msg_done(str(msg_id)):
                try:
                    helper.ack(self.outbox_stream, msg_id)
                    self._ctr["acked_claimed_done_only"] += 1
                except Exception as e:
                    self._ctr["ack_failed_claimed_done_only"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.outbox_stream, msg_id)
                continue

            # If ACK failed earlier, try ACK-only first
            if self._try_ack_retry_only(helper, self.outbox_stream, msg_id):
                continue

            if not self._try_acquire_lease(str(msg_id)):
                continue

            ok = False
            try:
                ok = self._handle_one(msg_id, fields)
            except Exception as exc:
                logger.error("Failed to handle claimed pending msg %s: %s", msg_id, exc, exc_info=True)
                ok = False
            if ok:
                self._mark_outbox_done(str(msg_id))
                try:
                    helper.ack(self.outbox_stream, msg_id)
                    self._ctr["acked_claimed"] += 1
                except Exception as e:
                    self._ctr["ack_failed_claimed"] += 1
                    if is_transient_error(e):
                        self._remember_ack_retry(self.outbox_stream, msg_id)
                        logger.warning("Transient ACK failed (claimed) %s: %s (will retry ack)", msg_id, e)
                    else:
                        logger.warning("ACK failed (claimed) %s: %s", msg_id, e)
            self._release_lease(str(msg_id))

    def _maybe_log_diagnostics(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag_mono < float(self.diag_every_sec):
            return
        self._last_diag_mono = now
        try:
            p = helper.pending_len(self.outbox_stream)
            by = helper.pending_by_consumer(self.outbox_stream)
            logger.info("outbox_pending=%s pending_by_consumer=%s", p, dict(by) if by else {})
        except Exception as e:
            logger.warning("diagnostics failed: %s", e)

    def _maybe_repair_marker_ttls(self) -> None:
        """
        Best-effort "orphan markers" repair:
          - scan limited number of marker keys
          - if TTL == -1 (no expire) -> set expire
        This avoids permanent orphans after manual ops / older versions.
        """
        if not self.redis:
            return
        now = time.monotonic()
        if now - self._last_marker_repair_mono < float(self.marker_repair_every_sec):
            return
        self._last_marker_repair_mono = now
        try:
            cursor = 0
            scanned = 0
            pattern = f"{self.delivery_marker_prefix}:*"
            while scanned < self.marker_repair_scan_count:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=10000)
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for k in keys:
                    scanned += 1
                    try:
                        ttl = self.redis.ttl(k)
                        if ttl == -1:
                            self.redis.expire(k, int(self.delivery_marker_ttl_sec))
                    except Exception:
                        continue
                    if scanned >= self.marker_repair_scan_count:
                        break
                if cursor == 0:
                    break
        except Exception as e:
            logger.warning("marker repair failed: %s", e)


if __name__ == "__main__":
    dispatcher = SignalDispatcher()
    dispatcher.run()

# --- Restored Helper for Tests ---
def _parse_envelope_fields(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        raw = fields.get("data") or fields.get("payload") or fields.get("payload_json")
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        return None
    except Exception:
        return None
