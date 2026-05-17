import json
import logging
import os
import random
import time
import uuid
from typing import Any

from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)

from prometheus_client import Counter, Gauge, Histogram

from common.transient_errors import is_transient_error
from core.delivery_atomic import DeliveryAtomic, DeliveryAtomicSettings
from core.dual_redis_client import get_dual_signals_redis
from core.notify_gate import NotifyGate, NotifyGateSettings
from core.outbox_envelope import SCHEMA_VERSION
from core.outbox_retry_queue import OutboxRetryQueue, RetryQueueSettings
from core.redis_client import get_redis
from core.redis_keys import STREAM_RETENTION as _STREAM_RETENTION
from core.redis_keys import RedisKeyPrefixes as RK
from core.redis_keys import RedisStreams as RS
from core.redis_stream_consumer import SyncRedisStreamHelper
from core.sid_lease import SidLease, SidLeaseSettings
from services.dispatcher.target_registry import TargetRegistry
import contextlib


# ── Dual-read schema_version skeleton (Phase 3) ────────────────────────────────
# The dispatcher accepts any schema_version in this set; everything else is
# DLQ'd as "unsupported_schema_version". To stage a v2 rollout:
#   1. Bump core.outbox_envelope.SCHEMA_VERSION to 2.
#   2. Set env OUTBOX_ACCEPT_SCHEMA_VERSIONS="1,2" for dual-read in canary.
#   3. After all producers emit v2, drop "1" from the env (single-read).
# Wire types: the field may arrive as int ("1") or as a stringified int;
# `_normalize_schema_version` canonicalises both to int before comparison.
def _parse_accepted_versions(default: int) -> frozenset:
    raw = os.getenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", "").strip()
    if not raw:
        return frozenset({int(default)})
    out = set()
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.add(int(tok))
        except (TypeError, ValueError):
            logger.warning("OUTBOX_ACCEPT_SCHEMA_VERSIONS: cannot parse %r, skipped", tok)
    if not out:
        return frozenset({int(default)})
    return frozenset(out)


ACCEPTED_SCHEMA_VERSIONS = _parse_accepted_versions(SCHEMA_VERSION)


def _normalize_schema_version(raw: Any) -> int | None:
    """Canonicalise the schema_version field to int. Returns None on parse failure.

    Handles all of: int, bool (rejected), numeric string, float-looking string.
    Trims whitespace. Never raises.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        if raw != raw or raw == float("inf") or raw == float("-inf"):  # NaN/inf
            return None
        iv = int(raw)
        return iv if iv == raw else None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", "replace")
        except Exception:
            return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        try:
            f = float(s)
        except ValueError:
            return None
        iv = int(f)
        return iv if iv == f else None

AUDIT_SKIPPED_VIRTUAL = Counter("audit_skipped_virtual_total", "Skipped virtual signals", ["consumer", "stream"])
DISPATCHER_LEASE_CONTENTION = Counter("dispatcher_lease_contention_reschedules_total", "Lease contention extensions", ["consumer"])

# ── New metrics (P3 gap closure) ───────────────────────────────────────────────────
OUTBOX_QUEUE_DEPTH = Gauge(
    "outbox_queue_depth",
    "Number of pending messages in SIGNAL_OUTBOX stream (XLEN)",
)

OUTBOX_DLQ_DEPTH = Gauge(
    "outbox_dlq_depth",
    "Number of messages in DLQ stream (XLEN)",
)

DISPATCHER_PER_TARGET_DELIVERY_TOTAL = Counter(
    "dispatcher_per_target_delivery_total",
    "Successful per-target deliveries from outbox dispatcher",
    ["target", "consumer"],
)

SIGNAL_LOSS_SILENT_TOTAL = Counter(
    "signal_loss_silent_total",
    "Silent signal loss events: DLQ write failures + retry-increment failures",
    ["reason"],
)

# Explicit reject counter — incremented BEFORE the DLQ write so it is visible
# even when the DLQ write itself fails (SIGNAL_LOSS_SILENT_TOTAL only fires on
# DLQ write failure, not on the schema reject decision itself).
DISPATCHER_SCHEMA_VERSION_REJECTED_TOTAL = Counter(
    "dispatcher_schema_version_rejected_total",
    "Envelopes rejected due to unsupported or malformed schema_version",
    ["consumer", "schema_version"],
)

# ── #19: latency / depth metrics ──────────────────────────────────────────────
# ENV override: OUTBOX_DISPATCH_BUCKETS=1,5,10,25,50,100,250,500,1000,2500,5000
def _parse_outbox_buckets(env_var: str, default: tuple) -> tuple:
    import os as _os
    raw = _os.getenv(env_var, "")
    if not raw:
        return default
    try:
        return tuple(sorted(float(x.strip()) for x in raw.split(",") if x.strip()))
    except Exception:
        import logging as _log
        _log.getLogger(__name__).warning(
            "Invalid histogram buckets in %s=%r; using defaults", env_var, raw
        )
        return default

_DISPATCH_BUCKETS = _parse_outbox_buckets(
    "OUTBOX_DISPATCH_BUCKETS",
    (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
)
_TARGET_BUCKETS = _parse_outbox_buckets(
    "OUTBOX_TARGET_BUCKETS",
    (0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

DISPATCHER_DISPATCH_LAT_MS = Histogram(
    "dispatcher_dispatch_latency_ms",
    "End-to-end dispatch latency per outbox message (ms), from dequeue to ACK",
    ["consumer"],
    buckets=_DISPATCH_BUCKETS,
)

DISPATCHER_TARGET_LAT_MS = Histogram(
    "dispatcher_target_latency_ms",
    "Per-target xadd_once delivery latency (ms)",
    ["consumer", "target"],
    buckets=_TARGET_BUCKETS,
)

DISPATCHER_QUEUE_DEPTH = Gauge(
    "dispatcher_queue_depth",
    "Approximate number of pending messages in outbox stream for this consumer group",
    ["consumer", "stream"],
)

DISPATCHER_RETRY_READY = Gauge(
    "dispatcher_retry_queue_ready",
    "Messages in retry-ready ZSET waiting to be reprocessed",
    ["group"],
)

DISPATCHER_RETRY_INFLIGHT = Gauge(
    "dispatcher_retry_queue_inflight",
    "Messages currently held in retry-inflight ZSET (leased)",
    ["group"],
)

# ── #20: schema_version distribution ──────────────────────────────────────────
DISPATCHER_SCHEMA_VERSION_TOTAL = Counter(
    "dispatcher_schema_version_total",
    "Envelopes processed, labelled by schema_version; tracks v1/v2 distribution",
    ["consumer", "schema_version"],
)

class SignalDispatcher:
    """
    Outbox consumer with retry queue support:
      - Consumes from outbox stream with exactly-once delivery
      - Uses 2-phase retry queue (ready/inflight) for crash safety
      - Handles transient failures with exponential backoff
      - Supports multiple delivery targets (atomic per-target)
    """

    def __init__(self):
        self.redis = get_redis()
        self.dual = get_dual_signals_redis()
        self.dual_redis = get_dual_signals_redis()
        self.simple_redis = get_redis()

        # Stream config
        self.outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", RS.SIGNAL_DLQ)
        self.group = os.getenv("SIGNAL_OUTBOX_GROUP", "signals-outbox-group")
        self.consumer = os.getenv("SIGNAL_OUTBOX_CONSUMER", f"outbox-dispatcher-{uuid.uuid4().hex[:8]}")
        self.mt5_plans_stream = os.getenv("SIGNAL_MT5_PLANS_STREAM", RS.SIGNAL_PLANS)

        # Reading config
        self.read_count = int(os.getenv("SIGNAL_OUTBOX_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_OUTBOX_READ_BLOCK_MS", "1000"))

        # Retry config
        self.max_attempts = int(os.getenv("SIGNAL_OUTBOX_MAX_ATTEMPTS", "7"))
        self.retry_base_ms = int(os.getenv("SIGNAL_OUTBOX_RETRY_BASE_MS", "250"))
        self.retry_max_ms = int(os.getenv("SIGNAL_OUTBOX_RETRY_MAX_MS", "30000"))
        self.retry_pop_limit = int(os.getenv("SIGNAL_OUTBOX_RETRY_POP_LIMIT", "200"))
        self.retry_lease_ms = int(os.getenv("SIGNAL_OUTBOX_RETRY_LEASE_MS", "60000"))
        self.retry_requeue_limit = int(os.getenv("SIGNAL_OUTBOX_RETRY_REQUEUE_LIMIT", "200"))
        self.retry_meta_ttl_sec = int(os.getenv("SIGNAL_OUTBOX_RETRY_META_TTL_SEC", "86400"))

        # Retry queue keys
        self.retry_ready_zset = os.getenv("SIGNAL_OUTBOX_RETRY_READY_ZSET", f"sig:outbox:retry:ready:{self.group}")
        self.retry_inflight_zset = os.getenv("SIGNAL_OUTBOX_RETRY_INFLIGHT_ZSET", f"sig:outbox:retry:inflight:{self.group}")
        self.retry_due_hash = os.getenv("SIGNAL_OUTBOX_RETRY_DUE_HASH", f"sig:outbox:retry:due:{self.group}")
        self.retry_owner_hash = os.getenv("SIGNAL_OUTBOX_RETRY_OWNER_HASH", f"sig:outbox:retry:owner:{self.group}")

        # Initialize retry queue
        self._retryq = OutboxRetryQueue(
            self.redis,
            settings=RetryQueueSettings(
                ready_zset=self.retry_ready_zset,
                inflight_zset=self.retry_inflight_zset,
                due_hash=self.retry_due_hash,
                owner_hash=self.retry_owner_hash,
                meta_prefix=os.getenv("SIGNAL_OUTBOX_RETRY_META_PREFIX", f"sig:outbox:retry_meta:{self.group}"),
                meta_ttl_sec=self.retry_meta_ttl_sec,
            ),
        )

        # Delivery settings
        self.sid_lease_ttl_ms = int(os.getenv("SIGNAL_SID_LEASE_TTL_MS", "15000"))
        self.delivery_timeout_ms = int(os.getenv("SIGNAL_DELIVERY_TIMEOUT_MS", "30000"))

        # Initialize delivery components
        self._delivery = DeliveryAtomic(
            self.dual,
            settings=DeliveryAtomicSettings(
                marker_ttl_sec=int(os.getenv("SIGNAL_DELIVERY_MARKER_TTL_SEC", "86400")),
            ),
        )

        self.signal_notify_stream = os.getenv("SIGNAL_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        self.signal_manual_stream = os.getenv("SIGNAL_MANUAL_STREAM", RS.SIGNAL_MANUAL)
        self.signal_notify_maxlen = int(os.getenv("SIGNAL_NOTIFY_MAXLEN", "10000"))
        self.signal_manual_maxlen = int(os.getenv("SIGNAL_MANUAL_MAXLEN", "10000"))


        self._lease = SidLease(
            self.redis,
            settings=SidLeaseSettings(
                prefix=os.getenv("SIGNAL_SID_LEASE_PREFIX", "lease:sid:"),
            ),
        )

        self.sid_lease_renew_every_target = os.getenv("SIGNAL_SID_LEASE_RENEW_EVERY_TARGET", "1") == "1"

        # Notify gate settings
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        self.notify_signal_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", RK.NOTIFY_SIGNAL_COUNTER)
        self._notify_gate = NotifyGate(
            self.redis,
            settings=NotifyGateSettings(
                mode=os.getenv("NOTIFY_GATE_MODE", "hash"),
                every_n=int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1") or 1),
                ttl_sec=int(os.getenv("NOTIFY_GATE_TTL_SEC", "86400") or 86400),
                counter_key=self.notify_signal_counter_key,
            ),
        )

        # Claim settings (prevent early steal while message is scheduled for retry)
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_MIN_IDLE_MS", "0") or 0)
        if self.claim_min_idle_ms <= 0:
            self.claim_min_idle_ms = max(60000, int(self.retry_max_ms) * 2 + 5000)

        self.claim_count = int(os.getenv("SIGNAL_OUTBOX_CLAIM_COUNT", "200"))
        self.claim_every_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_EVERY_MS", "2000"))
        self._claim_start_id = "0-0"
        self._last_claim_mono = 0.0

        # Metrics
        self._last_metrics_mono = 0.0
        self._metrics_interval_ms = int(os.getenv("SIGNAL_METRICS_INTERVAL_MS", "10000"))
        self._handle_one_count = 0

    def _compute_delay_ms(self, attempt: int) -> int:
        """Exponential backoff with jitter"""
        if attempt <= 1:
            return 0
        delay = min(self.retry_max_ms, self.retry_base_ms * (2 ** min(attempt - 1, 8)))
        return int(delay * (0.5 + random.random() * 0.5))

    def _schedule_retry(self, msg_id: str, fields: dict[str, Any], attempt: int, err: Exception) -> None:
        delay_ms = self._compute_delay_ms(attempt)
        now_ms = get_ny_time_millis()
        due_ms = now_ms + int(delay_ms)
        # fields не сохраняем отдельно: сообщение остаётся в PEL, заберём через XCLAIM по msg_id
        self._retryq.schedule(
            str(msg_id),
            due_ms=due_ms,
            owner=str(self.consumer),
            meta={
                "ts": now_ms,
                "due": due_ms,
                "attempt": int(attempt),
                "err": str(err),
                "consumer": str(self.consumer),
            },
        )

    def _requeue_expired_retry_leases_tick(self) -> None:
        """
        Crash-safety for retry queue:
          if dispatcher popped msg_id into inflight and died before processing,
          lease expires => msg_id moves back to ready and will be retried by any dispatcher.
        """
        try:
            self._retryq.requeue_expired_inflight(limit=int(self.retry_requeue_limit))
        except Exception:
            # best-effort; do not crash main loop
            return

    def _process_retry_due(self, helper: SyncRedisStreamHelper) -> None:
        """
        Process due retry messages:
          - retry scheduler в Redis (ZSET)
          - pop_due -> INFLIGHT lease (Lua) => no duplicates AND no "lost" ids on crash
          - поля берём через XCLAIM (сообщение остаётся pending, re-enqueue не нужен)
        """
        ids = self._retryq.pop_due_to_inflight(
            limit=int(self.retry_pop_limit),
            lease_ms=int(self.retry_lease_ms),
        )
        if not ids:
            return

        for msg_id in ids:
            # 1) забираем payload из PEL в свой consumer (min-idle=0)
            claimed = None
            try:
                claimed = self.redis.execute_command(
                    "XCLAIM",
                    self.outbox_stream,
                    self.group,
                    self.consumer,
                    0,
                    msg_id,
                    "IDLE",
                    0,
                )
            except Exception as e:
                if self._is_transient(e):
                    # reschedule (also clears inflight)
                    self._retryq.schedule(
                        str(msg_id),
                        due_ms=get_ny_time_millis() + 250,
                        owner=str(self.consumer),
                        meta={"err": str(e), "phase": "xclaim"},
                    )
                    continue
                raise

            if not claimed:
                # сообщение могло быть уже ACKed/trimmed — просто отменяем ретрай (clean inflight too)
                self._retryq.cancel(msg_id)
                continue

            # claimed: [(id, {field: value}), ...]
            # decode_responses=True => dict[str,str]
            _id, fields = claimed[0]
            ok = self._handle_one(str(msg_id), dict(fields or {}), helper=helper, attempt_hint=0)
            if ok:
                # _handle_one ACK'ает; cleanup retry meta so it doesn't accumulate
                self._retryq.cancel(msg_id)
                continue
            # если _handle_one вернул False — он сам перескедулил через _schedule_retry() (which clears inflight)

    def _claim_pending_tick(self, helper: SyncRedisStreamHelper) -> None:
        now_mono = time.monotonic()
        if (now_mono - self._last_claim_mono) * 1000.0 < float(self.claim_every_ms):
            return
        self._last_claim_mono = now_mono

        start_id = self._claim_start_id
        next_id, msgs = helper.claim_pending(
            self.outbox_stream,
            min_idle_ms=int(self.claim_min_idle_ms),
            start_id=start_id,
            count=self.claim_count,
        )
        self._claim_start_id = next_id

        for msg in msgs:
            try:
                self._handle_one(str(msg.msg_id), msg.fields, helper=helper, attempt_hint=0)
            except Exception as exc:
                logger.error("❌ [%s] Error in _claim_pending_tick for %s: %s", self.consumer, msg.msg_id, exc, exc_info=True)

    def _pending_diag_tick(self) -> None:
        """Periodic diagnostics: retry queue sizes + outbox pending depth."""
        # Retry queue gauges (#19)
        try:
            rdy, inf = self._retryq.sizes()
            DISPATCHER_RETRY_READY.labels(group=self.group).set(float(rdy))
            DISPATCHER_RETRY_INFLIGHT.labels(group=self.group).set(float(inf))
        except Exception:
            pass
        # Queue depth (#19): XPENDING summary gives pending count
        try:
            info = self.redis.xpending(self.outbox_stream, self.group)
            depth = int((info or {}).get("pending", 0))
            DISPATCHER_QUEUE_DEPTH.labels(
                consumer=self.consumer, stream=self.outbox_stream
            ).set(depth)
        except Exception:
            pass
        # Stream depth gauges
        try:
            depth = self.redis.xlen(self.outbox_stream)
            OUTBOX_QUEUE_DEPTH.set(float(depth))
        except Exception:
            pass
        try:
            dlq_depth = self.redis.xlen(self.dlq_stream)
            OUTBOX_DLQ_DEPTH.set(float(dlq_depth))
        except Exception:
            pass

    def _marker_cleanup_tick(self) -> None:
        """Periodic cleanup of expired markers"""
        try:
            # Implementation for marker cleanup
            pass
        except Exception:
            pass

    def _is_transient(self, e: Exception) -> bool:
        return bool(is_transient_error(e))

    def run(self) -> None:
        helper = SyncRedisStreamHelper(self.redis, self.group, self.consumer, recovery_start_id="0")
        helper.ensure_group(self.outbox_stream, start_id="0")

        logger.info("🚀 [%s] SignalDispatcher.run() loop starting", self.consumer)
        while True:
            try:
                # print(f"DEBUG: [{self.consumer}] Loop tick...", flush=True)
                self._marker_cleanup_tick()
                self._pending_diag_tick()
                self._requeue_expired_retry_leases_tick()
                self._claim_pending_tick(helper)
                self._process_retry_due(helper)

                messages = helper.read({self.outbox_stream: ">"}, count=self.read_count, block=self.read_block_ms)
                if not messages:
                    continue

                logger.debug("📦 [%s] Read %d streams from outbox", self.consumer, len(messages))
                for stream, items in messages:
                    logger.debug("  📂 Stream %s has %d items", stream, len(items))
                    for msg_id, fields in items:
                        try:
                            logger.debug("    👉 Handling %s...", msg_id)
                            self._handle_one(str(msg_id), fields, helper=helper, attempt_hint=0)
                        except Exception as exc:
                            logger.error("❌ [%s] Error in main loop for %s: %s", self.consumer, msg_id, exc, exc_info=True)
            except KeyboardInterrupt:
                break
            except Exception as exc:
                logger.error("❌ [%s] Fatal error in SignalDispatcher loop: %s", self.consumer, exc, exc_info=True)
                time.sleep(1)

    def _parse_envelope(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        """Parse signal envelope from stream fields"""
        try:
            data = fields.get("data") or fields.get("payload") or fields.get("payload_json")
            if not data:
                return None
            if isinstance(data, str):
                return json.loads(data)
            return data
        except Exception:
            return None

    def _env_done_is_set(self, sid: str) -> bool:
        """Check if signal delivery is already complete"""
        try:
            # Implementation to check delivery status
            return False
        except Exception:
            return False

    def _mark_env_done(self, sid: str) -> None:
        """Mark signal delivery as complete"""
        try:
            # Implementation to mark delivery complete
            pass
        except Exception:
            pass

    def _send_dlq(self, msg_id: str, envelope: dict[str, Any], reason: str) -> None:
        """Send failed message to DLQ"""
        try:
            payload = {
                "original_msg_id": msg_id,
                "reason": reason,
                "envelope": envelope,
                "ts": get_ny_time_millis(),
                "consumer": self.consumer,
            }
            self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload)}, maxlen=_STREAM_RETENTION.get(self.dlq_stream, 10_000))
        except Exception as dlq_err:
            # True silent loss: message couldn't be written to DLQ either.
            with contextlib.suppress(Exception):
                SIGNAL_LOSS_SILENT_TOTAL.labels(reason="dlq_write_failed").inc()
            logger.error(
                "❌ [%s] DLQ write FAILED for %s reason=%s: %s (SILENT LOSS)",
                self.consumer, msg_id, reason, dlq_err,
            )

    def _bump_attempt(self, msg_id: str) -> int:
        """Increment and return attempt count in Redis"""
        try:
            key = f"sig:outbox:attempts:{self.group}:{msg_id}"
            n = self.redis.incr(key)
            self.redis.expire(key, 3600)  # cleanup after 1h
            return int(n)
        except Exception as e:
            logger.warning("⚠️ Failed to bump attempt for %s: %s", msg_id, e)
            with contextlib.suppress(Exception):
                SIGNAL_LOSS_SILENT_TOTAL.labels(reason="retry_incr_failed").inc()
            return 1

    def _deliver_all_atomic(self, env: dict[str, Any], *, sid: str, lease_token: str) -> None:
        targets = env.get("targets") or {}
        meta = env.get("meta") or {}
        # Helper: time a single target delivery and observe histogram (#19)
        def _timed_xadd_once(target_name: str, **kwargs):
            _t0 = time.monotonic()
            result = self._delivery.xadd_once(**kwargs)
            with contextlib.suppress(Exception):
                DISPATCHER_TARGET_LAT_MS.labels(
                    consumer=self.consumer, target=target_name
                ).observe((time.monotonic() - _t0) * 1000)
            return result

        def _renew_or_raise() -> None:
            if not self.sid_lease_renew_every_target:
                return
            ok = self._lease.renew(sid, token=lease_token, ttl_ms=int(self.sid_lease_ttl_ms))
            if not ok:
                raise RuntimeError("sid_lease_lost")

        dual_client = self.dual_redis or self.simple_redis or self.redis
        simple_client = self.simple_redis or self.redis

        # 1) notify stream
        notify_payload = targets.get("notify")
        if notify_payload and dual_client:
            _renew_or_raise()
            marker_key = self._delivery.marker_key("notify", sid)
            symbol = env.get("symbol") or env.get("sym") or ""
            if self._notify_gate.should_send(sid, symbol=symbol):
                ok, _ = _timed_xadd_once(
                    "notify",
                    marker_key=marker_key,
                    stream=self.signal_notify_stream,
                    payload=notify_payload,
                    maxlen=self.signal_notify_maxlen,
                )
                if ok:
                    with contextlib.suppress(Exception):
                        DISPATCHER_PER_TARGET_DELIVERY_TOTAL.labels(target="notify", consumer=self.consumer).inc()

        # 2) strategy stream
        signal_stream = str(meta.get("signal_stream") or TargetRegistry.get_task_stream("signal_stream"))
        signal_payload = targets.get("signal_stream_payload")
        is_virtual = bool(meta.get("is_virtual") or (isinstance(signal_payload, dict) and signal_payload.get("is_virtual")))
        if signal_payload and simple_client:
            if is_virtual:
                logger.info("ℹ️ [%s] Paper trade for %s: skipping signal_stream=%s (is_virtual=1)", self.consumer, sid, signal_stream)
                AUDIT_SKIPPED_VIRTUAL.labels(consumer=self.consumer, stream="signal").inc()
            else:
                logger.debug("      Delivering to signal_stream=%s", signal_stream)
                _renew_or_raise()
                marker_key = self._delivery.marker_key("signal_stream", sid)
                ok, _ = _timed_xadd_once(
                    "signal_stream",
                    marker_key=marker_key,
                    stream=signal_stream,
                    payload=signal_payload,
                    maxlen=1000,
                )
                if ok:
                    with contextlib.suppress(Exception):
                        DISPATCHER_PER_TARGET_DELIVERY_TOTAL.labels(target="signal_stream", consumer=self.consumer).inc()

        # 3) audit stream
        audit_stream = str(meta.get("audit_stream") or TargetRegistry.get_task_stream("audit"))
        audit_payload = targets.get("audit_payload")
        if audit_payload and self.redis:
            if is_virtual:
                logger.info("ℹ️ [%s] Paper trade for %s: skipping audit_stream (is_virtual=1)", self.consumer, sid)
                AUDIT_SKIPPED_VIRTUAL.labels(consumer=self.consumer, stream="audit").inc()
            else:
                logger.debug("      Delivering to audit_stream=%s sid=%s", audit_stream, sid)
                _renew_or_raise()
                marker_key = self._delivery.marker_key("audit", sid)
                try:
                    ok, res_id = _timed_xadd_once(
                        "audit",
                        marker_key=marker_key,
                        stream=audit_stream,
                        payload=audit_payload,
                        maxlen=200000,
                    )
                    if ok:
                        with contextlib.suppress(Exception):
                            DISPATCHER_PER_TARGET_DELIVERY_TOTAL.labels(target="audit", consumer=self.consumer).inc()
                    logger.debug("[OUTBOX] Audit delivery result ok=%s id=%s", ok, res_id)
                except Exception as e:
                    logger.warning("[OUTBOX] Audit delivery FAILED sid=%s: %s", sid, e)
                    raise

        # 4) manual stream (also gated/deduplicated)
        manual_payload = targets.get("manual_payload")
        if self.signal_manual_stream and manual_payload and dual_client:
            logger.debug("      Delivering to manual stream")
            _renew_or_raise()
            marker_key = self._delivery.marker_key("manual", sid)
            ok, _ = self._delivery.xadd_once(
                marker_key=marker_key,
                stream=self.signal_manual_stream,
                payload=manual_payload,
                maxlen=self.signal_manual_maxlen,
            )

        # 5) mt5 plans (skip if virtual)
        mt5_plan = targets.get("mt5_plan")
        is_virtual = bool(meta.get("is_virtual"))
        if self.mt5_plans_stream and mt5_plan and simple_client:
            if is_virtual:
                logger.info("ℹ️ [%s] Paper trade for %s: skipping mt5_plans_stream (is_virtual=1)", self.consumer, sid)
            else:
                logger.debug("      Delivering to mt5_plans_stream")
                _renew_or_raise()
                marker_key = self._delivery.marker_key("mt5_plan", sid)
                # mt5_bridge.redis_consumer wants specific format: { "payload": JSON({"plan": ...}) }
                # but here we just put the plan object itself into a wrapper
                # redis_consumer expects: { "payload": "{ \"plan\": { ... } }" }

                # Wrap plan into envelope expected by mt5_bridge
                wrapper = {"plan": mt5_plan}
                payload_json = json.dumps(wrapper, ensure_ascii=False)

                ok, _ = self._delivery.xadd_once(
                    marker_key=marker_key,
                    stream=self.mt5_plans_stream,
                    payload=wrapper,
                    maxlen=1000,
                )
                if ok:
                    with contextlib.suppress(Exception):
                        DISPATCHER_PER_TARGET_DELIVERY_TOTAL.labels(target="mt5", consumer=self.consumer).inc()

        # 5) snapshot
        snap_key = (meta.get("snap_key") or "")
        snap_ttl = int(meta.get("snap_ttl") or 21600)
        snap_payload = targets.get("snapshot")
        if snap_key and snap_payload and self.redis:
            logger.debug("      Delivering to snapshot")
            _renew_or_raise()
            marker_key = self._delivery.marker_key("snapshot", sid)

            # P1 queue to snapshot task stream instead of inline SETEX
            snapshot_tasks_stream = TargetRegistry.get_task_stream("snapshot")
            task_payload = json.dumps({
                "snap_key": snap_key,
                "snap_ttl": snap_ttl,
                "payload": snap_payload
            }, ensure_ascii=False)

            ok, _ = self._delivery.xadd_once(
                marker_key=marker_key,
                stream=snapshot_tasks_stream,
                payload={
                    "snap_key": snap_key,
                    "snap_ttl": snap_ttl,
                    "payload": snap_payload
                },
                maxlen=10000,
            )

        # 6) trade_back via Target Worker HTTP POST
        trade_back_payload = targets.get("trade_back")
        if trade_back_payload and self.redis:
            tb_url = TargetRegistry.get_http_url("trade_back")
            if tb_url:
                logger.debug("      Delivering to trade_back (HTTP POST)")
                _renew_or_raise()
                marker_key = self._delivery.marker_key("trade_back", sid)
                tb_tasks_stream = TargetRegistry.get_task_stream("trade_back")

                # We offload HTTP to Target Worker (with op=http_post)
                # It natively handles DLQ, backoff, retry, and gives us confirmation
                task_payload = json.dumps({
                    "op": "http_post",
                    "url": tb_url,
                    "payload": json.dumps(trade_back_payload, ensure_ascii=False),
                    "headers": {"Content-Type": "application/json"},
                    "timeout_sec": TargetRegistry.get_http_timeout("trade_back")
                }, ensure_ascii=False)

                ok, _ = self._delivery.xadd_once(
                    marker_key=marker_key,
                    stream=tb_tasks_stream,
                    payload={
                        "op": "http_post",
                        "url": tb_url,
                        "payload": trade_back_payload,
                        "headers": {"Content-Type": "application/json"},
                        "timeout_sec": TargetRegistry.get_http_timeout("trade_back")
                    },
                    maxlen=10000,
                )

    def _handle_one(self, msg_id: str, fields: dict[str, Any], *, helper: SyncRedisStreamHelper, attempt_hint: int = 0) -> bool:
        """Process one outbox message"""
        _t_handle_start = time.monotonic()  # #19: end-to-end dispatch latency
        try:
            self._handle_one_count += 1
            if self._handle_one_count % 10000 == 0:
                logger.debug("      Parsing envelope for %s... (thinned 1/10000)", msg_id)
            # parse + validate envelope
            env = self._parse_envelope(fields)
            if not env:
                self._send_dlq(msg_id, fields, reason="bad_envelope")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                self._retryq.cancel(msg_id)
                return True

            # Phase 3: dual-typed schema_version (int | numeric string).
            # See _normalize_schema_version + ACCEPTED_SCHEMA_VERSIONS above.
            raw_sv = env.get("schema_version", "")
            sv_int = _normalize_schema_version(raw_sv)
            # For Prometheus label cardinality: use canonical int or "unknown"/"malformed"
            sv_label = str(sv_int) if sv_int is not None else (
                "unknown" if raw_sv in ("", None) else "malformed"
            )
            with contextlib.suppress(Exception):
                DISPATCHER_SCHEMA_VERSION_TOTAL.labels(
                    consumer=self.consumer,
                    schema_version=sv_label,
                ).inc()
            if sv_int is None or sv_int not in ACCEPTED_SCHEMA_VERSIONS:
                with contextlib.suppress(Exception):
                    DISPATCHER_SCHEMA_VERSION_REJECTED_TOTAL.labels(
                        consumer=self.consumer,
                        schema_version=sv_label,
                    ).inc()
                self._send_dlq(msg_id, fields, reason=f"unsupported_schema_version:{sv_label}")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                self._retryq.cancel(msg_id)
                return True

            sid = env.get("sid")
            if not sid:
                self._send_dlq(msg_id, fields, reason="missing_sid")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                self._retryq.cancel(msg_id)
                return True

            if self._env_done_is_set(sid):
                try:
                    logger.debug("      ✅ [%s] Signal %s already done, ACKing %s", self.consumer, sid, msg_id)
                    helper.ack(self.outbox_stream, msg_id)
                except Exception as e:
                    if self._is_transient(e):
                        self._schedule_retry(msg_id, fields, max(1, attempt_hint), e)
                        return False
                return True

            # lease per sid
            token = f"{self.consumer}:{msg_id}:{uuid.uuid4().hex}"
            if not self._lease.acquire(sid, token=token, ttl_ms=self.sid_lease_ttl_ms):
                DISPATCHER_LEASE_CONTENTION.labels(consumer=self.consumer).inc()
                self._schedule_retry(msg_id, fields, max(1, attempt_hint), Exception("lease_contended"))
                return False

            # deliver all targets (atomic per-target)
            self._deliver_all_atomic(env, sid=sid, lease_token=token)

            self._mark_env_done(sid)

            # phase 2: final outbox ACK
            try:
                helper.ack(self.outbox_stream, msg_id)
            except Exception as e:
                if self._is_transient(e):
                    self._schedule_retry(msg_id, fields, max(1, attempt_hint), e)
                    return False
            # if we got here, delivery done and ACK succeeded
            self._retryq.cancel(msg_id)
            # #19: observe end-to-end latency on success
            with contextlib.suppress(Exception):
                DISPATCHER_DISPATCH_LAT_MS.labels(consumer=self.consumer).observe(
                    (time.monotonic() - _t_handle_start) * 1000
                )
            return True
        except Exception as exc:
            if self._is_transient(exc):
                n = self._bump_attempt(msg_id)
                if n >= self.max_attempts:
                    self._send_dlq(msg_id, fields, reason="max_attempts_transient")
                    self.redis.xack(self.outbox_stream, self.group, msg_id)
                    self._retryq.cancel(msg_id)
                    return True
                self._schedule_retry(msg_id, fields, n, exc)
                return False
            # non-transient -> DLQ quickly (or retry until max_attempts)
            n = self._bump_attempt(msg_id)
            if n >= self.max_attempts:
                self._send_dlq(msg_id, fields, reason="max_attempts")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                self._retryq.cancel(msg_id)
                return True
            self._schedule_retry(msg_id, fields, n, exc)
            return False


if __name__ == "__main__":
    SignalDispatcher().run()
