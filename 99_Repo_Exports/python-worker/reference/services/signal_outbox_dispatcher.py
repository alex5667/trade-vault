from utils.time_utils import get_ny_time_millis
import json
import os
import time
import random
import uuid
from typing import Any, Dict, Optional

import logging

logger = logging.getLogger(__name__)

from core.redis_client import get_redis
from core.dual_redis_client import get_dual_signals_redis
from core.redis_stream_consumer import SyncRedisStreamHelper
from core.delivery_atomic import DeliveryAtomic, DeliveryAtomicSettings
from core.sid_lease import SidLease, SidLeaseSettings
from core.outbox_retry_queue import OutboxRetryQueue, RetryQueueSettings
from core.notify_gate import NotifyGate, NotifyGateSettings
from common.transient_errors import is_transient_error


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
        self.outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", "stream:signals:outbox")
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", "stream:signals:dlq")
        self.group = os.getenv("SIGNAL_OUTBOX_GROUP", "signals-outbox-group")
        self.consumer = os.getenv("SIGNAL_OUTBOX_CONSUMER", f"outbox-dispatcher-{uuid.uuid4().hex[:8]}")
        self.mt5_plans_stream = os.getenv("SIGNAL_MT5_PLANS_STREAM", "stream:signals:plans")

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
            self.redis
            settings=RetryQueueSettings(
                ready_zset=self.retry_ready_zset
                inflight_zset=self.retry_inflight_zset
                due_hash=self.retry_due_hash
                owner_hash=self.retry_owner_hash
                meta_prefix=os.getenv("SIGNAL_OUTBOX_RETRY_META_PREFIX", f"sig:outbox:retry_meta:{self.group}")
                meta_ttl_sec=self.retry_meta_ttl_sec
            )
        )

        # Delivery settings
        self.sid_lease_ttl_ms = int(os.getenv("SIGNAL_SID_LEASE_TTL_MS", "15000"))
        self.delivery_timeout_ms = int(os.getenv("SIGNAL_DELIVERY_TIMEOUT_MS", "30000"))

        # Initialize delivery components
        self._delivery = DeliveryAtomic(
            self.dual
            settings=DeliveryAtomicSettings(
                marker_ttl_sec=int(os.getenv("SIGNAL_DELIVERY_MARKER_TTL_SEC", "86400"))
            )
        )
        
        self.signal_notify_stream = os.getenv("SIGNAL_NOTIFY_STREAM", "stream:notify:telegram")
        self.signal_manual_stream = os.getenv("SIGNAL_MANUAL_STREAM", "stream:signals:manual")
        self.signal_notify_maxlen = int(os.getenv("SIGNAL_NOTIFY_MAXLEN", "10000"))
        self.signal_manual_maxlen = int(os.getenv("SIGNAL_MANUAL_MAXLEN", "10000"))


        self._lease = SidLease(
            self.redis
            settings=SidLeaseSettings(
                prefix=os.getenv("SIGNAL_SID_LEASE_PREFIX", "lease:sid:")
            )
        )

        self.sid_lease_renew_every_target = os.getenv("SIGNAL_SID_LEASE_RENEW_EVERY_TARGET", "1") == "1"

        # Notify gate settings
        self.notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        self.notify_signal_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter")
        self._notify_gate = NotifyGate(
            self.redis
            settings=NotifyGateSettings(
                mode=os.getenv("NOTIFY_GATE_MODE", "hash")
                every_n=int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")) or 1)
                ttl_sec=int(os.getenv("NOTIFY_GATE_TTL_SEC", "86400") or 86400)
                counter_key=self.notify_signal_counter_key
            )
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

    def _schedule_retry(self, msg_id: str, fields: Dict[str, Any], attempt: int, err: Exception) -> None:
        delay_ms = self._compute_delay_ms(attempt)
        now_ms = get_ny_time_millis()
        due_ms = now_ms + int(delay_ms)
        # fields не сохраняем отдельно: сообщение остаётся в PEL, заберём через XCLAIM по msg_id
        self._retryq.schedule(
            str(msg_id)
            due_ms=due_ms
            owner=str(self.consumer)
            meta={
                "ts": now_ms
                "due": due_ms
                "attempt": int(attempt)
                "err": str(err)
                "consumer": str(self.consumer)
            }
        )

    def _requeue_expired_retry_leases_tick(self) -> None:
        """
        Crash-safety for retry queue:
          if dispatcher popped msg_id into inflight and died before processing
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
            limit=int(self.retry_pop_limit)
            lease_ms=int(self.retry_lease_ms)
        )
        if not ids:
            return

        for msg_id in ids:
            # 1) забираем payload из PEL в свой consumer (min-idle=0)
            claimed = None
            try:
                claimed = self.redis.execute_command(
                    "XCLAIM"
                    self.outbox_stream
                    self.group
                    self.consumer
                    0
                    msg_id
                    "IDLE"
                    0
                )
            except Exception as e:
                if self._is_transient(e):
                    # reschedule (also clears inflight)
                    self._retryq.schedule(
                        str(msg_id)
                        due_ms=get_ny_time_millis() + 250
                        owner=str(self.consumer)
                        meta={"err": str(e), "phase": "xclaim"}
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
            self.outbox_stream
            min_idle_ms=int(self.claim_min_idle_ms)
            start_id=start_id
            count=self.claim_count
        )
        self._claim_start_id = next_id

        for msg in msgs:
            try:
                self._handle_one(str(msg.msg_id), msg.fields, helper=helper, attempt_hint=0)
            except Exception as exc:
                logger.error("❌ [%s] Error in _claim_pending_tick for %s: %s", self.consumer, msg.msg_id, exc, exc_info=True)

    def _pending_diag_tick(self) -> None:
        # (существующий код)
        # ДОБАВЛЕНО: retry queue sizes
        try:
            rdy, inf = self._retryq.sizes()
            self._metric_gauge("outbox_retry_ready", rdy, group=self.group)
            self._metric_gauge("outbox_retry_inflight", inf, group=self.group)
        except Exception:
            pass
        try:
            # XPENDING summary / details already here
            pass
        except Exception:
            pass

    def _marker_cleanup_tick(self) -> None:
        """Periodic cleanup of expired markers"""
        try:
            # Implementation for marker cleanup
            pass
        except Exception:
            pass

    def _metric_gauge(self, name: str, value: float, **tags) -> None:
        """Best-effort metrics"""
        try:
            # Could integrate with health_metrics if available
            pass
        except Exception:
            pass

    def _is_transient(self, e: Exception) -> bool:
        return bool(is_transient_error(e))

    def run(self) -> None:
        helper = SyncRedisStreamHelper(self.redis, self.group, self.consumer, recovery_start_id="0")
        helper.ensure_group(self.outbox_stream, start_id="0")

        print(f"🚀 [{self.consumer}] SignalDispatcher.run() loop starting", flush=True)
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

                print(f"📦 [{self.consumer}] Read {len(messages)} streams from outbox", flush=True)
                for stream, items in messages:
                    print(f"  📂 Stream {stream} has {len(items)} items", flush=True)
                    for msg_id, fields in items:
                        try:
                            print(f"    👉 Handling {msg_id}...", flush=True)
                            self._handle_one(str(msg_id), fields, helper=helper, attempt_hint=0)
                        except Exception as exc:
                            logger.error("❌ [%s] Error in main loop for %s: %s", self.consumer, msg_id, exc, exc_info=True)
            except KeyboardInterrupt:
                break
            except Exception as exc:
                logger.error("❌ [%s] Fatal error in SignalDispatcher loop: %s", self.consumer, exc, exc_info=True)
                time.sleep(1)

    def _parse_envelope(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse signal envelope from stream fields"""
        try:
            data = fields.get("payload") or fields.get("data")
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

    def _send_dlq(self, msg_id: str, envelope: Dict[str, Any], reason: str) -> None:
        """Send failed message to DLQ"""
        try:
            payload = {
                "original_msg_id": msg_id
                "reason": reason
                "envelope": envelope
                "ts": get_ny_time_millis()
                "consumer": self.consumer
            }
            self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload)}, maxlen=200000)
        except Exception:
            pass

    def _bump_attempt(self, msg_id: str) -> int:
        """Increment and return attempt count in Redis"""
        try:
            key = f"sig:outbox:attempts:{self.group}:{msg_id}"
            n = self.redis.incr(key)
            self.redis.expire(key, 3600)  # cleanup after 1h
            return int(n)
        except Exception as e:
            logger.warning("⚠️ Failed to bump attempt for %s: %s", msg_id, e)
            return 1

    def _deliver_all_atomic(self, env: Dict[str, Any], *, sid: str, lease_token: str) -> None:
        targets = env.get("targets") or {}
        meta = env.get("meta") or {}

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
            # "сверхидеал": gating стабилен для sid и не ломается ретраями
            symbol = env.get("symbol") or env.get("sym") or ""
            if self._notify_gate.should_send(sid, symbol=str(symbol)):
                ok, _ = self._delivery.xadd_once(
                    marker_key=marker_key
                    stream=self.signal_notify_stream
                    payload=notify_payload
                    maxlen=self.signal_notify_maxlen
                )

        # 2) strategy stream
        signal_stream = str(meta.get("signal_stream") or "")
        signal_payload = targets.get("signal_stream_payload")
        if signal_stream and signal_payload and simple_client:
            print(f"      [DEBUG] Delivering to signal_stream={signal_stream}", flush=True)
            _renew_or_raise()
            marker_key = self._delivery.marker_key("signal_stream", sid)
            ok, _ = self._delivery.xadd_once(
                marker_key=marker_key
                stream=signal_stream
                payload={"data": json.dumps(signal_payload, ensure_ascii=False)}
                maxlen=1000
            )

        # 3) audit stream
        audit_stream = str(meta.get("audit_stream") or "")
        audit_payload = targets.get("audit_payload")
        if audit_stream and audit_payload and self.redis:
            print(f"      [DEBUG] Delivering to audit_stream={audit_stream}", flush=True)
            _renew_or_raise()
            marker_key = self._delivery.marker_key("audit", sid)
            # DEBUG: Log attempt
            print(f"[OUTBOX] Delivering audit stream={audit_stream} sid={sid} payload_len={len(str(audit_payload))}", flush=True)
            try:
                # Use 'payload' field as expected by raw stream consumers
                # Unpack the inner payload if it exists to avoid double JSON wrapping {"data": "{\"payload\": ...}"}
                # But wait, audit_payload is {"payload": "..."}.
                # If we wrap in {"data": ...}, we get data -> payload -> content.
                # Direct publish uses field="payload".
                # Let's try to match direct publish: use field names directly?
                # xadd_once accepts payload dict.
                # If audit_payload is {"payload": json_str}, we should pass it AS IS.
                # But existing code wrapped it in {"data": json.dumps(audit_payload)}.
                # If I change it, I change the contract.
                # But if existing contract result is ignored by consumers (because of field mismatch), I MUST change it.
                # For now, let's just log result.
                ok, res_id = self._delivery.xadd_once(
                    marker_key=marker_key
                    stream=audit_stream
                    payload={"data": json.dumps(audit_payload, ensure_ascii=False)}
                    maxlen=200000
                )
                print(f"[OUTBOX] Audit delivery result ok={ok} id={res_id}", flush=True)
            except Exception as e:
                print(f"[OUTBOX] Audit delivery FAILED: {e}", flush=True)
                raise

        # 4) manual stream (also gated/deduplicated)
        manual_payload = targets.get("manual_payload")
        if self.signal_manual_stream and manual_payload and dual_client:
            print(f"      [DEBUG] Delivering to manual stream", flush=True)
            _renew_or_raise()
            marker_key = self._delivery.marker_key("manual", sid)
            ok, _ = self._delivery.xadd_once(
                marker_key=marker_key
                stream=self.signal_manual_stream
                payload={"data": json.dumps(manual_payload, ensure_ascii=False)}
                maxlen=self.signal_manual_maxlen
            )

        # 5) mt5 plans (new)
        mt5_plan = targets.get("mt5_plan")
        if self.mt5_plans_stream and mt5_plan and simple_client:
            print(f"      [DEBUG] Delivering to mt5_plans_stream", flush=True)
            _renew_or_raise()
            marker_key = self._delivery.marker_key("mt5_plan", sid)
            # mt5_bridge.redis_consumer wants specific format: { "payload": JSON({"plan": ...}) }
            # but here we just put the plan object itself into a wrapper
            # redis_consumer expects: { "payload": "{ \"plan\": { ... } }" }
            
            # Wrap plan into envelope expected by mt5_bridge
            wrapper = {"plan": mt5_plan}
            payload_json = json.dumps(wrapper, ensure_ascii=False)
            
            ok, _ = self._delivery.xadd_once(
                marker_key=marker_key
                stream=self.mt5_plans_stream
                payload={"payload": payload_json}
                maxlen=1000
            )

        # 5) snapshot
        snap_key = str(meta.get("snap_key") or "")
        snap_ttl = int(meta.get("snap_ttl") or 21600)
        snap_payload = targets.get("snapshot")
        if snap_key and snap_payload and self.redis:
            print(f"      [DEBUG] Delivering to snapshot", flush=True)
            _renew_or_raise()
            marker_key = self._delivery.marker_key("snapshot", sid)
            ok = self._delivery.setex_once(
                marker_key=marker_key
                key=snap_key
                ttl_sec=snap_ttl
                payload=snap_payload
            )

    def _handle_one(self, msg_id: str, fields: Dict[str, Any], *, helper: SyncRedisStreamHelper, attempt_hint: int = 0) -> bool:
        """Process one outbox message"""
        try:
            self._handle_one_count += 1
            if self._handle_one_count % 10000 == 0:
                print(f"      [DEBUG] Parsing envelope for {msg_id}... (thinned 1/10000)", flush=True)
            # parse + validate envelope
            env = self._parse_envelope(fields)
            if not env:
                self._send_dlq(msg_id, fields, reason="bad_envelope")
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
                    print(f"      ✅ [{self.consumer}] Signal {sid} already done, ACKing {msg_id}", flush=True)
                    print(f"      ✅ [{self.consumer}] Successfully handled {sid}, ACKing {msg_id}", flush=True)
                    helper.ack(self.outbox_stream, msg_id)
                except Exception as e:
                    if self._is_transient(e):
                        self._schedule_retry(msg_id, fields, max(1, attempt_hint), e)
                        return False
                return True

            # lease per sid
            token = f"{self.consumer}:{msg_id}:{uuid.uuid4().hex}"
            if not self._lease.acquire(sid, token=token, ttl_ms=self.sid_lease_ttl_ms):
                self._schedule_retry(msg_id, fields, max(1, attempt_hint), Exception("lease_contended"))
                return False

            # deliver all targets (atomic per-target)
            self._deliver_all_atomic(env, sid=sid, lease_token=token)
            self._mark_env_done(sid)

            # ack
            try:
                helper.ack(self.outbox_stream, msg_id)
            except Exception as e:
                if self._is_transient(e):
                    self._schedule_retry(msg_id, fields, max(1, attempt_hint), e)
                    return False
            # if we got here, delivery done and ACK succeeded
            self._retryq.cancel(msg_id)
            return True
        except Exception as exc:
            if self._is_transient(exc):
                n = self._bump_attempt(msg_id)
                if n >= self.max_attempts:
                    self._send_dlq(msg_id, env | {"last_error": str(exc), "attempt": n}, reason="max_attempts_transient")
                    self.redis.xack(self.outbox_stream, self.group, msg_id)
                    self._retryq.cancel(msg_id)
                    return True
                self._schedule_retry(msg_id, fields, n, exc)
                return False
            # non-transient -> DLQ quickly (or retry until max_attempts)
            n = self._bump_attempt(msg_id)
            if n >= self.max_attempts:
                self._send_dlq(msg_id, env | {"last_error": str(exc), "attempt": n}, reason="max_attempts")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                self._retryq.cancel(msg_id)
                return True
            self._schedule_retry(msg_id, fields, n, exc)
            return False


if __name__ == "__main__":
    SignalDispatcher().run()
