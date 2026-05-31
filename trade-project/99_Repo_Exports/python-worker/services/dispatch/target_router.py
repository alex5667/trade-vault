import json
import contextlib
from typing import Any

from common.decision_trace import DecisionTrace
from common.transient import is_transient_error
from services.dispatcher.key_utils import KeyUtils


class PermanentDeliveryError(RuntimeError):
    """
    A permanent, non-retriable delivery error.
    """


class TargetRouter:
    def __init__(self, config: Any, redis_client: Any, dual_client: Any, simple_client: Any, idempotency_store: Any, retry_scheduler: Any, dlq_writer: Any, logger: Any):
        self.config = config
        self.redis = redis_client
        self.dual_redis = dual_client
        self.simple_redis = simple_client
        self.idempotency_store = idempotency_store
        self.retry_scheduler = retry_scheduler
        self.dlq_writer = dlq_writer
        self.logger = logger

    @property
    def redis(self):
        return getattr(self, "_redis", None)
    
    @redis.setter
    def redis(self, val):
        self._redis = val

    def set_retry_scheduler(self, retry_scheduler: Any) -> None:
        self.retry_scheduler = retry_scheduler

    def targets_list(self, env: dict[str, Any]) -> list[str]:
        t = env.get("targets") or {}
        out: list[str] = []
        if t.get("notify"): out.append("notify")
        if t.get("signal_stream_payload"): out.append("signal_stream")
        if t.get("audit_payload"): out.append("audit")
        if t.get("manual_payload"): out.append("manual")
        if t.get("mt5_plan"): out.append("mt5_plan")
        if t.get("snapshot_payload") or t.get("snapshot"): out.append("snapshot")
        return out

    async def deliver_targets_with_retry(
        self,
        env: dict[str, Any],
        sid: str,
        *,
        targets: list[str] | None = None,
        base_attempts: dict[str, int] | None = None,
        _trace: DecisionTrace | None = None,
    ) -> None:
        targets_obj = env.get("targets") or {}
        meta = env.get("meta") or {}
        to_process = targets or self.targets_list(env)
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
                    ok=ok,
                    veto=False,
                    reason_code=reason_code or ("OK" if ok else "DELIVERY_ERROR"),
                    etype="gate",
                    extra={"err": err} if err else None,
                )
            except TypeError:
                with contextlib.suppress(Exception):
                    _trace.add(where="delivery", name=f"delivery_{target}", ok=ok, metrics={"err": err} if err else None)
            except Exception:
                pass

        for idx, target in enumerate(to_process):
            marker_client = self.idempotency_store.marker_client_for_target(target, dual_client, simple_client) or self.redis
            try:
                if await self.idempotency_store.marker_exists(marker_client, target, sid):
                    continue
            except Exception:
                pass

            if idx == 0 and isinstance(base_attempts, dict) and "__forced__" in base_attempts:
                attempt = base_attempts.get("__forced__") or 0
            else:
                attempt = ((base_attempts or {}).get(target, attempts_obj.get(target, 0)) or 0) + 1
            attempts_obj[target] = attempt

            try:
                await self.deliver_one_target(
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
                if hasattr(self.retry_scheduler, "schedule_target_retry"):
                    if hasattr(self.retry_scheduler.schedule_target_retry, "__code__") and self.retry_scheduler.schedule_target_retry.__code__.co_flags & 0x80:
                        await self.retry_scheduler.schedule_target_retry(
                            target=target, sid=sid, env=env, attempt=attempt, last_error=str(e)
                        )
                    else:
                        self.retry_scheduler.schedule_target_retry(
                            target=target, sid=sid, env=env, attempt=attempt, last_error=str(e)
                        )
                
                if not is_transient_error(e):
                    with contextlib.suppress(Exception):
                        if hasattr(self.dlq_writer, "send_target_dlq"):
                            if hasattr(self.dlq_writer.send_target_dlq, "__code__") and self.dlq_writer.send_target_dlq.__code__.co_flags & 0x80:
                                await self.dlq_writer.send_target_dlq(target, sid, env, reason="target_delivery_error", err=str(e))
                            else:
                                self.dlq_writer.send_target_dlq(target, sid, env, reason="target_delivery_error", err=str(e))
                _trace_delivery(target=target, ok=False, reason_code="DELIVERY_ERROR", err=str(e))

        if not any_failure:
            # Only mark done if ALL targets in the full list are now present in Redis
            all_required = self.targets_list(env)
            all_done = True
            for t in all_required:
                m_client = self.idempotency_store.marker_client_for_target(t, dual_client, simple_client) or self.redis
                try:
                    if not await self.idempotency_store.marker_exists(m_client, t, sid):
                        all_done = False
                        break
                except Exception:
                    all_done = False
                    break
            
            if all_done:
                with contextlib.suppress(Exception):
                    await self.idempotency_store.mark_env_done(self.redis, sid, env)

    async def deliver_one_target(
        self,
        *,
        env: dict[str, Any],
        sid: str,
        target: str,
        targets_obj: dict[str, Any],
        meta: dict[str, Any],
        dual_client: Any,
        simple_client: Any,
    ) -> None:
        import os
        if os.environ.get("TARGETS_MUTATION_GUARD") == "1":
            payload = targets_obj.get(f"{target}_payload")
            if isinstance(payload, dict):
                original_len = len(payload)
                try:
                    list(payload)
                except Exception:
                    pass
                if len(payload) != original_len:
                    raise RuntimeError("Mutation detected during serialization")

        if hasattr(self.idempotency_store, "deliver_one_target"):
            return await self.idempotency_store.deliver_one_target(
                env=env,
                sid=sid,
                target=target,
                targets_obj=targets_obj,
                meta=meta,
                dual_client=dual_client,
                simple_client=simple_client,
            )
        if not env.get("trace_id"):
            env["trace_id"] = sid
        client = self.idempotency_store.marker_client_for_target(target, dual_client, simple_client) or self.redis

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

        if payload and isinstance(payload, dict):
            if payload.get("type") == "delta_spike" or "delta" in payload:
                delta_val = payload.get("delta")
                delta_z_val = payload.get("delta_z") or payload.get("z")
                if delta_val is not None and delta_z_val is not None:
                    if self.logger:
                        self.logger.debug(f"✅ [{sid}] Payload closed: delta={delta_val:.4f}, z={delta_z_val:.4f}, target={target}")
                else:
                    if self.logger:
                        self.logger.warning(f"⚠️ [{sid}] Missing delta/z in payload: delta={delta_val}, z={delta_z_val}, target={target}")

        if target == "notify":
            notify_stream = getattr(self.config, "notify_stream", None)
            if not notify_stream:
                raise PermanentDeliveryError("notify missing config.notify_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError(f"notify missing targets.notify payload (got {type(payload)}, keys={list(targets_obj.keys()) if isinstance(targets_obj, dict) else 'not a dict'})")
            if not dual_client:
                raise Exception("notify missing dual_client redis")
            wrapped_payload = self._prepare_target_payload(payload, sid=sid, trace_id=env.get("trace_id"))
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not await self.idempotency_store.notify_idempotent(client, sid=sid, payload=fields):
                raise PermanentDeliveryError("notify_failed")
            return

        if target == "signal_stream":
            stream = meta.get("signal_stream") or getattr(self.config, "signal_stream", None)
            if not stream:
                raise Exception("signal_stream missing meta.signal_stream")
            if not isinstance(payload, dict):
                raise Exception("signal_stream missing targets.signal_stream_payload payload")
            
            # Ensure simple_client is available
            actual_simple = simple_client or self.simple_redis
            if not actual_simple:
                raise Exception("signal_stream missing simple_client redis")
            
            wrapped_payload = self._prepare_target_payload(payload, sid=sid, trace_id=env.get("trace_id"))
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not await self.idempotency_store.xadd_idempotent_atomic(
                client, target="signal_stream", sid=sid, stream=stream, fields=fields, maxlen=getattr(self.config, "signal_maxlen", 1000)
            ):
                raise PermanentDeliveryError("signal_stream_failed")
            return

        if target == "audit":
            stream = meta.get("audit_stream") or getattr(self.config, "audit_stream", None)
            if not stream:
                raise PermanentDeliveryError("missing_audit_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_audit_payload")
            wrapped_payload = self._prepare_target_payload(payload, sid=sid, trace_id=env.get("trace_id"))
            fields = {"payload": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not await self.idempotency_store.xadd_idempotent_atomic(
                client, target="audit", sid=sid, stream=stream, fields=fields, maxlen=getattr(self.config, "audit_maxlen", 1000)
            ):
                raise PermanentDeliveryError("audit_failed")
            return

        if target == "manual":
            stream = meta.get("manual_stream") or getattr(self.config, "manual_stream", None)
            if not stream:
                raise Exception("manual missing meta.manual_stream")
            if not isinstance(payload, dict):
                raise Exception("manual missing targets.manual_payload payload")
            
            # Ensure dual_client is available
            actual_dual = dual_client or self.dual_redis
            if not actual_dual:
                raise Exception("manual missing dual_client redis")
            
            wrapped_payload = self._prepare_target_payload(payload, sid=sid, trace_id=env.get("trace_id"))
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not await self.idempotency_store.xadd_idempotent_atomic(
                client, target="manual", sid=sid, stream=stream, fields=fields, maxlen=getattr(self.config, "manual_maxlen", 100)
            ):
                raise PermanentDeliveryError("manual_failed")
            return

        if target == "mt5_plan":
            if not getattr(self.config, "mt5_plans_stream", None):
                raise PermanentDeliveryError("missing_mt5_plans_stream")
            if not isinstance(payload, dict):
                raise PermanentDeliveryError("invalid_mt5_plan_payload")

            wrapper = {"plan": payload}
            payload_json = json.dumps(wrapper, ensure_ascii=False, separators=(",", ":"))
            fields = {"payload": payload_json}

            if not await self.idempotency_store.xadd_idempotent_atomic(
                client, target="mt5_plan", sid=sid, stream=getattr(self.config, "mt5_plans_stream", ""), fields=fields, maxlen=getattr(self.config, "mt5_plans_maxlen", 500)
            ):
                raise PermanentDeliveryError("mt5_plan_failed")
            return

        if target == "snapshot":
            key = meta.get("snap_key") or (f"{self.config.snapshot_prefix}:{sid}" if getattr(self.config, "snapshot_prefix", None) else None)
            if not key:
                return
            ttl = meta.get("snap_ttl") or getattr(self.config, "snapshot_ttl_sec", 21600)
            if payload is not None:
                wrapped_payload = self._prepare_target_payload(payload, sid=sid, trace_id=env.get("trace_id"))
                val = json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))
            else:
                val = ""
            if not await self.idempotency_store.setex_idempotent_atomic(
                client, target="snapshot", sid=sid, key=key, ttl_sec=ttl, value_json=val
            ):
                raise PermanentDeliveryError("snapshot_failed")
            return

    def _prepare_target_payload(self, original_payload: dict[str, Any], sid: str, trace_id: str | None) -> dict[str, Any]:
        p = original_payload.copy()
        if "sid" not in p:
            p["sid"] = sid
        if "trace_id" not in p and trace_id:
            p["trace_id"] = trace_id
        return p
