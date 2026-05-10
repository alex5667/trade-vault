import json
import contextlib
from typing import Any

from common.decision_trace import DecisionTrace
from common.transient import is_transient_error


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

    def deliver_targets_with_retry(
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
                    ok=bool(ok),
                    veto=False,
                    reason_code=str(reason_code or ("OK" if ok else "DELIVERY_ERROR")),
                    etype="gate",
                    extra={"err": str(err)} if err else None,
                )
            except TypeError:
                with contextlib.suppress(Exception):
                    _trace.add(where="delivery", name=f"delivery_{target}", ok=bool(ok), metrics={"err": str(err)} if err else None)
            except Exception:
                pass

        for idx, t in enumerate(to_process):
            target = str(t)
            marker_client = self.idempotency_store.marker_client_for_target(target, dual_client, simple_client) or self.redis
            try:
                if self.idempotency_store.marker_exists(marker_client, target, sid):
                    continue
            except Exception:
                pass

            if idx == 0 and isinstance(base_attempts, dict) and "__forced__" in base_attempts:
                attempt = int(base_attempts.get("__forced__") or 0)
            else:
                attempt = int((base_attempts or {}).get(target, attempts_obj.get(target, 0)) or 0) + 1
            attempts_obj[target] = int(attempt)

            try:
                self.deliver_one_target(
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
                self.retry_scheduler.schedule_target_retry(
                    target=target,
                    sid=sid,
                    env=env,
                    attempt=int(attempt),
                    last_error=str(e),
                )
                if not is_transient_error(e):
                    with contextlib.suppress(Exception):
                        self.dlq_writer.send_target_dlq(target, sid, env, reason="target_delivery_error", err=str(e))
                _trace_delivery(target=target, ok=False, reason_code="DELIVERY_ERROR", err=str(e))

        if not any_failure:
            with contextlib.suppress(Exception):
                self.redis.set(KeyUtils.env_done_key(self.config.env_done_prefix, sid), "1", ex=int(self.config.delivery_marker_ttl_sec), nx=True)

    def deliver_one_target(
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
                raise Exception("notify missing config.notify_stream")
            if not isinstance(payload, dict):
                raise Exception("notify missing targets.notify payload")
            if not dual_client:
                raise Exception("notify missing dual_client redis")
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not self.idempotency_store.notify_idempotent(client, sid=sid, payload=fields):
                raise PermanentDeliveryError("notify_failed")
            return

        if target == "signal_stream":
            stream = meta.get("signal_stream") or getattr(self.config, "signal_stream", None)
            if not stream:
                raise Exception("signal_stream missing meta.signal_stream")
            if not isinstance(payload, dict):
                raise Exception("signal_stream missing targets.signal_stream_payload payload")
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not self.idempotency_store.xadd_idempotent_atomic(
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
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"payload": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not self.idempotency_store.xadd_idempotent_atomic(
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
            wrapped_payload = payload.copy()
            wrapped_payload["sid"] = sid
            fields = {"data": json.dumps(wrapped_payload, ensure_ascii=False, separators=(",", ":"))}

            if not self.idempotency_store.xadd_idempotent_atomic(
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

            if not self.idempotency_store.xadd_idempotent_atomic(
                client, target="mt5_plan", sid=sid, stream=getattr(self.config, "mt5_plans_stream", ""), fields=fields, maxlen=getattr(self.config, "mt5_plans_maxlen", 500)
            ):
                raise PermanentDeliveryError("mt5_plan_failed")
            return

        if target == "snapshot":
            snapshot_prefix = getattr(self.config, "snapshot_prefix", None)
            if not snapshot_prefix:
                return
            key = f"{snapshot_prefix}:{sid}"
            val = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) if payload is not None else ""
            if not self.idempotency_store.setex_idempotent_atomic(
                client, target="snapshot", sid=sid, key=key, ttl_sec=self.config.snapshot_ttl_sec, value_json=val
            ):
                raise PermanentDeliveryError("snapshot_failed")
            return
