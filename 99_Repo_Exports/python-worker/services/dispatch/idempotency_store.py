import json
import os
import contextlib
from typing import Any

from services.dispatcher.delivery_helpers import DeliveryHelpers
from services.dispatcher.key_utils import KeyUtils
from services.dispatcher.observability import sd_fail_open


class IdempotencyStore:
    def __init__(self, config: Any, redis_client: Any, lua_scripts: Any, logger: Any, ctr: dict[str, int]):
        self.config = config
        self._redis = redis_client
        self.lua_scripts = lua_scripts
        self.logger = logger
        self.ctr = ctr

    @property
    def redis(self):
        return self._redis
    
    @redis.setter
    def redis(self, val):
        self._redis = val

    async def is_msg_done(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        if self.redis is None:
            return False
        try:
            v = await self.redis.get(KeyUtils.outbox_done_key(self.config.msg_done_prefix, msg_id))
            if v in (None, "", b""):
                return False
            if isinstance(v, bytes):
                v = v.decode("utf-8", "ignore")
            return str(v).strip() == "1"
        except Exception:
            return False

    async def mark_msg_done(self, msg_id: str) -> None:
        try:
            if self.redis is None:
                return
            await self.redis.set(KeyUtils.outbox_done_key(self.config.msg_done_prefix, msg_id), "1", ex=int(self.config.done_ttl_sec), nx=True)
        except Exception:
            return

    async def is_outbox_done(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        if self.redis is None:
            return False
        try:
            v = await self.redis.get(KeyUtils.outbox_done_key(f"{self.config.done_prefix}:msg", msg_id))
            if v in (None, "", b""):
                v2 = await self.redis.get(KeyUtils.done_key(self.config.done_prefix, msg_id))
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

    async def mark_outbox_done(self, msg_id: str) -> None:
        try:
            await self.redis.setex(KeyUtils.outbox_done_key(f"{self.config.done_prefix}:msg", msg_id), self.config.done_ttl_sec, "1")
            if os.getenv("SIGNAL_OUTBOX_WRITE_LEGACY_DONE", "1").lower() not in {"0", "false", "no"}:
                with contextlib.suppress(Exception):
                    await self.redis.setex(KeyUtils.done_key(self.config.done_prefix, msg_id), self.config.done_ttl_sec, "1")
        except Exception as e:
            def _incr(key: str) -> None:
                self.ctr[key] += 1
            sd_fail_open(
                self.logger,
                key="mark_outbox_done_error",
                err=e,
                incr_fn=_incr,
                metric_key=f"{self.config.metrics_prefix}:mark_outbox_done_errors_total",
            )

    async def mark_env_done(self, client: Any, sid: str, env: dict[str, Any]) -> None:
        try:
            if client is None:
                return
            await client.set(KeyUtils.env_done_key(self.config.env_done_prefix, sid), "1", ex=int(self.config.delivery_marker_ttl_sec), nx=True)
            if os.getenv("SIGNAL_OUTBOX_WRITE_LEGACY_DONE", "1").lower() not in {"0", "false", "no"}:
                with contextlib.suppress(Exception):
                    await client.set(KeyUtils.done_key(self.config.done_prefix, sid), "1", ex=int(self.config.delivery_marker_ttl_sec), nx=True)
        except Exception:
            return

    async def is_env_done(self, sid: str) -> bool:
        if not sid:
            return False
        if self.redis is None:
            return False
        try:
            v = await self.redis.get(KeyUtils.env_done_key(self.config.env_done_prefix, sid))
            if v in (None, "", b""):
                v = await self.redis.get(KeyUtils.done_key(self.config.done_prefix, sid))
            if v in (None, "", b""):
                return False
            return True
        except Exception:
            return False

    def marker_client_for_target(self, target: str, dual_client: Any, simple_client: Any) -> Any:
        if target in ("notify", "manual"):
            return dual_client or self.redis
        if target == "signal_stream":
            return simple_client or self.redis
        return self.redis

    async def marker_exists(self, client: Any, target: str, sid: str) -> bool:
        try:
            v = await client.get(DeliveryHelpers.delivery_key(self.config.marker_prefix, target, sid))
            return bool(v)
        except Exception:
            return False

    async def xadd_idempotent_atomic(self, client: Any, *, target: str, sid: str, stream: str,
                               fields: dict[str, Any], maxlen: int) -> bool:
        if not stream:
            return True
        marker = DeliveryHelpers.marker_key(self.config.marker_prefix, target, sid)
        argv: list[Any] = [str(self.config.delivery_marker_ttl_sec), str(maxlen)]
        for k, v in fields.items():
            argv.append(k)
            argv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = await self.lua_scripts.execute("xadd_and_mark", keys=[marker, stream], args=argv, client=client)
        if not res:
            return False
        code = int(res[0])
        if code == 1:
            return True
        if code == 0:
            return True
        raise RuntimeError(f"xadd_and_mark_failed code={code} target={target}")

    async def setex_idempotent_atomic(self, client: Any, *, target: str, sid: str, key: str,
                                ttl_sec: int, value_json: str) -> bool:
        if not key:
            return True
        marker = DeliveryHelpers.marker_key(self.config.marker_prefix, target, sid)
        res = await self.lua_scripts.execute(
            "setex_and_mark",
            keys=[marker, key],
            args=[str(self.config.delivery_marker_ttl_sec), str(ttl_sec), value_json],
            client=client
        )
        if not res:
            return False
        code = int(res[0])
        if code in (0, 1):
            return True
        raise RuntimeError(f"setex_and_mark_failed code={code} target={target}")

    async def xadd_idempotent(self, client: Any, *, target: str, sid: str, stream: str, fields: dict[str, Any], maxlen: int) -> bool:
        fv: list[str] = []
        for k, v in (fields or {}).items():
            fv.append(k)
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = await self.lua_scripts.execute(
            "xadd_fields_then_mark",
            keys=[DeliveryHelpers.marker_key(self.config.marker_prefix, target, sid), stream],
            args=[str(self.config.delivery_marker_ttl_sec), str(maxlen)] + fv,
            client=client
        )
        return bool(res and int(res[0]) in (0, 1))

    async def setex_idempotent(self, client: Any, *, target: str, sid: str, key: str, value_json: str, ttl_sec: int) -> bool:
        res = await self.lua_scripts.execute(
            "setex_then_mark",
            keys=[DeliveryHelpers.marker_key(self.config.marker_prefix, target, sid), key],
            args=[str(self.config.delivery_marker_ttl_sec), str(ttl_sec), value_json],
            client=client
        )
        return bool(res and int(res[0]) in (0, 1))

    async def notify_idempotent(self, client: Any, *, sid: str, payload: dict[str, Any]) -> bool:
        fv: list[str] = []
        for k, v in (payload or {}).items():
            fv.append(k)
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = await self.lua_scripts.execute(
            "notify_gate",
            keys=[DeliveryHelpers.marker_key(self.config.marker_prefix, "notify", sid), self.config.notify_stream, self.config.notify_signal_counter_key],
            args=[str(self.config.delivery_marker_ttl_sec), str(500000), str(self.config.notify_signal_every_n)] + fv,
            client=client
        )

        with contextlib.suppress(Exception):
             self.logger.info(f"[SignalGate] SID={sid} N={self.config.notify_signal_every_n} Result={res} (1=Sent, 0=Skipped)")

        return bool(res and int(res[0]) in (0, 1))
