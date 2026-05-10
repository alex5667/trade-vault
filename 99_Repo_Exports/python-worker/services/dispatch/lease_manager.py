import uuid
import contextlib
from typing import Any

from services.dispatcher.key_utils import KeyUtils
from utils.time_utils import get_ny_time_millis


class LeaseManager:
    def __init__(self, config: Any, redis_client: Any, lua_scripts: Any, ctr: dict[str, int]):
        self.config = config
        self.redis = redis_client
        self.lua_scripts = lua_scripts
        self.ctr = ctr

    def try_acquire_lease(self, msg_id: str) -> bool:
        """
        True => можно обрабатывать.
        False => уже кто-то обрабатывает (не ACK, оставить pending).
        """
        try:
            key = KeyUtils.lease_key(self.config.msg_lease_prefix, msg_id)
            ok = self.redis.set(key, "1", nx=True, px=self.config.msg_lease_ttl_ms)
            if ok:
                return True
        except Exception:
            # если Redis совсем плохо — считаем transient и не трогаем message
            return False
        self.ctr["lease_contention"] += 1
        return False

    def release_lease(self, msg_id: str) -> None:
        with contextlib.suppress(Exception):
            key = KeyUtils.lease_key(self.config.msg_lease_prefix, msg_id)
            self.redis.delete(key)

    def _sid_lease_key(self, sid: str) -> str:
        return f"{self.config.sid_lease_prefix}:{sid}"

    def try_acquire_sid_lease(self, sid: str) -> str | None:
        """
        Token-based lease. Возвращает token если взяли lease, иначе None.
        """
        token = uuid.uuid4().hex
        key = self._sid_lease_key(sid)
        try:
            ok = self.redis.set(key, token, nx=True, px=int(self.config.sid_lease_ttl_ms))
            if ok:
                self.ctr["sid_lease_acquired"] += 1
                return token
        except Exception:
            pass
        return None

    def release_sid_lease(self, sid: str, token: str) -> None:
        with contextlib.suppress(Exception):
            self.lua_scripts.execute("release_lease", keys=[self._sid_lease_key(sid)], args=[token])

    def maybe_extend_sid_lease(self, sid: str, token: str, last_extend_ms: int) -> int:
        """
        Продлеваем lease каждые sid_lease_extend_every_ms (best-effort).
        """
        now_ms = get_ny_time_millis()
        if now_ms - int(last_extend_ms) < int(self.config.sid_lease_extend_every_ms):
            return last_extend_ms
        try:
            ok = self.lua_scripts.execute(
                "extend_lease",
                keys=[self._sid_lease_key(sid)],
                args=[token, str(int(self.config.sid_lease_ttl_ms))],
            )
            if int(ok or 0) == 1:
                self.ctr["sid_lease_extended"] += 1
                return now_ms
        except Exception:
            pass
        return last_extend_ms
