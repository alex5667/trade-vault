import time
import contextlib
from typing import Any
from utils.time_utils import get_ny_time_millis


class MarkerRepair:
    def __init__(self, config: Any, redis_client: Any, logger: Any, ctr: dict[str, int]):
        self.config = config
        self.redis = redis_client
        self.logger = logger
        self.ctr = ctr
        self._last_maint_mono = 0.0
        self._last_repair_mono = 0.0
        self._last_marker_repair_mono = 0.0
        self._last_janitor = 0.0
        self._scan_cursor_markers = 0
        self._scan_cursor_done = 0
        self._repair_cursor = 0

    async def maint_scan_prefix(self, prefix: str, cursor: int) -> int:
        try:
            res = await self.redis.scan(cursor=cursor, match=f"{prefix}*", count=self.config.maintenance_scan_count)
            cursor2, keys = res
        except Exception:
            return cursor
        now_ms = get_ny_time_millis()
        ttl_cap = int(self.config.delivery_marker_ttl_sec)
        for k in keys or []:
            try:
                ttl = int(await self.redis.ttl(k))
            except Exception:
                continue
            if ttl == -1:
                try:
                    v = await self.redis.get(k)
                    v_ms = int(v) if v and str(v).isdigit() else 0
                except Exception:
                    v_ms = 0
                if v_ms > 0 and (now_ms - v_ms) > (ttl_cap * 1000 * 2):
                    with contextlib.suppress(Exception):
                        await self.redis.delete(k)
                else:
                    with contextlib.suppress(Exception):
                        await self.redis.expire(k, ttl_cap)
            elif ttl > (ttl_cap * 2):
                with contextlib.suppress(Exception):
                    await self.redis.expire(k, ttl_cap)
        return int(cursor2 or 0)

    async def maybe_maintenance(self) -> None:
        now = time.monotonic()
        if (now - self._last_maint_mono) * 1000 < self.config.maintenance_every_ms:
            return
        self._last_maint_mono = now
        self._scan_cursor_markers = await self.maint_scan_prefix(f"{self.config.marker_prefix}:", self._scan_cursor_markers)
        self._scan_cursor_done = await self.maint_scan_prefix(f"{self.config.done_prefix}:", self._scan_cursor_done)

    async def repair_orphan_markers_best_effort(self) -> None:
        now = time.monotonic()
        if now - self._last_repair_mono < float(self.config.orphan_repair_every_sec):
            return
        self._last_repair_mono = now
        prefixes = (self.config.marker_prefix, self.config.done_prefix)
        try:
            for pref in prefixes:
                res = await self.redis.scan(
                    cursor=self._repair_cursor,
                    match=f"{pref}:*",
                    count=int(self.config.marker_repair_batch),
                )
                cursor, keys = res
                self._repair_cursor = int(cursor or 0)
                if not keys:
                    continue
                repaired = 0
                for k in keys:
                    try:
                        ttl = await self.redis.ttl(k)
                        if int(ttl) < 0:
                            await self.redis.expire(k, int(self.config.delivery_marker_ttl_sec))
                            repaired += 1
                    except Exception:
                        continue
                if repaired:
                    self.ctr["marker_repaired"] += repaired
        except Exception:
            return

    async def maybe_repair_marker_ttls(self) -> None:
        if not self.redis:
            return
        now = time.monotonic()
        if now - self._last_marker_repair_mono < float(self.config.marker_repair_every_sec):
            return
        self._last_marker_repair_mono = now
        try:
            cursor = 0
            scanned = 0
            pattern = f"{self.config.delivery_marker_prefix}:*"
            while scanned < self.config.marker_repair_scan_count:
                res = await self.redis.scan(cursor=cursor, match=pattern, count=10000)
                cursor, keys = res
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for k in keys:
                    scanned += 1
                    try:
                        ttl = await self.redis.ttl(k)
                        if ttl == -1:
                            await self.redis.expire(k, int(self.config.delivery_marker_ttl_sec))
                    except Exception:
                        continue
                    if scanned >= self.config.marker_repair_scan_count:
                        break
                if cursor == 0:
                    break
        except Exception as e:
            self.logger.warning("marker repair failed: %s", e)

    async def janitor(self) -> None:
        if not self.config.janitor_enabled:
            return
        now = time.monotonic()
        if now - self._last_janitor < self.config.janitor_every_sec:
            return
        self._last_janitor = now
        try:
            cursor = 0
            scanned = 0
            pattern = f"{self.config.marker_prefix}:*"
            while scanned < self.config.janitor_scan_count:
                res = await self.redis.scan(cursor=cursor, match=pattern, count=10000)
                cursor, keys = res
                for k in keys or []:
                    scanned += 1
                    try:
                        ttl = int(await self.redis.ttl(k))
                        if ttl < 0:
                            await self.redis.expire(k, self.config.marker_ttl_sec)
                    except Exception:
                        continue
                if scanned >= self.config.janitor_scan_count:
                    break
                if cursor == 0:
                    break
        except Exception:
            pass
