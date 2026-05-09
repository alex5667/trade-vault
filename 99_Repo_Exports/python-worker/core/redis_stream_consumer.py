from __future__ import annotations

"""
Utility helpers for working with Redis Streams consumer groups.

Provides lightweight wrappers that encapsulate common patterns:
 - ensure consumer groups exist (with optional recreation)
 - perform XREADGROUP calls with consistent defaults
 - ACK processed messages

Available helpers:
 - SyncRedisStreamHelper  — for redis.Redis clients (blocking, threaded code)
 - AsyncRedisStreamHelper — for redis.asyncio clients
"""


import asyncio
import logging
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from core.redis_keys import RedisStreams as RS
from core.retention import MAXLEN_GLOBAL
import contextlib

# redis-py is optional in unit-test environments.
try:
    import redis  # type: ignore
    import redis.asyncio as aioredis  # type: ignore
    from redis.exceptions import ConnectionError, ResponseError, TimeoutError  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore
    aioredis = None  # type: ignore

    class ResponseError(Exception):
        pass

    class ConnectionError(Exception):
        pass

    class TimeoutError(Exception):
        pass

logger = logging.getLogger(__name__)


def _safe_disconnect_pool(pool: Any) -> None:
    """Safely disconnects a redis pool in the background without raising unretrieved asyncio task exceptions."""
    try:
        task = asyncio.create_task(pool.disconnect())
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    except Exception:
        pass


def _decode_any(x: Any) -> str:
    """Safe decode for bytes/str/None."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    try:
        return str(x)
    except Exception:
        return ""


def _fields_to_dict(fields: Any) -> dict[str, Any]:
    """Normalize redis stream entry fields to a dict.

    redis-py usually returns a dict, but raw RESP / execute_command can return:
      - flat list/tuple: [k1, v1, k2, v2, ...]
      - list of pairs: [(k1, v1), (k2, v2), ...]
    Keys are decoded to strings; values are returned as-is.
    """
    if fields is None:
        return {}
    if isinstance(fields, dict):
        # ensure keys are strings
        try:
            return { _decode_any(k): v for k, v in fields.items() }
        except Exception:
            return dict(fields)
    if isinstance(fields, (list, tuple)):
        if fields and all(isinstance(it, (list, tuple)) and len(it) >= 2 for it in fields):
            out: dict[str, Any] = {}
            for k, v, *_ in fields:
                out[_decode_any(k)] = v
            return out
        out2: dict[str, Any] = {}
        it = list(fields)
        for i in range(0, len(it) - 1, 2):
            out2[_decode_any(it[i])] = it[i + 1]
        return out2
    return {}


def _is_unsupported_xautoclaim(err: Exception) -> bool:
    """Detects if XAUTOCLAIM is not available or has wrong argument count."""
    s = str(err).lower()
    return ("unknown command" in s) or ("wrong number of arguments" in s) or ("no such command" in s)


def _parse_xpending_summary(res: Any) -> int:
    """
    redis-py может вернуть:
      - dict: {"pending": int, "min": ..., "max": ..., "consumers": [...]}
      - tuple/list: (pending, min, max, consumers)
    Возвращаем только pending count.
    """
    if res is None:
        return 0

    # dict format
    if isinstance(res, dict):
        v = res.get("pending", 0)
        try:
            return int(v)
        except Exception:
            return 0

    # tuple/list format
    if isinstance(res, (tuple, list)) and len(res) >= 1:
        try:
            return int(res[0])
        except Exception:
            return 0

    return 0


def _parse_xpending_consumers(res: Any) -> dict[str, int]:
    """
    XPENDING summary может вернуть consumers:
      - dict: {"consumers": [{"name": "...", "pending": 12}, ...]} или {"consumers": [["c1", 12], ...]}
      - tuple/list: (pending, min, max, consumers) где consumers = [[name, pending], ...]
    """
    out: dict[str, int] = {}
    if res is None:
        return out

    consumers = None
    if isinstance(res, dict):
        consumers = res.get("consumers")
    elif isinstance(res, (tuple, list)) and len(res) >= 4:
        consumers = res[3]

    if not consumers:
        return out

    # list of dicts
    if isinstance(consumers, list) and consumers and isinstance(consumers[0], dict):
        for c in consumers:
            try:
                name = (c.get("name", "") or "")
                pending = int(c.get("pending", 0) or 0)
                if name:
                    out[name] = pending
            except Exception:
                continue
        return out

    # list of [name, pending]
    if isinstance(consumers, list):
        for item in consumers:
            try:
                if isinstance(item, (tuple, list)) and len(item) >= 2:
                    name = item[0]
                    if isinstance(name, (bytes, bytearray)):
                        name = name.decode("utf-8", errors="ignore")
                    name_s = str(name)
                    pending = int(item[1] or 0)
                    if name_s:
                        out[name_s] = pending
            except Exception:
                continue
    return out


def _parse_xpending_details(res: Any) -> dict[str, Any]:
    """
    Normalize XPENDING summary into:
      {"pending": int, "min": str|None, "max": str|None, "consumers": [{"name": str, "pending": int}]}
    """
    out: dict[str, Any] = {"pending": 0, "min": None, "max": None, "consumers": []}
    if res is None:
        return out
    # dict format
    if isinstance(res, dict):
        out["pending"] = int(res.get("pending", 0) or 0)
        out["min"] = res.get("min")
        out["max"] = res.get("max")
        cons = res.get("consumers") or []
        normalized = []
        for c in cons:
            if isinstance(c, dict):
                normalized.append({"name": (c.get("name", "")), "pending": int(c.get("pending", 0) or 0)})
            elif isinstance(c, (list, tuple)) and len(c) >= 2:
                normalized.append({"name": str(c[0]), "pending": int(c[1] or 0)})
        out["consumers"] = normalized
        return out
    # tuple/list format: (pending, min, max, consumers)
    if isinstance(res, (tuple, list)) and len(res) >= 4:
        try:
            out["pending"] = int(res[0] or 0)
        except Exception:
            out["pending"] = 0
        out["min"] = res[1]
        out["max"] = res[2]
        cons = res[3] or []
        normalized = []
        for c in cons:
            if isinstance(c, dict):
                normalized.append({"name": (c.get("name", "")), "pending": int(c.get("pending", 0) or 0)})
            elif isinstance(c, (list, tuple)) and len(c) >= 2:
                normalized.append({"name": str(c[0]), "pending": int(c[1] or 0)})
        out["consumers"] = normalized
        return out
    # minimal fallback
    out["pending"] = _parse_xpending_summary(res)
    return out

def _parse_xpending_consumers(res: Any) -> dict[str, int]:
    """
    Возвращает dict consumer -> pending_count из XPENDING summary.

    redis-cli: XPENDING key group
      -> [count, min, max, [[consumer, count], ...]]
    redis-py иногда:
      -> dict {"pending":..., "min":..., "max":..., "consumers":[{"name":..,"pending":..}, ...]}
    """
    out: dict[str, int] = {}
    if res is None:
        return out
    try:
        if isinstance(res, dict):
            cons = res.get("consumers") or []
            for c in cons:
                if isinstance(c, dict):
                    name = str(c.get("name") or c.get("consumer") or "")
                    if not name:
                        continue
                    try:
                        out[name] = int(c.get("pending", 0))
                    except Exception:
                        out[name] = 0
                elif isinstance(c, (list, tuple)) and len(c) >= 2:
                    out[str(c[0])] = int(c[1])
            return out
        if isinstance(res, (list, tuple)) and len(res) >= 4:
            cons = res[3] or []
            for c in cons:
                if isinstance(c, (list, tuple)) and len(c) >= 2:
                    out[str(c[0])] = int(c[1])
            return out
    except Exception:
        return {}
    return out


@dataclass
class StreamMsg:
    """Message from Redis stream."""
    stream: str
    msg_id: str
    fields: dict[str, Any]


def _normalize_streams(streams: Mapping[str, str | None] | Iterable[str]) -> dict[str, str]:
    """
    Normalizes different stream specifications to a dict {stream_name: id}.

    Args:
        streams: Either a mapping (stream -> id) or an iterable of stream names.

    Returns:
        Dict[str, str]: Mapping ready for xreadgroup/xread operations.
    """
    if isinstance(streams, Mapping):
        return {
            name: stream_id if stream_id not in (None, "", ">") else ">"
            for name, stream_id in streams.items()
        }
    return dict.fromkeys(streams, ">")


class SyncRedisStreamHelper:
    """Helper for synchronous (blocking) Redis stream consumers."""

    def __init__(
        self,
        client: redis.Redis,
        group: str,
        consumer: str,
        *,
        recovery_start_id: str = "$",
    ) -> None:
        self.client = client
        self.group = group
        self.consumer = consumer
        # IMPORTANT:
        # - generic consumers often want "$" on recovery (read only fresh after group recreation)
        # - outbox MUST use "0" (do not skip already-in-stream messages)
        self.recovery_start_id = recovery_start_id
        # Keep backward compatibility
        self.group_start_id = recovery_start_id

    def ensure_group(self, stream: str, *, recreate: bool = False, start_id: str | None = None,
                     stop_event: Any = None) -> None:
        """Ensures that a consumer group exists for the stream."""
        if start_id in (None, "", "$") and self.group_start_id:
            start_id = self.group_start_id
        if recreate:
            with contextlib.suppress(ResponseError):
                self.client.xgroup_destroy(stream, self.group)

        max_retries = 30  # Retry for up to 30 attempts
        retry_count = 0

        while retry_count < max_retries:
            if stop_event is not None and stop_event.is_set():
                logger.info("Stop requested during group creation for %s", stream)
                return

            try:
                sid = start_id or self.recovery_start_id or "$"
                self.client.xgroup_create(stream, self.group, id=sid, mkstream=True)
                return
            except ResponseError as exc:
                error_msg = str(exc).lower()
                if "busygroup" in error_msg:
                    return
                if "loading" in error_msg:
                    retry_count += 1
                    wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30 seconds
                    logger.warning("⚠️ Redis загружает данные в память (попытка %d/%d), ждём %d сек...",
                                 retry_count, max_retries, wait_time)
                    if stop_event is None:
                        time.sleep(wait_time)
                    else:
                        stop_event.wait(timeout=wait_time)
                    continue
                else:
                    raise

        # If we exhausted all retries
        logger.error("❌ Не удалось создать consumer group после %d попыток: Redis всё ещё загружает данные", max_retries)
        raise RuntimeError(f"Redis is still loading dataset after {max_retries} attempts")

    def ensure_groups(
        self,
        streams: Iterable[str],
        *,
        recreate: bool = False,
        start_id: str = "$",
        stop_event: Any = None,
    ) -> None:
        """Ensures consumer groups exist for all provided streams."""
        for stream in streams:
            if stop_event is not None and stop_event.is_set():
                break
            self.ensure_group(stream, recreate=recreate, start_id=start_id, stop_event=stop_event)

    def ensure_group_for_outbox(self, stream: str, *, recreate: bool = False, stop_event: Any = None) -> None:
        """
        Outbox MUST be created from '0' to avoid losing already-enqueued messages
        if group is created/recovered after stream already has data.
        """
        self.ensure_group(stream, recreate=recreate, start_id="0", stop_event=stop_event)

    def read(
        self,
        streams: Mapping[str, str | None] | Iterable[str],
        *,
        count: int,
        block: int,
        recover_start_id: str | None = None,
    ):
        """Wrapper around XREADGROUP that normalizes stream specs."""
        normalized = _normalize_streams(streams)
        if recover_start_id in (None, ""):
            recover_start_id = self.recovery_start_id
        # Try-catch with retries for ConnectionError
        max_retries = 3
        attempt = 0
        while True:
            try:
                return self.client.xreadgroup(
                    self.group,
                    self.consumer,
                    normalized,
                    count=count,
                    block=block,
                )
            except Exception as exc:
                exc_str = str(exc).lower()
                if type(exc).__name__ == "BusyLoadingError" or "loading" in exc_str:
                    logger.warning("⏳ Redis is loading dataset in memory, waiting 5s before xreadgroup...")
                    time.sleep(5.0)
                    continue
                if isinstance(exc, (ConnectionError, TimeoutError)):
                    if attempt < max_retries - 1:
                        attempt += 1
                        time.sleep(0.5)
                        continue
                    raise exc
                if isinstance(exc, ResponseError):
                    if "nogroup" not in exc_str:
                        raise exc
                # Автовосстановление: создаём группу и пробуем ещё раз
                for stream_name in normalized.keys():
                    try:
                        self.client.xgroup_create(stream_name, self.group, id=recover_start_id, mkstream=True)
                    except ResponseError as create_exc:
                        if "BUSYGROUP" not in str(create_exc):
                            raise
                # Retry once after group creation
                return self.client.xreadgroup(
                    self.group,
                    self.consumer,
                    normalized,
                    count=count,
                    block=block,
                )

    def pending_details(self, stream: str) -> dict[str, Any]:
        """
        XPENDING summary + consumers list (для метрик "pending-by-consumer").
        Возвращает dict:
          {"pending": int, "min": str|None, "max": str|None, "consumers":[{"name":str,"pending":int},...]}
        """
        if not stream:
            return {"pending": 0, "min": None, "max": None, "consumers": []}

        def _d(x: Any) -> str:
            return x if isinstance(x, str) else x.decode("utf-8", errors="ignore")

        try:
            res = self.client.xpending(stream, self.group)
        except ResponseError as exc:
            s = str(exc)
            if "NOGROUP" in s or "No such key" in s or "no such key" in s:
                return {"pending": 0, "min": None, "max": None, "consumers": []}
            raise
        except Exception:
            return {"pending": 0, "min": None, "max": None, "consumers": []}

        out: dict[str, Any] = {"pending": 0, "min": None, "max": None, "consumers": []}

        if isinstance(res, dict):
            out["pending"] = int(res.get("pending", 0) or 0)
            out["min"] = _d(res.get("min")) if res.get("min") else None
            out["max"] = _d(res.get("max")) if res.get("max") else None
            cons = res.get("consumers") or []
            arr = []
            for c in cons:
                try:
                    name = _d(c.get("name")) if isinstance(c, dict) else _d(c[0])
                    cnt = int(c.get("pending", 0)) if isinstance(c, dict) else int(c[1])
                    arr.append({"name": name, "pending": cnt})
                except Exception:
                    continue
            out["consumers"] = arr
            return out

        if isinstance(res, (tuple, list)) and len(res) >= 4:
            try:
                out["pending"] = int(res[0] or 0)
            except Exception:
                out["pending"] = 0
            out["min"] = _d(res[1]) if res[1] else None
            out["max"] = _d(res[2]) if res[2] else None
            cons = res[3] or []
            arr = []
            for c in cons:
                try:
                    # redis returns [(consumer_name, pending_count), ...]
                    name = _d(c[0])
                    cnt = int(c[1] or 0)
                    arr.append({"name": name, "pending": cnt})
                except Exception:
                    continue
            out["consumers"] = arr
            return out

        return out

    def consumers_info(self, stream: str) -> list[dict[str, Any]]:
        """
        XINFO CONSUMERS <stream> <group>
        Возвращает list[dict] (name,pending,idle,...) в decode_responses-совместимом виде.
        """
        if not stream:
            return []

        def _d(x: Any) -> Any:
            if isinstance(x, (bytes, bytearray)):
                return x.decode("utf-8", errors="ignore")
            return x

        try:
            res = self.client.xinfo_consumers(stream, self.group)
        except Exception:
            return []

        out: list[dict[str, Any]] = []
        for row in res or []:
            if isinstance(row, dict):
                out.append({str(_d(k)): _d(v) for k, v in row.items()})
            else:
                # safety: try to coerce list/tuple into dict-like pairs
                try:
                    d = {}
                    for i in range(0, len(row), 2):
                        d[str(_d(row[i]))] = _d(row[i + 1])
                    out.append(d)
                except Exception:
                    continue
        return out

    def pending_oldest_idle_ms(self, stream: str, *, sample: int = 1) -> int:
        """
        XPENDING RANGE: берём самое старое pending и возвращаем idle_ms.
        Нужен для диагностики stuck pending.
        """
        if not stream:
            return 0
        try:
            # redis-py: xpending_range(name, groupname, min, max, count, consumername=None, idle=None)
            items = self.client.xpending_range(stream, self.group, "-", "+", sample)
        except Exception:
            return 0

        # items: [{'message_id':..., 'consumer':..., 'time_since_delivered':..., 'times_delivered':...}, ...]
        if not items:
            return 0
        try:
            it = items[0]
            if isinstance(it, dict):
                return int(it.get("time_since_delivered") or 0)
        except Exception:
            pass
        return 0

    def read_pending_self(self, stream: str, *, count: int = 200) -> Any:
        """
        Reads PEL entries (pending) for THIS consumer via XREADGROUP with ID "0".
        This does NOT claim from other consumers (that's XAUTOCLAIM).
        """
        if not stream:
            return []
        try:
            return self.client.xreadgroup(
                self.group,
                self.consumer,
                {stream: "0"},
                count=count,
                block=0,
            )
        except Exception:
            return []

    # read_own_pending: XREADGROUP STREAMS stream "0" (own PEL for fast retry)
    # Call via: helper.read({stream: "0"}, count=..., block=0)

    def read_new(
        self,
        streams: Iterable[str],
        *,
        count: int,
        block_ms: int,
        recover_start_id: str | None = None,
    ) -> list[StreamMsg]:
        """
        Reads new messages (>) from streams and returns list of StreamMsg.

        Args:
            streams: List of stream names
            count: Maximum number of messages to read
            block_ms: Block time in milliseconds

        Returns:
            List of StreamMsg objects
        """
        streams_dict = {s: ">" for s in streams if s}
        if not streams_dict:
            return []

        # from redis.exceptions import ResponseError  <-- Removed local import


        max_retries = 3
        retry_delay = 0.5
        res = None  # Initialize res to avoid UnboundLocalError
        success = False  # Flag to track successful read after group creation

        attempt = 0
        while True:
            try:
                res = self.client.xreadgroup(
                    self.group,
                    self.consumer,
                    streams_dict,
                    count=count,
                    block=block_ms,
                )
                success = True
                break  # Success - exit retry loop
            except Exception as exc:
                exc_str = str(exc).lower()
                if type(exc).__name__ == "BusyLoadingError" or "loading" in exc_str:
                    logger.warning("⏳ Redis is loading dataset in memory, waiting 5s before read_new...")
                    time.sleep(5.0)
                    continue
                if isinstance(exc, (ConnectionError, TimeoutError)):
                    if attempt < max_retries - 1:
                        attempt += 1
                        time.sleep(retry_delay)
                        continue
                    raise exc
                if isinstance(exc, ResponseError):
                    if "nogroup" not in exc_str:
                        raise exc
                    # If it IS a NOGROUP error, break to auto-create the consumer group
                    break
                # If it's not a NOGROUP error, re-raise
                raise exc

        if not success and res is None:
            # We broke out because of NOGROUP error
            exc = ResponseError("NOGROUP")
            # If it IS a NOGROUP error, auto-create the consumer group
            logger.warning(f"⚠️ NOGROUP error for streams {list(streams_dict.keys())}: {exc}. Auto-creating group.")

            rid = recover_start_id if recover_start_id not in (None, "") else self.group_start_id
            # Auto-create consumer group if it doesn't exist
            # NOTE: rid controls whether we can replay (e.g. outbox must use "0")
            # Handle race conditions: multiple workers may try to create the same group
            for s in streams_dict:
                max_create_retries = 3
                for create_attempt in range(max_create_retries):
                    try:
                        self.client.xgroup_create(s, self.group, id=rid, mkstream=True)
                        logger.info(f"✅ Created consumer group '{self.group}' for stream '{s}'")
                        break  # Success - exit retry loop
                    except ResponseError as create_exc:
                        error_str = str(create_exc)
                        if "BUSYGROUP" in error_str:
                            # Group already exists (created by another worker) - this is fine
                            logger.debug(f"ℹ️ Consumer group '{self.group}' already exists for stream '{s}' (race condition)")
                            break  # Success - exit retry loop
                        elif create_attempt < max_create_retries - 1:
                            # Transient error - retry with backoff
                            wait_time = 0.2 * (create_attempt + 1)
                            logger.warning(f"⚠️ Failed to create group for {s} (attempt {create_attempt + 1}/{max_create_retries}): {create_exc}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            continue
                        else:
                            # Final attempt failed - log and raise
                            logger.error(f"❌ Failed to create group for {s} after {max_create_retries} attempts: {create_exc}")
                            raise

            # Small delay to ensure Redis has processed the group creation
            time.sleep(0.1)

            # Retry after group creation with additional error handling
            retry_after_create_max = 3
            retry_after_create_delay = 0.2
            for retry_attempt in range(retry_after_create_max):
                try:
                    res = self.client.xreadgroup(
                        self.group,
                        self.consumer,
                        streams_dict,
                        count=count,
                        block=block_ms,
                    )
                    logger.info("✅ Successfully read from streams after group creation")
                    success = True
                    break  # Success - exit inner retry loop
                except (ConnectionError, TimeoutError) as retry_exc:
                    if retry_attempt < retry_after_create_max - 1:
                        logger.warning(f"⚠️ Connection error after group creation (attempt {retry_attempt + 1}/{retry_after_create_max}): {retry_exc}. Retrying...")
                        time.sleep(retry_after_create_delay * (retry_attempt + 1))
                        continue
                    logger.error(f"❌ Connection error after group creation after {retry_after_create_max} attempts: {retry_exc}")
                    raise
                except ResponseError as retry_exc:
                    retry_exc_str = str(retry_exc)
                    # Handle various Redis errors that might occur after group creation
                    if "NOGROUP" in retry_exc_str:
                        # Group was deleted between creation and read - recreate
                        logger.warning(f"⚠️ NOGROUP error after group creation (attempt {retry_attempt + 1}/{retry_after_create_max}): {retry_exc}. Recreating group...")
                        for s in streams_dict:
                            recreate_retries = 2
                            for recreate_attempt in range(recreate_retries):
                                try:
                                    self.client.xgroup_create(s, self.group, id=rid, mkstream=True)
                                    break  # Success
                                except ResponseError as recreate_exc:
                                    error_str = str(recreate_exc)
                                    if "BUSYGROUP" in error_str:
                                        # Group exists - fine
                                        break
                                    elif recreate_attempt < recreate_retries - 1:
                                        time.sleep(0.2)
                                        continue
                                    else:
                                        logger.error(f"❌ Failed to recreate group for {s} after {recreate_retries} attempts: {recreate_exc}")
                        time.sleep(retry_after_create_delay * (retry_attempt + 1))
                        continue
                    elif "no such key" in retry_exc_str.lower() or "No such key" in retry_exc_str:
                        # Stream doesn't exist - try to create it with an initial message
                        logger.warning(f"⚠️ Stream not found after group creation (attempt {retry_attempt + 1}/{retry_after_create_max}): {retry_exc}. Creating stream...")
                        for s in streams_dict:
                            try:
                                # Create stream with an initial message if it doesn't exist
                                self.client.xadd(s, {"init": "stream_created"}, maxlen=1, approximate=True)
                                # Recreate group to ensure it's properly linked
                                try:
                                    self.client.xgroup_create(s, self.group, id=rid, mkstream=True)
                                except ResponseError:
                                    pass  # Group might already exist
                            except Exception as stream_exc:
                                logger.warning(f"⚠️ Could not create stream {s}: {stream_exc}")
                        time.sleep(retry_after_create_delay * (retry_attempt + 1))
                        continue
                    elif retry_attempt < retry_after_create_max - 1:
                        # Other transient errors - retry
                        logger.warning(f"⚠️ Error after group creation (attempt {retry_attempt + 1}/{retry_after_create_max}): {retry_exc}. Retrying...")
                        time.sleep(retry_after_create_delay * (retry_attempt + 1))
                        continue
                    else:
                        # Fatal error after all retries
                        logger.error(f"❌ Error after group creation after {retry_after_create_max} attempts: {retry_exc}")
                        raise

            # If we successfully read after group creation, we just fall through to the message extraction

        # res: [(stream_name, [(msg_id, {field: value}), ...]), ...]
        msgs: list[StreamMsg] = []
        for stream_name, items in res or []:
            stream_str = (
                stream_name
                if isinstance(stream_name, str)
                else stream_name.decode("utf-8", errors="ignore")
            )
            for msg_id, fields in items or []:
                msg_id_str = (
                    msg_id if isinstance(msg_id, str) else msg_id.decode("utf-8", errors="ignore")
                )

                # Normalize fields: bytes -> str
                fields_dict: dict[str, Any] = {}
                for k, v in (fields or {}).items():
                    k_str = k if isinstance(k, str) else k.decode("utf-8", errors="ignore")
                    if isinstance(v, str):
                        v_str = v
                    elif isinstance(v, bytes):
                        v_str = v.decode("utf-8", errors="ignore")
                    else:
                        v_str = str(v)
                    fields_dict[k_str] = v_str

                msgs.append(StreamMsg(stream=stream_str, msg_id=msg_id_str, fields=fields_dict))

        return msgs

    def ack(self, stream: str, message_id: str) -> None:
        """ACKs a processed message."""
        self.client.xack(stream, self.group, message_id)

    def ack_many(self, stream: str, message_ids: list[str]) -> None:
        """ACKs multiple processed messages."""
        if not message_ids:
            return
        self.client.xack(stream, self.group, *message_ids)

    def pending_len(self, stream: str) -> int:
        """Best-effort XPENDING summary pending count."""
        if not stream:
            return 0
        try:
            res = self.client.xpending(stream, self.group)
        except Exception:
            return 0
        if isinstance(res, dict):
            try:
                return int(res.get("pending", 0) or 0)
            except Exception:
                return 0
        if isinstance(res, (tuple, list)) and len(res) >= 1:
            try:
                return int(res[0] or 0)
            except Exception:
                return 0
        return 0

    def pending_by_consumer(self, stream: str) -> dict[str, int]:
        """Best-effort XPENDING details: pending counts per consumer."""
        if not stream:
            return {}
        try:
            res = self.client.xpending(stream, self.group)
        except Exception:
            return {}
        out: dict[str, int] = {}
        if isinstance(res, dict):
            consumers = res.get("consumers") or []
            for c in consumers:
                try:
                    name = (c.get("name", "")) if isinstance(c, dict) else ""
                    cnt = int(c.get("pending", 0) or 0) if isinstance(c, dict) else 0
                    if name:
                        out[name] = cnt
                except Exception:
                    continue
            return out
        if isinstance(res, (tuple, list)) and len(res) >= 4:
            consumers = res[3] or []
            for item in consumers:
                try:
                    if isinstance(item, (tuple, list)) and len(item) >= 2:
                        n, p = item[0], item[1]
                        name = n if isinstance(n, str) else n.decode("utf-8", errors="ignore")
                        out[name] = int(p or 0)
                    elif isinstance(item, dict):
                        name = (item.get("name", ""))
                        if name:
                            out[name] = int(item.get("pending", 0) or 0)
                except Exception:
                    continue
            return out
        return {}

    def add_dlq(self, stream: str, fields: dict[str, Any]) -> str:
        """Adds a message to the Dead Letter Queue stream."""
        # Можно добавить maxlen=100_000 если нужно ограничить рост DLQ
        return self.client.xadd(stream, fields, maxlen=MAXLEN_GLOBAL, approximate=True)

    def add_to_dlq(self, payload: dict[str, Any]) -> str:
        """Adapter for MessageHandler compatibility."""
        dlq_stream = os.getenv("ORDERFLOW_DLQ_STREAM", RS.DLQ_ORDERFLOW)
        return self.add_dlq(dlq_stream, payload)

    def claim_pending(
        self,
        stream: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
        create_group_start_id: str = "$",
    ) -> tuple[str, list[StreamMsg]]:
        """
        Claims pending messages using XAUTOCLAIM.
        Auto-creates consumer group if NOGROUP error occurs.

        Returns:
            Tuple of (next_start_id, list of StreamMsg)
        """
        if not stream:
            return start_id, []

        # 1) Try to get response
        res = None
        try:
            # Try high-level method first
            res = self.client.xautoclaim(
                name=stream,
                groupname=self.group,
                consumername=self.consumer,
                min_idle_time=min_idle_ms,
                start_id=start_id,
                count=count,
            )
        except ResponseError as e:
            err_s = str(e)
            if "NOGROUP" in err_s:
                # Auto-create group (rid controls replay semantics)
                try:
                    self.client.xgroup_create(stream, self.group, id=self.group_start_id, mkstream=True)
                except ResponseError as create_exc:
                    if "BUSYGROUP" not in str(create_exc):
                        logger.warning("Failed to create consumer group for %s: %s", stream, create_exc)
                    # Even if group created, this call failed. We can try one more time or just let MainLoop try next tick.
                    # Best effort: attempt fallback or retry

                try:
                    res = self.client.xautoclaim(
                        name=stream, groupname=self.group, consumername=self.consumer,
                        min_idle_time=min_idle_ms, start_id=start_id, count=count,
                    )
                except ResponseError as e2:
                    if _is_unsupported_xautoclaim(e2):
                        logger.warning("XAUTOCLAIM unsupported after NOGROUP recovery for %s, using fallback", stream)
                        res = self.client.execute_command(
                            "XAUTOCLAIM", stream, self.group, self.consumer,
                            min_idle_ms, start_id, "COUNT", count,
                        )
                    else:
                        raise
            elif _is_unsupported_xautoclaim(e):
                logger.warning("XAUTOCLAIM unsupported for %s, using fallback", stream)
                res = self.client.execute_command(
                    "XAUTOCLAIM", stream, self.group, self.consumer,
                    min_idle_ms, start_id, "COUNT", count,
                )
            else:
                raise
        except (ConnectionError, TimeoutError, OSError):
            # Transient error - let upper levels handle it
            raise
        except Exception as e:
            # For other exceptions, if it's "unsupported" try fallback, otherwise raise
            if _is_unsupported_xautoclaim(e):
                logger.warning("XAUTOCLAIM unsupported (generic catch) for %s, using fallback", stream)
                res = self.client.execute_command(
                    "XAUTOCLAIM", stream, self.group, self.consumer,
                    min_idle_ms, start_id, "COUNT", count,
                )
            else:
                raise

        # 2) Parse result with guards
        # Format: [next_id, [ (id, fields), ... ], [deleted_ids...]]
        if not res or not isinstance(res, (list, tuple)) or len(res) < 2:
            return start_id, []

        next_id_raw = res[0]
        raw_msgs = res[1] or []
        next_id = _decode_any(next_id_raw)

        msgs: list[StreamMsg] = []
        for item in raw_msgs:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            msg_id, fields = item[0], item[1]
            msg_id_str = _decode_any(msg_id)

            # Normalize fields (dict or list format)
            fields_map = _fields_to_dict(fields)
            fields_dict: dict[str, Any] = {}
            for k, v in fields_map.items():
                k_str = _decode_any(k)
                if isinstance(v, str):
                    v_str = v
                elif isinstance(v, (bytes, bytearray)):
                    v_str = v.decode("utf-8", errors="ignore")
                else:
                    v_str = str(v)
                fields_dict[k_str] = v_str
            msgs.append(StreamMsg(stream=stream, msg_id=msg_id_str, fields=fields_dict))

        # IMPORTANT: if scan finished (next_id="0-0") and we got no messages,
        # do NOT reset caller cursor to "0-0" (it would rescan from beginning).
        if next_id == "0-0" and not msgs:
            return start_id, []

        # Optimization: if reached end and no messages, keep start_id (avoid endless 0-0 resets)
        if next_id == "0-0" and not msgs:
            return start_id, []
        return next_id, msgs

    def pending_len(self, stream: str) -> int:
        """
        Pending length for (stream, group).
        Важно: XPENDING требует существования stream и consumer group.
        Если группы нет (NOGROUP) — возвращаем 0 (метрика "пока не применима").
        """
        if not stream:
            return 0
        try:
            # XPENDING summary call
            res = self.client.xpending(stream, self.group)
            return _parse_xpending_summary(res)
        except ResponseError as exc:
            s = str(exc)
            # группа ещё не создана или stream пустой — считаем 0
            if "NOGROUP" in s or "No such key" in s or "no such key" in s:
                return 0
            # любые другие ошибки — пробрасываем (чтобы видеть проблему)
            raise
        except (ConnectionError, TimeoutError):
            # Transient error, metrics should not fail the loop but we can't get the value
            return 0
        except Exception:
            # Other errors - safety return
            return 0

    def pending_by_consumer(self, stream: str) -> dict[str, int]:
        """
        Pending breakdown by consumer for (stream, group).
        Best-effort: never raises on transient / missing group.
        """
        if not stream:
            return {}
        try:
            res = self.client.xpending(stream, self.group)
            return _parse_xpending_consumers(res)
        except ResponseError as exc:
            s = str(exc)
            if "NOGROUP" in s or "No such key" in s or "no such key" in s:
                return {}
            return {}
        except (ConnectionError, TimeoutError):
            return {}
        except Exception:
            return {}

    def pending_details(self, stream: str) -> dict[str, Any]:
        """
        XPENDING summary + per-consumer pending.
        Возвращает:
          {
            "pending": int,
            "min": str|None,
            "max": str|None,
            "consumers": { "name": int, ... }
          }
        """
        if not stream:
            return {"pending": 0, "min": None, "max": None, "consumers": {}}
        try:
            res = self.client.xpending(stream, self.group)
        except ResponseError as exc:
            s = str(exc)
            if "NOGROUP" in s or "No such key" in s or "no such key" in s:
                return {"pending": 0, "min": None, "max": None, "consumers": {}}
            raise
        except Exception:
            return {"pending": 0, "min": None, "max": None, "consumers": {}}

        out: dict[str, Any] = {"pending": 0, "min": None, "max": None, "consumers": {}}

        # redis-py may return dict
        if isinstance(res, dict):
            try:
                out["pending"] = int(res.get("pending", 0) or 0)
            except Exception:
                out["pending"] = 0
            out["min"] = res.get("min")
            out["max"] = res.get("max")
            consumers = res.get("consumers") or []
            m: dict[str, int] = {}
            for c in consumers:
                # can be dict {"name": "...", "pending": n} or tuple(name, n)
                if isinstance(c, dict):
                    name = (c.get("name") or "")
                    try:
                        m[name] = int(c.get("pending", 0) or 0)
                    except Exception:
                        m[name] = 0
                elif isinstance(c, (list, tuple)) and len(c) >= 2:
                    name = str(c[0])
                    try:
                        m[name] = int(c[1] or 0)
                    except Exception:
                        m[name] = 0
            out["consumers"] = m
            return out

        # tuple/list format: (pending, min, max, consumers)
        if isinstance(res, (tuple, list)) and len(res) >= 4:
            try:
                out["pending"] = int(res[0] or 0)
            except Exception:
                out["pending"] = 0
            out["min"] = res[1]
            out["max"] = res[2]
            consumers = res[3] or []
            m: dict[str, int] = {}
            for c in consumers:
                if isinstance(c, (list, tuple)) and len(c) >= 2:
                    name = str(c[0])
                    try:
                        m[name] = int(c[1] or 0)
                    except Exception:
                        m[name] = 0
            out["consumers"] = m
            return out

        return out


class AsyncRedisStreamHelper:
    """Helper for asyncio-based Redis stream consumers."""

    def __init__(
        self,
        client: aioredis.Redis,
        group: str,
        consumer: str,
        *,
        recovery_start_id: str = "$",
    ) -> None:
        self.client = client
        self.group = group
        self.consumer = consumer
        self.recovery_start_id = recovery_start_id or "$"

    async def ensure_group(
        self,
        stream: str,
        *,
        recreate: bool = False,
        start_id: str = "$",
    ) -> None:
        """Ensures that a consumer group exists for async client usage."""
        if recreate:
            with contextlib.suppress(ResponseError):
                await self.client.xgroup_destroy(stream, self.group)

        max_retries = 30
        retry_count = 0
        while retry_count < max_retries:
            try:
                await self.client.xgroup_create(stream, self.group, id=start_id, mkstream=True)
                return
            except ResponseError as exc:
                error_msg = str(exc).lower()
                if "busygroup" in error_msg:
                    return
                if "loading" in error_msg:
                    retry_count += 1
                    wait_time = min(5 * retry_count, 30)
                    logger.warning("⚠️ Redis loading data (attempt %d/%d), waiting %d sec...",
                                 retry_count, max_retries, wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
            except (ConnectionError, TimeoutError, OSError) as exc:
                retry_count += 1
                wait_time = min(2 * retry_count, 10)
                logger.warning(
                    "⚠️ Redis connection/resolution error during ensure_group (attempt %d/%d): %s. Waiting %d sec...",
                    retry_count,
                    max_retries,
                    exc,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
                continue
            except Exception as exc:
                retry_count += 1
                wait_time = min(2 * retry_count, 10)
                logger.error(
                    "❌ Unexpected error in ensure_group (attempt %d/%d): %s. Waiting %d sec...",
                    retry_count,
                    max_retries,
                    exc,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
                continue
        raise RuntimeError(f"Redis unavailable or still loading after {max_retries} attempts")

    async def ensure_groups(self, streams: Iterable[str], *, recreate: bool = False) -> None:
        """Ensures consumer groups exist for all provided streams."""
        for stream in streams:
            await self.ensure_group(stream, recreate=recreate)

    async def read(
        self,
        streams: Mapping[str, str | None] | Iterable[str],
        *,
        count: int,
        block: int,
        recovery_start_id: str = "$",
    ):
        """Normalized wrapper around aioredis xreadgroup call.

        IMPORTANT: Do NOT wrap xreadgroup with asyncio.wait_for().
        XREADGROUP already has a server-side deadline via the `block` parameter
        (Redis returns an empty list after `block` ms). Using wait_for() is both
        redundant and harmful: when wait_for() cancels the coroutine mid-read,
        the redis.asyncio RESP2 parser is left in a corrupted state (socket is
        open but reader is at an unknown offset), causing all subsequent commands
        on that connection to fail with CancelledError → TimeoutError cascades.

        Connection-level timeouts should be set on the aioredis.Redis pool via
        socket_timeout= at pool creation time, not via asyncio cancellation.
        """
        normalized = _normalize_streams(streams)

        async def _do_xreadgroup() -> Any:
            return await self.client.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams=normalized,
                count=count,
                block=block,
            )

        try:
            return await _do_xreadgroup()
        except asyncio.CancelledError:
            # When the asyncio task is cancelled mid-read, the RESP2 parser is left in
            # a corrupted state (TCP socket open but reader at an unknown offset).
            # Disconnect the pool immediately so the corrupt connection is never reused.
            try:
                pool = getattr(self.client, "connection_pool", None)
                if pool is not None:
                    _safe_disconnect_pool(pool)
            except Exception:
                pass
            raise
        except TimeoutError as exc:
            # Catch TimeoutError first before Exception!
            # Python 3.12: asyncio.timeouts.__aexit__ wraps an *external* CancelledError
            # (e.g. Docker SIGTERM → asyncio task.cancel()) as a plain TimeoutError with
            # exc.__cause__ set to the original CancelledError. Re-raise it so the caller's
            # graceful-shutdown handler fires correctly.
            is_cancelled = False
            if isinstance(exc.__cause__, asyncio.CancelledError) or type(exc.__cause__) is asyncio.CancelledError:
                is_cancelled = True

            try:
                pool = getattr(self.client, "connection_pool", None)
                if pool is not None:
                    _safe_disconnect_pool(pool)
            except Exception:
                pass

            if is_cancelled:
                raise exc.__cause__
            raise TimeoutError(
                f"xreadgroup socket timeout (block={block}ms, group={self.group})"
            ) from exc
        except ResponseError as exc:
            # Handle NOGROUP before catching generic Exception
            if "NOGROUP" not in str(exc):
                raise
            for stream_name in normalized.keys():
                try:
                    await self.client.xgroup_create(
                        stream_name,
                        self.group,
                        id=self.recovery_start_id,
                        mkstream=True,
                    )
                except ResponseError as create_exc:
                    if "BUSYGROUP" not in str(create_exc):
                        raise
            # Retry once after group creation
            try:
                return await _do_xreadgroup()
            except asyncio.CancelledError:
                try:
                    pool = getattr(self.client, "connection_pool", None)
                    if pool is not None:
                        _safe_disconnect_pool(pool)
                except Exception:
                    pass
                raise
            except TimeoutError as exc:
                is_cancelled = False
                if isinstance(exc.__cause__, asyncio.CancelledError) or type(exc.__cause__) is asyncio.CancelledError:
                    is_cancelled = True

                try:
                    pool = getattr(self.client, "connection_pool", None)
                    if pool is not None:
                        _safe_disconnect_pool(pool)
                except Exception:
                    pass

                if is_cancelled:
                    raise exc.__cause__
                raise TimeoutError(
                    f"xreadgroup (post-NOGROUP) socket timeout (group={self.group})"
                ) from exc
        except (ConnectionError, OSError):
            # For connection-level errors, let the redis-py pool handle reconnection
            # of the single socket. Do NOT disconnect the whole pool as it would
            # kill all other active consumers sharing this pool (disconnect storm).
            raise
        except Exception as exc:
            exc_str = str(exc).lower()
            if type(exc).__name__ == "BusyLoadingError" or "loading" in exc_str:
                logger.warning("⏳ Redis is loading dataset in memory, waiting 5s before xreadgroup (async)...")
                await asyncio.sleep(5.0)
                return await _do_xreadgroup()

            # For other truly unexpected exceptions (like redis-py bugs throwing 'NoneType' is not callable)
            # disconnect the pool to flush corrupted state.
            try:
                pool = getattr(self.client, "connection_pool", None)
                if pool is not None:
                    _safe_disconnect_pool(pool)
            except Exception:
                pass

            if isinstance(exc, TypeError) and "nonetype" in exc_str and "callable" in exc_str:
                # Workaround for redis-py async bug where a broken parser/callback throws NoneType not callable.
                # Route as a transient ConnectionError so consumer gracefully backs off.
                raise ConnectionError(f"redis-py internal TypeError wrapped as ConnectionError: {exc}") from exc

            raise


    async def pending_len(self, stream: str) -> int:
        """
        Best-effort async XPENDING summary -> pending count.
        Fail-open: returns 0 on any error (NOGROUP, timeout, connection).
        """
        if not stream:
            return 0
        try:
            res = await self.client.xpending(stream, self.group)
            return _parse_xpending_summary(res)
        except ResponseError as exc:
            s = str(exc)
            if "NOGROUP" in s or "No such key" in s or "no such key" in s:
                return 0
            return 0
        except (ConnectionError, TimeoutError):
            return 0
        except Exception:
            return 0

    async def claim_pending(
        self,
        stream: str,
        *,
        min_idle_ms: int = 5000,
        count: int = 100,
        start_id: str = "0-0",
    ) -> tuple[str, list[StreamMsg]]:
        """
        Claim idle pending messages via XAUTOCLAIM (Redis ≥ 7.0).
        Falls back to XPENDING_RANGE + XCLAIM on older Redis.
        Returns (next_id, [StreamMsg, ...]).
        Fail-open: returns (start_id, []) on any error.
        """
        if not stream:
            return start_id, []
        try:
            # XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
            res = await self.client.xautoclaim(
                stream,
                self.group,
                self.consumer,
                min_idle_time=min_idle_ms,
                start_id=start_id,
                count=count,
            )
            # res: [next_id, [(msg_id, fields), ...], ...]
            if not res:
                return start_id, []
            next_id = res[0] if isinstance(res[0], str) else str(res[0])
            raw_entries = res[1] if len(res) > 1 else []
            msgs: list[StreamMsg] = []
            for msg_id, fields in raw_entries or []:
                msg_id_str = msg_id if isinstance(msg_id, str) else str(msg_id)
                fields_dict: dict[str, Any] = {}
                for k, v in (fields or {}).items():
                    k_str = k if isinstance(k, str) else k.decode("utf-8", errors="ignore")
                    v_str = v if isinstance(v, str) else (
                        v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
                    )
                    fields_dict[k_str] = v_str
                msgs.append(StreamMsg(stream=stream, msg_id=msg_id_str, fields=fields_dict))
            return next_id, msgs
        except ResponseError as exc:
            s = str(exc).lower()
            # XAUTOCLAIM not available (Redis < 7.0) — graceful degradation
            if "unknown command" in s or "wrong number of arguments" in s:
                return start_id, []
            # NOGROUP / no such key — PEL doesn't exist yet
            if "nogroup" in s or "no such key" in s:
                return start_id, []
            return start_id, []
        except (ConnectionError, TimeoutError):
            return start_id, []
        except Exception:
            return start_id, []

    async def ack(self, stream: str, message_id: str) -> None:
        """Async ACK wrapper."""
        await self.client.xack(stream, self.group, message_id)

    async def ack_many(self, stream: str, message_ids: list[str]) -> None:
        """Async ACK wrapper for multiple messages."""
        if not message_ids:
            return
        await self.client.xack(stream, self.group, *message_ids)

