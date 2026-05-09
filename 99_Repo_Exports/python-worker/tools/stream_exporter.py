"""
Stream Exporter - NDJSON.gz архивирование Redis Streams на диск

Fail-safe архив для offline replay и долгосрочного хранения.
Работает независимо от PostgreSQL archiver.

Key features:
- XRANGE для инкрементального чтения (последний stream_id хранится в Redis)
- NDJSON.gz формат (gzip compression + newline-delimited JSON)
- Ротация по дням (файлы по дате первого события в chunk)
- Retention policy (удаление старых файлов)
- Idempotent: можно перезапускать без дублей

Структура файлов:
/var/log/trade/exports/
  stream_trade_entry_audit/
    stream_trade_entry_audit_20260127.ndjson.gz
    stream_trade_entry_audit_20260128.ndjson.gz
  events_trades/
    events_trades_20260127.ndjson.gz
    events_trades_20260128.ndjson.gz
"""
import asyncio
import gzip
import json
import os
import time
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
import contextlib
from core.redis_keys import RedisStreams as RS


def env(name: str, default: str) -> str:
    """Env helper with empty string handling"""
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    """Env int helper"""
    v = os.getenv(name)
    return int(v) if v else default


def ensure_dir(p: str) -> None:
    """Create directory if not exists"""
    os.makedirs(p, exist_ok=True)


def stream_ms(stream_id: str) -> int:
    """Extract timestamp in milliseconds from stream ID: '<ms>-<seq>'"""
    return int(stream_id.split("-", 1)[0])


def cleanup_old(dir_path: str, keep_days: int) -> None:
    """
    Delete .ndjson.gz files older than keep_days.
    Uses file mtime for simplicity.
    """
    if keep_days <= 0:
        return
    now = time.time()
    cutoff = now - keep_days * 86400
    for root, _, files in os.walk(dir_path):
        for fn in files:
            if not fn.endswith(".ndjson.gz"):
                continue
            fp = os.path.join(root, fn)
            try:
                st = os.stat(fp)
                if st.st_mtime < cutoff:
                    os.remove(fp)
            except Exception:
                pass


async def wait_for_redis_ready(
    r: aioredis.Redis,
    max_retries: int = 60,
    base_delay: float = 2.0,
    max_delay: float = 30.0
) -> bool:
    """
    Wait for Redis to be ready (not loading dataset).
    
    Args:
        r: Redis client
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff
        max_delay: Maximum delay in seconds
    
    Returns:
        True if Redis is ready, False if max retries exceeded
    """
    for attempt in range(max_retries):
        try:
            # Try a simple operation to check if Redis is ready
            await r.ping()

            # Try to get a key to ensure Redis is fully loaded
            # Use a non-existent key to avoid side effects
            try:
                await r.get("__redis_ready_check__")
                return True
            except ResponseError as e:
                error_msg = str(e).lower()
                if "loading" in error_msg or "dataset" in error_msg:
                    # Redis is still loading
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    if attempt < max_retries - 1:
                        print(f"⏳ Redis is loading dataset, waiting {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"❌ Redis still loading after {max_retries} attempts")
                        return False
                else:
                    # Different error, Redis is ready but operation failed
                    return True
            except Exception:
                # Any other error means Redis is ready (just the operation failed)
                return True

        except (RedisConnectionError, OSError) as e:
            # Connection error - wait and retry
            delay = min(base_delay * (2 ** attempt), max_delay)
            if attempt < max_retries - 1:
                print(f"⏳ Redis connection error, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(delay)
                continue
            else:
                print(f"❌ Failed to connect to Redis after {max_retries} attempts")
                return False
        except Exception as e:
            # Unexpected error - assume Redis is ready
            print(f"⚠️  Unexpected error checking Redis readiness: {e}")
            return True

    return False


async def create_redis_connection(
    redis_url: str,
    max_retries: int = 30,
    base_delay: float = 2.0
) -> aioredis.Redis | None:
    """
    Create Redis connection with retry logic and readiness check.
    
    Args:
        redis_url: Redis connection URL
        max_retries: Maximum number of connection attempts
        base_delay: Base delay in seconds for exponential backoff
    
    Returns:
        Redis client if successful, None otherwise
    """
    for attempt in range(max_retries):
        try:
            r = aioredis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=10,
                health_check_interval=30
            )

            # Test connection first
            try:
                await r.ping()
            except ResponseError as e:
                error_msg = str(e).lower()
                if "loading" in error_msg or "dataset" in error_msg:
                    # Redis is loading, wait and retry
                    delay = min(base_delay * (2 ** attempt), 30.0)
                    if attempt < max_retries - 1:
                        print(f"⏳ Redis is loading dataset, waiting {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                        with contextlib.suppress(Exception):
                            await r.aclose()
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"❌ Redis still loading after {max_retries} attempts")
                        with contextlib.suppress(Exception):
                            await r.aclose()
                        return None
                else:
                    # Other response error, re-raise to be caught by outer handler
                    with contextlib.suppress(Exception):
                        await r.aclose()
                    raise

            # Wait for Redis to be fully ready
            if await wait_for_redis_ready(r, max_retries=60, base_delay=base_delay):
                print("✅ Redis connection established and ready")
                return r
            else:
                await r.aclose()
                return None

        except (RedisConnectionError, OSError) as e:
            # Connection error - check if it's BusyLoading
            error_msg = str(e).lower()
            if "loading" in error_msg or "dataset" in error_msg:
                # Redis is loading, wait and retry with appropriate message
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis is loading dataset, waiting {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Redis still loading after {max_retries} attempts")
                    return None
            else:
                # Other connection error - wait and retry
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis connection error, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Failed to connect to Redis after {max_retries} attempts: {e}")
                    return None
        except ResponseError as e:
            # Redis response error (including BusyLoading)
            error_msg = str(e).lower()
            if "loading" in error_msg or "dataset" in error_msg:
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis is loading dataset, waiting {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Redis still loading after {max_retries} attempts")
                    return None
            else:
                # Other response error
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis response error, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Redis response error after {max_retries} attempts: {e}")
                    return None
        except Exception as e:
            # Unexpected error
            error_msg = str(e).lower()
            if "loading" in error_msg or "dataset" in error_msg:
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis is loading dataset, waiting {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Redis still loading after {max_retries} attempts")
                    return None
            else:
                delay = min(base_delay * (2 ** attempt), 30.0)
                if attempt < max_retries - 1:
                    print(f"⏳ Redis connection attempt {attempt + 1}/{max_retries} failed, retrying in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"❌ Failed to create Redis connection after {max_retries} attempts: {e}")
                    return None

    return None


def is_redis_loading_error(error: Exception) -> bool:
    """Check if error indicates Redis is loading dataset."""
    error_msg = str(error).lower()
    return "loading" in error_msg and "dataset" in error_msg


async def export_stream(
    r: aioredis.Redis,
    stream: str,
    out_dir: str,
    last_id_key: str,
    chunk: int = 2000
) -> int:
    """
    Export stream to NDJSON.gz incrementally.
    
    Args:
        r: Redis client
        stream: Stream name (e.g. RS.EVENTS_TRADES)
        out_dir: Output directory root
        last_id_key: Redis key to store last exported stream_id
        chunk: Number of messages per XRANGE call
    
    Returns:
        Number of messages exported
    
    Raises:
        Exception: If Redis is loading or other error occurs
    """
    # Read last exported stream_id
    try:
        last_id = await r.get(last_id_key) or "0-0"
    except ResponseError as e:
        if is_redis_loading_error(e):
            raise Exception("Redis is loading the dataset in memory") from e
        raise
    except Exception as e:
        if is_redis_loading_error(e):
            raise Exception("Redis is loading the dataset in memory") from e
        raise

    exported = 0

    while True:
        # XRANGE: read chunk after last_id
        try:
            items: list[tuple[str, dict[str, Any]]] = await r.xrange(
                stream, min=last_id, max="+", count=chunk
            )
        except ResponseError as e:
            if is_redis_loading_error(e):
                raise Exception("Redis is loading the dataset in memory") from e
            raise
        except Exception as e:
            if is_redis_loading_error(e):
                raise Exception("Redis is loading the dataset in memory") from e
            raise

        if not items:
            break

        fetched_count = len(items)

        # Skip first item if it's the last_id (already exported)
        if items and items[0][0] == last_id:
            items = items[1:]
        if not items:
            # We must break here if the only item returned was the one we already processed
            # Wait! if fetched_count was exactly chunk, there might be more, but that's impossible
            # because if fetched_count > 1, after stripping 1, we'd have elements left.
            # If we don't have elements, fetched_count is <= 1, which is < chunk (unless chunk=1)
            if fetched_count < chunk:
                break
            # If for some weird reason chunk was 1, we continue with last_id modified?
            # last_id hasn't changed. We must bump last_id or use '(' prefix in min.
            # But the best is just to break, since if we got exactly 1 item and it's the one we
            # requested min=last_id for, we are at the end.
            break

        # Determine day from first item's timestamp (for file rotation)
        day = datetime.fromtimestamp(
            stream_ms(items[0][0]) / 1000.0, tz=UTC
        ).strftime("%Y%m%d")

        # Build path: out_dir/stream_name/stream_name_YYYYMMDD.ndjson.gz
        stream_safe = stream.replace(":", "_")
        path = os.path.join(out_dir, stream_safe)
        ensure_dir(path)
        fname = os.path.join(path, f"{stream_safe}_{day}.ndjson.gz")

        # Append to NDJSON.gz (mode='at' = append text)
        with gzip.open(fname, "at", encoding="utf-8") as f:
            for sid, fields in items:
                record = {
                    "stream": stream,
                    "stream_id": sid,
                    "fields": fields,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                last_id = sid
                exported += 1

        # Update last_id in Redis (checkpoint)
        try:
            await r.set(last_id_key, last_id)
        except ResponseError as e:
            if is_redis_loading_error(e):
                raise Exception("Redis is loading the dataset in memory") from e
            raise
        except Exception as e:
            if is_redis_loading_error(e):
                raise Exception("Redis is loading the dataset in memory") from e
            raise

        # If Redis returned less than chunk, we are at the end
        if fetched_count < chunk:
            break

    return exported


async def main() -> None:
    """Main entry point"""
    # Check if enabled
    if env_int("STREAM_EXPORT_ENABLED", 1) != 1:
        return

    redis_url = env("REDIS_URL", "redis://redis:6379/0")

    # Config
    out_dir = env("STREAM_EXPORT_DIR", "/var/log/trade/exports")
    interval = env_int("STREAM_EXPORT_INTERVAL_SEC", 300)
    keep_days = env_int("STREAM_EXPORT_KEEP_DAYS", 90)

    entry_stream = env("TRADE_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit")
    events_stream = env("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)

    print(f"✅ Stream Exporter starting | out_dir={out_dir} interval={interval}s keep_days={keep_days}")

    # Create Redis connection with retry and readiness check
    r = await create_redis_connection(redis_url, max_retries=30, base_delay=2.0)
    if r is None:
        print("❌ Failed to establish Redis connection. Exiting.")
        return

    print(f"✅ Stream Exporter started | out_dir={out_dir} interval={interval}s keep_days={keep_days}")

    consecutive_errors = 0
    max_consecutive_errors = 10

    while True:
        cycle_errors = 0

        # Export entry audit stream
        try:
            n1 = await export_stream(
                r, entry_stream, out_dir, f"export:last_id:{entry_stream}"
            )
            if n1 > 0:
                print(f"📦 Exported {n1} messages from {entry_stream}")
            consecutive_errors = 0  # Reset on success
        except Exception as e:
            error_msg = str(e)
            if is_redis_loading_error(e) or "loading" in error_msg.lower():
                print(f"⏳ Redis is loading dataset, skipping export of {entry_stream} (will retry)")
                cycle_errors += 1
            else:
                print(f"❌ Error exporting {entry_stream}: {e}")
                cycle_errors += 1
            consecutive_errors += 1

        # Export events stream
        try:
            n2 = await export_stream(
                r, events_stream, out_dir, f"export:last_id:{events_stream}"
            )
            if n2 > 0:
                print(f"📦 Exported {n2} messages from {events_stream}")
            consecutive_errors = 0  # Reset on success
        except Exception as e:
            error_msg = str(e)
            if is_redis_loading_error(e) or "loading" in error_msg.lower():
                print(f"⏳ Redis is loading dataset, skipping export of {events_stream} (will retry)")
                cycle_errors += 1
            else:
                print(f"❌ Error exporting {events_stream}: {e}")
                cycle_errors += 1
            consecutive_errors += 1

        # Cleanup old files
        try:
            cleanup_old(out_dir, keep_days)
        except Exception as e:
            print(f"⚠️  Error cleaning up old files: {e}")

        # If we had errors, check if we need to reconnect
        if cycle_errors > 0:
            # Check Redis connection health
            try:
                await r.ping()
            except Exception:
                print("⚠️  Redis connection lost, attempting to reconnect...")
                with contextlib.suppress(Exception):
                    await r.aclose()
                r = await create_redis_connection(redis_url, max_retries=10, base_delay=2.0)
                if r is None:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"❌ Too many consecutive errors ({consecutive_errors}), exiting")
                        return
                else:
                    consecutive_errors = 0

        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
