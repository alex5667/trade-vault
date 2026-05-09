from __future__ import annotations

"""
pel_bootstrap_cleanup.py — Automatic zombie consumer cleanup on worker startup.

Problem:
  Each container restart creates a new PID-based consumer_id.
  Old (zombie) consumers accumulate a PEL (Pending Entry List) of unACKed ticks.
  When the PEL sweeper XAUTOCLAIM reclaims these, the worker receives ticks
  with event_ts_ms from minutes/hours ago → lag_ms spikes → p99 explodes.

Solution:
  On startup, BEFORE processing any ticks, enumerate all consumers in each
  stream's consumer group and remove any consumer whose idle time exceeds a
  threshold (default: 60s). ACK all their pending entries to prevent
  lag poisoning.

Usage:
  Called from CryptoOrderflowService.run_forever() after Redis is connected
  but before load_dynamic_symbols().

ENV:
  PEL_CLEANUP_ON_STARTUP       = "1" (default) — enable startup cleanup
  PEL_CLEANUP_IDLE_THRESHOLD_MS = "60000" (default) — idle threshold for zombie detection
  PEL_CLEANUP_PERIODIC_ENABLE  = "1" (default) — enable periodic background cleanup
  PEL_CLEANUP_PERIODIC_SEC     = "300" (default) — periodic cleanup interval
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger("pel_cleanup")

# Prometheus counter (optional, fail-open)
try:
    from services.orderflow.metrics import _get_or_create_prom_counter, _get_or_create_prom_gauge

    pel_zombie_cleaned_total = _get_or_create_prom_counter(
        "pel_zombie_cleaned_total",
        "Total zombie consumers removed during PEL cleanup",
        ["symbol", "kind", "phase"],
    )
    pel_pending_acked_total = _get_or_create_prom_counter(
        "pel_pending_acked_total",
        "Total pending entries ACKed during PEL cleanup",
        ["symbol", "kind", "phase"],
    )
    pel_consumer_count_gauge = _get_or_create_prom_gauge(
        "pel_consumer_count",
        "Number of consumers in stream consumer group after cleanup",
        ["symbol", "kind"],
    )
except Exception:
    pel_zombie_cleaned_total = None
    pel_pending_acked_total = None
    pel_consumer_count_gauge = None


async def cleanup_zombie_consumers(
    redis_client: Any,
    symbols: list[str],
    *,
    current_consumer_id: str,
    idle_threshold_ms: int = 60_000,
    phase: str = "startup",
    stream_prefix: str = "stream:tick_",
    group_prefix: str = "crypto-of:",
    book_stream_prefix: str = "stream:book_",
    book_group_prefix: str = "crypto-of-book:",
) -> dict[str, int]:
    """Remove zombie consumers and ACK their pending entries.

    Returns dict with summary stats: {zombies_removed, pending_acked, symbols_processed}.
    """
    if not redis_client:
        logger.warning("PEL cleanup: Redis client is None, skipping")
        return {"zombies_removed": 0, "pending_acked": 0, "symbols_processed": 0}

    total_zombies = 0
    total_acked = 0

    stream_configs: list[tuple[str, str, str]] = []
    for sym in symbols:
        stream_configs.append((f"{stream_prefix}{sym}", f"{group_prefix}{sym}", "ticks"))
        stream_configs.append((f"{book_stream_prefix}{sym}", f"{book_group_prefix}{sym}", "books"))

    for stream, group, kind in stream_configs:
        try:
            # Check if stream exists
            exists = await redis_client.exists(stream)
            if not exists:
                continue

            # Get consumer info
            try:
                consumers_info = await redis_client.xinfo_consumers(stream, group)
            except Exception:
                # Group might not exist yet — that's fine
                continue

            if not consumers_info:
                continue

            sym = stream.split("_", 1)[1] if "_" in stream else stream

            zombies_found = 0
            pending_cleaned = 0

            for cinfo in consumers_info:
                consumer_name = _get_consumer_field(cinfo, "name")
                consumer_idle = _get_consumer_field(cinfo, "idle", 0)
                consumer_pending = _get_consumer_field(cinfo, "pending", 0)

                if not consumer_name:
                    continue

                # Skip our own consumer
                if str(consumer_name) == str(current_consumer_id):
                    continue

                # Skip consumers that are still active
                if int(consumer_idle) < idle_threshold_ms:
                    continue

                # This is a zombie consumer — ACK its pending entries and remove it
                try:
                    if int(consumer_pending) > 0:
                        acked = await _ack_consumer_pending(
                            redis_client, stream, group, str(consumer_name),
                            current_consumer=current_consumer_id,
                            batch_size=200,
                        )
                        pending_cleaned += acked
                except Exception as exc:
                    logger.debug("Failed to ACK pending for %s/%s: %s", stream, consumer_name, exc)

                # Delete the zombie consumer
                try:
                    await redis_client.xgroup_delconsumer(stream, group, str(consumer_name))
                    zombies_found += 1
                except Exception as exc:
                    logger.debug("Failed to delete consumer %s/%s: %s", stream, consumer_name, exc)

            total_zombies += zombies_found
            total_acked += pending_cleaned

            # Update metrics
            if zombies_found > 0 or pending_cleaned > 0:
                logger.info(
                    "🧹 PEL cleanup [%s] %s/%s: removed %d zombies, ACKed %d pending",
                    phase, stream, group, zombies_found, pending_cleaned,
                )

            try:
                if pel_zombie_cleaned_total and zombies_found > 0:
                    pel_zombie_cleaned_total.labels(symbol=sym, kind=kind, phase=phase).inc(zombies_found)
                if pel_pending_acked_total and pending_cleaned > 0:
                    pel_pending_acked_total.labels(symbol=sym, kind=kind, phase=phase).inc(pending_cleaned)
                if pel_consumer_count_gauge:
                    # Count remaining consumers
                    remaining = len(consumers_info) - zombies_found
                    pel_consumer_count_gauge.labels(symbol=sym, kind=kind).set(max(0, remaining))
            except Exception:
                pass

        except Exception as exc:
            logger.warning("PEL cleanup error for %s: %s", stream, exc)

    if total_zombies > 0 or total_acked > 0:
        logger.info(
            "✅ PEL cleanup [%s] complete: %d zombies removed, %d pending ACKed across %d streams",
            phase, total_zombies, total_acked, len(stream_configs),
        )
    else:
        logger.info("✅ PEL cleanup [%s]: no zombies found across %d streams", phase, len(stream_configs))

    return {
        "zombies_removed": total_zombies,
        "pending_acked": total_acked,
        "symbols_processed": len(symbols),
    }


async def _ack_consumer_pending(
    redis_client: Any,
    stream: str,
    group: str,
    consumer: str,
    *,
    current_consumer: str = "",
    batch_size: int = 200,
) -> int:
    """XCLAIM and ACK all pending entries for a specific consumer. Returns count ACKed."""
    total_acked = 0
    start_id = "-"

    for _ in range(50):  # safety limit: max 50 iterations × 200 = 10000 entries
        try:
            pending = await redis_client.xpending_range(
                stream, group, start_id, "+", batch_size, consumername=consumer
            )
        except Exception:
            break

        if not pending:
            break

        ids = []
        for entry in pending:
            msg_id = _get_pending_field(entry, "message_id")
            if msg_id:
                ids.append(str(msg_id))

        if not ids:
            break

        try:
            # Active PEL Management: XCLAIM the messages to current consumer before ACKing
            if current_consumer:
                await redis_client.xclaim(stream, group, current_consumer, min_idle_time=0, message_ids=ids)

            await redis_client.xack(stream, group, *ids)
            total_acked += len(ids)
        except Exception:
            break

        if len(pending) < batch_size:
            break

        # Advance cursor past last processed ID
        start_id = str(ids[-1])

    return total_acked


def _get_consumer_field(info: Any, field: str, default: Any = "") -> Any:
    """Extract field from consumer info (handles both dict and list formats)."""
    if isinstance(info, dict):
        return info.get(field, default)
    # Some redis-py versions return list of alternating key-value pairs
    if isinstance(info, (list, tuple)):
        for i, item in enumerate(info):
            if isinstance(item, (str, bytes)):
                key = item.decode() if isinstance(item, bytes) else item
                if key == field and i + 1 < len(info):
                    val = info[i + 1]
                    return val.decode() if isinstance(val, bytes) else val
    return default


def _get_pending_field(info: Any, field: str, default: Any = "") -> Any:
    """Extract field from pending entry info."""
    if isinstance(info, dict):
        return info.get(field, default)
    # redis-py xpending_range returns list of dicts with 'message_id' key
    if hasattr(info, "message_id"):
        return getattr(info, field, default)
    return default


async def periodic_pel_cleanup_loop(
    redis_client_fn,
    symbols_fn,
    current_consumer_id: str,
    *,
    is_shutdown_fn,
    interval_sec: float = 300.0,
    idle_threshold_ms: int = 60_000,
) -> None:
    """Background loop that periodically cleans zombie consumers.

    Args:
        redis_client_fn: Callable returning the Redis client.
        symbols_fn: Callable returning current list of symbols.
        current_consumer_id: Current worker's consumer ID.
        is_shutdown_fn: Callable returning True when shutdown is requested.
        interval_sec: Interval between cleanup cycles.
        idle_threshold_ms: Idle threshold for zombie detection.
    """
    logger.info(
        "🧹 PEL periodic cleanup loop started (interval=%ds, idle_threshold=%dms)",
        int(interval_sec), idle_threshold_ms,
    )

    # Stagger first run by 30s to avoid contention with other startup tasks
    await asyncio.sleep(30.0)

    while not is_shutdown_fn():
        try:
            redis_client = redis_client_fn()
            symbols = list(symbols_fn())

            if redis_client and symbols:
                await cleanup_zombie_consumers(
                    redis_client,
                    symbols,
                    current_consumer_id=current_consumer_id,
                    idle_threshold_ms=idle_threshold_ms,
                    phase="periodic",
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("PEL periodic cleanup error: %s", exc)

        await asyncio.sleep(interval_sec)
