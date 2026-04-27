from utils.time_utils import get_ny_time_millis
"""
Zombie Position Janitor
========================

Periodic cleanup timer for stale positions in orders:open.

Runs every JANITOR_INTERVAL_SEC (default: 60s) and removes positions
older than JANITOR_MAX_AGE_SEC (default: 1200s = 20min).

ENV:
  REDIS_URL              — Redis connection string (default: redis://redis-worker-1:6379/0)
  JANITOR_INTERVAL_SEC   — Scan interval in seconds (default: 60)
  JANITOR_MAX_AGE_SEC    — Max position age before forced cleanup (default: 1200 = 20min)
  JANITOR_DRY_RUN        — "1" to log only, no actual removal (default: "0")
  JANITOR_BATCH_SIZE     — Number of positions to check per scan batch (default: 500)
  PROMETHEUS_PORT         — Metrics port (default: 9119)
  LOG_LEVEL              — Logging level (default: INFO)

Metrics:
  zombie_janitor_scanned_total     — Total positions scanned
  zombie_janitor_removed_total     — Total zombie positions removed
  zombie_janitor_errors_total      — Total errors during cleanup
  zombie_janitor_open_positions    — Current orders:open gauge
  zombie_janitor_run_duration_sec  — Histogram of scan durations
"""

import json
import logging
import os
import signal
import sys
import time
from typing import Optional

import redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("zombie_janitor")

# ── Prometheus metrics ───────────────────────────────────────────────
SCANNED = Counter("zombie_janitor_scanned_total", "Positions scanned")
REMOVED = Counter("zombie_janitor_removed_total", "Zombie positions removed", ["symbol", "reason"])
ERRORS = Counter("zombie_janitor_errors_total", "Cleanup errors")
OPEN_GAUGE = Gauge("zombie_janitor_open_positions", "Current orders:open count")
RUN_DURATION = Histogram(
    "zombie_janitor_run_duration_sec",
    "Scan duration in seconds",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)

# ── Config ───────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
INTERVAL_SEC = int(os.getenv("JANITOR_INTERVAL_SEC", "60"))
MAX_AGE_SEC = int(os.getenv("JANITOR_MAX_AGE_SEC", "1200"))  # 20 min
DRY_RUN = os.getenv("JANITOR_DRY_RUN", "0") == "1"
BATCH_SIZE = int(os.getenv("JANITOR_BATCH_SIZE", "500"))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9119"))

# ── Key constants ────────────────────────────────────────────────────
ORDERS_OPEN = "orders:open"


def _connect_redis() -> redis.Redis:
    """Create a Redis connection with retry."""
    for attempt in range(5):
        try:
            r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10)
            r.ping()
            return r
        except Exception as e:
            logger.warning("Redis connect attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    logger.error("❌ Could not connect to Redis after 5 attempts")
    sys.exit(1)


def _get_position_age_sec(r: redis.Redis, pos_id: str) -> Optional[float]:
    """Get position age in seconds from order:{pos_id} hash."""
    try:
        # Try multiple timestamp fields in priority order
        fields = r.hmget(f"order:{pos_id}", "entry_ts_ms", "open_ts_ms", "created_at_ms", "ts_ms")
        for val in fields:
            if val is not None:
                try:
                    ts_ms = float(val)
                    if ts_ms > 1_000_000_000_000:  # milliseconds
                        return (get_ny_time_millis() - ts_ms) / 1000.0
                    elif ts_ms > 1_000_000_000:  # seconds
                        return time.time() - ts_ms
                except (ValueError, TypeError):
                    continue
        return None
    except Exception:
        return None


def _get_position_symbol(r: redis.Redis, pos_id: str) -> str:
    """Get symbol from position hash."""
    try:
        sym = r.hget(f"order:{pos_id}", "symbol")
        return str(sym) if sym else "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def run_cleanup(r: redis.Redis) -> int:
    """Run one cleanup cycle. Returns number of removed positions."""
    t0 = time.perf_counter()
    now_sec = time.time()
    removed = 0
    scanned = 0
    missing_hash = 0

    try:
        # Get all open position IDs
        members = r.smembers(ORDERS_OPEN)
        total = len(members)
        OPEN_GAUGE.set(total)

        if total == 0:
            logger.debug("orders:open is empty, nothing to scan")
            return 0

        logger.debug("🔍 Scanning %d open positions (max_age=%ds, dry_run=%s)", total, MAX_AGE_SEC, DRY_RUN)

        for pos_id in members:
            scanned += 1
            SCANNED.inc()

            # Check if hash exists
            exists = r.exists(f"order:{pos_id}")
            if not exists:
                # Orphan reference — hash is gone but ID still in set
                reason = "missing_hash"
                if not DRY_RUN:
                    r.srem(ORDERS_OPEN, pos_id)
                    REMOVED.labels(symbol="UNKNOWN", reason=reason).inc()
                    removed += 1
                    logger.debug("🧹 Removed orphan ref %s (hash missing)", pos_id)
                else:
                    logger.debug("🧹 [DRY-RUN] Would remove orphan ref %s (hash missing)", pos_id)
                    removed += 1
                missing_hash += 1
                continue

            age_sec = _get_position_age_sec(r, pos_id)
            if age_sec is None:
                continue

            if age_sec > MAX_AGE_SEC:
                symbol = _get_position_symbol(r, pos_id)
                reason = "age_exceeded"

                if not DRY_RUN:
                    # Save close reason to hash before removal
                    try:
                        r.hset(f"order:{pos_id}", mapping={
                            "closed": "1",
                            "close_reason": "ZOMBIE_JANITOR",
                            "close_ts_ms": str(int(now_sec * 1000)),
                        })
                    except Exception:
                        pass

                    r.srem(ORDERS_OPEN, pos_id)
                    REMOVED.labels(symbol=symbol, reason=reason).inc()
                    removed += 1
                    logger.debug(
                        "🧹 Removed zombie %s sym=%s age=%.0fs (>%ds)",
                        pos_id, symbol, age_sec, MAX_AGE_SEC,
                    )
                else:
                    logger.debug(
                        "🧹 [DRY-RUN] Would remove %s sym=%s age=%.0fs",
                        pos_id, symbol, age_sec,
                    )
                    removed += 1

    except Exception as e:
        ERRORS.inc()
        logger.error("❌ Cleanup error: %s", e)
    finally:
        dur = time.perf_counter() - t0
        RUN_DURATION.observe(dur)
        OPEN_GAUGE.set(r.scard(ORDERS_OPEN) or 0)
        logger.debug(
            "✅ Scan complete: scanned=%d removed=%d missing_hash=%d duration=%.2fs remaining=%d",
            scanned, removed, missing_hash, dur, r.scard(ORDERS_OPEN) or 0,
        )
    return removed


def main():
    logger.info(
        "🚀 Zombie Position Janitor starting (interval=%ds, max_age=%ds, dry_run=%s, port=%d)",
        INTERVAL_SEC, MAX_AGE_SEC, DRY_RUN, PROMETHEUS_PORT,
    )

    # Prometheus
    start_http_server(PROMETHEUS_PORT)

    # Redis
    r = _connect_redis()
    logger.info("✅ Connected to Redis")

    # Graceful shutdown
    running = True

    def _handle_signal(*_):
        nonlocal running
        running = False
        logger.info("Shutting down...")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Main loop
    while running:
        try:
            run_cleanup(r)
        except redis.ConnectionError:
            logger.warning("Redis connection lost, reconnecting...")
            try:
                r = _connect_redis()
            except SystemExit:
                break
        except Exception as e:
            ERRORS.inc()
            logger.error("Unexpected error: %s", e)

        # Sleep in small increments for responsive shutdown
        for _ in range(INTERVAL_SEC * 10):
            if not running:
                break
            time.sleep(0.1)

    logger.info("Goodbye.")


if __name__ == "__main__":
    main()
