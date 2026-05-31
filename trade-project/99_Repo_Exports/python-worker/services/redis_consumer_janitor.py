"""Redis Consumer Group Janitor.

Периодически сканирует все stream-группы на двух Redis-нодах и:

1. Удаляет zombie-consumers — idle > ZOMBIE_IDLE_MS И pending == 0.
2. ACK'ает stale PEL — сообщения которые уже удалены из stream (XRANGE empty)
   И idle > STALE_PEL_IDLE_MS (минимум ZOMBIE_IDLE_MS).
3. Пишет Prometheus-метрики по каждому stream/group.

ENV (defaults)
--------------
JANITOR_REDIS_URLS          Запятая-разделённый список URL (по умолчанию REDIS_URL + REDIS_TICKS_URL)
JANITOR_INTERVAL_SEC        Интервал между полными sweep (default 3600 = 1ч)
JANITOR_ZOMBIE_IDLE_MS      Idle threshold для zombie-consumer (default 7200000 = 2ч)
JANITOR_STALE_PEL_IDLE_MS   Idle threshold для stale PEL ACK (default 3600000 = 1ч)
JANITOR_DRY_RUN             1 = только логировать, не удалять (default 0)
JANITOR_STREAM_PATTERNS     Паттерны через запятую (default: events:*,signals:*,stream:*,trades:*,ml_replay_*,notify:*)
JANITOR_PEL_BATCH           Сколько pending читать за раз для проверки наличия в stream (default 200)
JANITOR_MIN_CONSUMERS_TO_CLEAN  Минимум consumers в группе для запуска чистки (default 2)
JANITOR_PROMETHEUS_PORT     Порт Prometheus (default 9872)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import redis

logger = logging.getLogger("redis_consumer_janitor")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default) or default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "")
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class JanitorConfig:
    redis_urls: list[str]
    interval_sec: float
    zombie_idle_ms: int        # idle threshold for zombie (no pending)
    stale_pel_idle_ms: int     # idle threshold to ACK stale PEL
    dry_run: bool
    stream_patterns: list[str]
    pel_batch: int
    min_consumers_to_clean: int

    @classmethod
    def from_env(cls) -> "JanitorConfig":
        # Collect Redis URLs: explicit JANITOR_REDIS_URLS or fallback to known envs
        urls_env = _env_str("JANITOR_REDIS_URLS", "")
        if urls_env:
            urls = [u.strip() for u in urls_env.split(",") if u.strip()]
        else:
            urls = []
            for key in ("REDIS_URL", "REDIS_TICKS_URL"):
                val = os.getenv(key, "")
                if val and val not in urls:
                    urls.append(val)
            if not urls:
                urls = ["redis://redis-worker-1:6379/0"]

        patterns_env = _env_str(
            "JANITOR_STREAM_PATTERNS",
            "events:*,signals:*,stream:*,trades:*,ml_replay_*,notify:*",
        )
        patterns = [p.strip() for p in patterns_env.split(",") if p.strip()]

        return cls(
            redis_urls=urls,
            interval_sec=_env_float("JANITOR_INTERVAL_SEC", 3600.0),
            zombie_idle_ms=_env_int("JANITOR_ZOMBIE_IDLE_MS", 7_200_000),     # 2h
            stale_pel_idle_ms=_env_int("JANITOR_STALE_PEL_IDLE_MS", 3_600_000),  # 1h
            dry_run=_env_bool("JANITOR_DRY_RUN", False),
            stream_patterns=patterns,
            pel_batch=_env_int("JANITOR_PEL_BATCH", 200),
            min_consumers_to_clean=_env_int("JANITOR_MIN_CONSUMERS_TO_CLEAN", 2),
        )


# ---------------------------------------------------------------------------
# Metrics (lazy Prometheus import — fail-open)
# ---------------------------------------------------------------------------

def _make_metrics() -> dict[str, Any]:
    try:
        from prometheus_client import Counter, Gauge, Histogram
        return {
            "zombies_deleted": Counter(
                "janitor_zombies_deleted_total",
                "Total zombie consumers deleted",
                ["redis_url", "stream", "group"],
            ),
            "pel_acked": Counter(
                "janitor_stale_pel_acked_total",
                "Total stale PEL entries ACKed",
                ["redis_url", "stream", "group"],
            ),
            "sweep_duration": Histogram(
                "janitor_sweep_duration_seconds",
                "Time spent per full sweep",
                ["redis_url"],
            ),
            "sweep_errors": Counter(
                "janitor_sweep_errors_total",
                "Total errors during sweep",
                ["redis_url", "kind"],
            ),
            "consumers_total": Gauge(
                "janitor_consumers_total",
                "Current consumer count per group",
                ["redis_url", "stream", "group"],
            ),
            "pending_total": Gauge(
                "janitor_pending_total",
                "Current pending count per group",
                ["redis_url", "stream", "group"],
            ),
            "lag_total": Gauge(
                "janitor_lag_total",
                "Current lag per group",
                ["redis_url", "stream", "group"],
            ),
        }
    except Exception:
        return {}


_metrics: dict[str, Any] = {}


def _inc(name: str, labels: dict[str, str], amount: int = 1) -> None:
    try:
        m = _metrics.get(name)
        if m is not None:
            m.labels(**labels).inc(amount)
    except Exception:
        pass


def _set(name: str, labels: dict[str, str], value: float) -> None:
    try:
        m = _metrics.get(name)
        if m is not None:
            m.labels(**labels).set(value)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

@dataclass
class SweepResult:
    stream: str
    group: str
    consumers_before: int = 0
    consumers_after: int = 0
    zombies_deleted: int = 0
    stale_pel_acked: int = 0
    pending_before: int = 0
    lag: int = 0
    errors: list[str] = field(default_factory=list)


def _decode(v: bytes | str) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _redis_url_label(url: str) -> str:
    """Compact label: redis://redis-worker-1:6379/0 → redis-worker-1:6379/0"""
    return url.replace("redis://", "").replace("rediss://", "")


def sweep_group(
    r: redis.Redis,
    *,
    stream: str,
    group: str,
    config: JanitorConfig,
    url_label: str,
) -> SweepResult:
    """Sweep one stream/group pair."""
    result = SweepResult(stream=stream, group=group)

    try:
        group_infos = r.xinfo_groups(stream)
    except Exception as e:
        result.errors.append(f"xinfo_groups: {e}")
        return result

    # Find our group
    g_info: dict | None = None
    for gi in group_infos:
        name = _decode(gi.get("name", b""))
        if name == group:
            g_info = gi
            break

    if g_info is None:
        return result  # group doesn't exist on this node

    result.pending_before = int(g_info.get("pending", 0))
    result.lag = int(g_info.get("lag") or 0)

    try:
        consumers = r.xinfo_consumers(stream, group)
    except Exception as e:
        result.errors.append(f"xinfo_consumers: {e}")
        return result

    result.consumers_before = len(consumers)

    if result.consumers_before < config.min_consumers_to_clean:
        return result

    # ── Step 1: Delete zombie consumers (pending=0, idle > threshold) ────────
    zombie_names: list[str] = []
    for c in consumers:
        name = _decode(c.get("name", b""))
        idle_ms = int(c.get("idle", 0))
        pending_cnt = int(c.get("pending", 0))

        if pending_cnt == 0 and idle_ms >= config.zombie_idle_ms:
            zombie_names.append(name)

    for name in zombie_names:
        try:
            if not config.dry_run:
                r.xgroup_delconsumer(stream, group, name)
            result.zombies_deleted += 1
            logger.debug(
                "[%s] %s::%s — deleted zombie consumer '%s' (dry_run=%s)",
                url_label, stream, group, name, config.dry_run,
            )
        except Exception as e:
            result.errors.append(f"delconsumer {name}: {e}")

    result.consumers_after = result.consumers_before - result.zombies_deleted

    # ── Step 2: ACK stale PEL (message deleted from stream, idle > threshold) ─
    if result.pending_before > 0:
        try:
            pending_entries = r.xpending_range(
                stream, group, min="-", max="+", count=config.pel_batch
            )
        except Exception as e:
            result.errors.append(f"xpending_range: {e}")
            return result

        stale_ids: list[str] = []
        for pe in pending_entries:
            msg_id = _decode(pe.get("message_id", b""))
            idle_ms = int(pe.get("time_since_delivered", 0))

            if idle_ms < config.stale_pel_idle_ms:
                continue

            # Check if message still exists in stream
            try:
                exists = r.xrange(stream, min=msg_id, max=msg_id, count=1)
                if not exists:
                    stale_ids.append(msg_id)
            except Exception:
                pass

        if stale_ids:
            try:
                if not config.dry_run:
                    r.xack(stream, group, *stale_ids)
                result.stale_pel_acked += len(stale_ids)
                logger.debug(
                    "[%s] %s::%s — ACKed %d stale PEL entries (dry_run=%s)",
                    url_label, stream, group, len(stale_ids), config.dry_run,
                )
            except Exception as e:
                result.errors.append(f"xack stale: {e}")

    return result


def sweep_redis(r: redis.Redis, config: JanitorConfig, url_label: str) -> list[SweepResult]:
    """Full sweep: discover all streams + groups, clean each."""
    results: list[SweepResult] = []

    # Discover all stream keys matching patterns
    streams: set[str] = set()
    for pat in config.stream_patterns:
        cursor = 0
        while True:
            try:
                cursor, keys = r.scan(cursor=cursor, match=pat, count=200)
            except Exception as e:
                logger.warning("[%s] scan(%s) error: %s", url_label, pat, e)
                break
            for k in keys:
                ks = _decode(k)
                try:
                    t = r.type(ks)
                    if _decode(t) == "stream":
                        streams.add(ks)
                except Exception:
                    pass
            if cursor == 0:
                break
            time.sleep(0.002)  # yield between scan batches

    logger.info("[%s] Discovered %d streams", url_label, len(streams))

    for stream in sorted(streams):
        try:
            group_infos = r.xinfo_groups(stream)
        except Exception as e:
            logger.warning("[%s] xinfo_groups(%s): %s", url_label, stream, e)
            _inc("sweep_errors", {"redis_url": url_label, "kind": "xinfo_groups"})
            continue

        for gi in group_infos:
            group = _decode(gi.get("name", b""))
            pending = int(gi.get("pending", 0))
            lag = int(gi.get("lag") or 0)
            consumers_cnt = int(gi.get("consumers", 0))

            # Publish current state metrics regardless of cleaning
            _set("consumers_total", {"redis_url": url_label, "stream": stream, "group": group}, consumers_cnt)
            _set("pending_total", {"redis_url": url_label, "stream": stream, "group": group}, pending)
            _set("lag_total", {"redis_url": url_label, "stream": stream, "group": group}, lag)

            res = sweep_group(r, stream=stream, group=group, config=config, url_label=url_label)

            if res.zombies_deleted > 0 or res.stale_pel_acked > 0 or res.errors:
                results.append(res)
                labels = {"redis_url": url_label, "stream": stream, "group": group}
                if res.zombies_deleted > 0:
                    _inc("zombies_deleted", labels, res.zombies_deleted)
                if res.stale_pel_acked > 0:
                    _inc("pel_acked", labels, res.stale_pel_acked)

    return results


def run_sweep(config: JanitorConfig) -> None:
    """Run one full sweep across all configured Redis nodes."""
    sweep_start = time.monotonic()

    for url in config.redis_urls:
        url_label = _redis_url_label(url)
        t0 = time.monotonic()
        try:
            r = redis.from_url(url, decode_responses=False, socket_timeout=10)
            r.ping()
        except Exception as e:
            logger.error("[%s] Redis connection failed: %s", url_label, e)
            _inc("sweep_errors", {"redis_url": url_label, "kind": "connection"})
            continue

        try:
            results = sweep_redis(r, config, url_label)
        except Exception as e:
            logger.error("[%s] Sweep failed: %s", url_label, e, exc_info=True)
            _inc("sweep_errors", {"redis_url": url_label, "kind": "sweep"})
            continue
        finally:
            try:
                r.close()
            except Exception:
                pass

        elapsed = time.monotonic() - t0
        try:
            m = _metrics.get("sweep_duration")
            if m:
                m.labels(redis_url=url_label).observe(elapsed)
        except Exception:
            pass

        # Summary log
        total_zombies = sum(r.zombies_deleted for r in results)
        total_pel = sum(r.stale_pel_acked for r in results)
        total_errors = sum(len(r.errors) for r in results)

        if total_zombies > 0 or total_pel > 0 or total_errors > 0:
            logger.info(
                "[%s] Sweep done in %.1fs | streams_cleaned=%d zombies_deleted=%d stale_pel_acked=%d errors=%d dry_run=%s",
                url_label, elapsed,
                len(results), total_zombies, total_pel, total_errors,
                config.dry_run,
            )
            for res in results:
                parts = []
                if res.zombies_deleted:
                    parts.append(f"zombies={res.zombies_deleted}")
                if res.stale_pel_acked:
                    parts.append(f"stale_pel={res.stale_pel_acked}")
                if res.errors:
                    parts.append(f"errors={res.errors[:3]}")
                if parts:
                    logger.info(
                        "  [%s] %s::%s consumers=%d→%d pending=%d lag=%d | %s",
                        url_label, res.stream, res.group,
                        res.consumers_before, res.consumers_after,
                        res.pending_before, res.lag,
                        " ".join(parts),
                    )
        else:
            logger.info(
                "[%s] Sweep done in %.1fs — nothing to clean",
                url_label, elapsed,
            )

    total_elapsed = time.monotonic() - sweep_start
    logger.info("Full sweep completed in %.1fs", total_elapsed)


def run_loop(config: JanitorConfig) -> None:
    """Main loop: sweep → sleep → repeat."""
    global _metrics
    _metrics = _make_metrics()

    logger.info(
        "Janitor started | interval=%.0fs zombie_idle=%.0fh stale_pel_idle=%.0fh dry_run=%s urls=%s",
        config.interval_sec,
        config.zombie_idle_ms / 3_600_000,
        config.stale_pel_idle_ms / 3_600_000,
        config.dry_run,
        config.redis_urls,
    )

    while True:
        try:
            run_sweep(config)
        except Exception as e:
            logger.error("Unexpected sweep error: %s", e, exc_info=True)

        logger.info("Next sweep in %.0f seconds", config.interval_sec)
        time.sleep(config.interval_sec)
