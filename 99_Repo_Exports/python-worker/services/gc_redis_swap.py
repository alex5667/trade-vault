#!/usr/bin/env python3
"""
gc_redis_swap.py — Redis GC + Swap flush daemon.

Loops:
  - Every GC_REDIS_INTERVAL_S (default 1800): MEMORY PURGE + XTRIM overlong streams
  - Every GC_PEL_INTERVAL_S   (default 7200): ACK stale PEL entries (idle > 24h)
  - Every GC_SWAP_INTERVAL_S  (default 3600): flush swap if safe (MemAvailable > SwapUsed + headroom)
  Exports Prometheus text to GC_PROM_FILE (default /tmp/gc_redis_swap.prom).
"""
import asyncio
import logging
import os
import subprocess
import time
from pathlib import Path

import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [gc] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
LOG = logging.getLogger("gc_redis_swap")

# ── Redis instances to manage ──────────────────────────────────────────────────
_REDIS_URLS: list[str] = [
    u for u in [
        os.getenv("REDIS_URL"),
        os.getenv("REDIS_WORKER_2_URL"),
        os.getenv("REDIS_TICKS_URL"),
    ]
    if u
]

# ── Stream trim policy: pattern → hard maxlen ──────────────────────────────────
# Approximate trim (XTRIM ... MAXLEN ~) — does not block.
_STREAM_MAXLEN: dict[str, int] = {
    # high-frequency tick / book  (go-worker → python; consumers read last N)
    "stream:tick_*":                        2_000,
    "stream:book_*":                        1_000,
    "stream:ticker-24h":                    2_000,
    # signal pipeline
    "stream:signals:outbox":               50_000,
    "stream:signals:gated_out":            10_000,
    "stream:signals:gated_out_outcomes":   10_000,
    "stream:signals:diag":                  5_000,
    "stream:signals:diagnostics":           5_000,
    "stream:signals:exec_events":          10_000,
    "stream:signals:plans":                 5_000,
    "stream:signals:labels":               5_000,
    "stream:signals:bridge:*":              5_000,
    "stream:signals:manual":               5_000,
    "stream:signals:dlq":                  5_000,
    "stream:signals:dlq:*":                5_000,
    # metrics streams (bounded internally, but enforce as backstop)
    "metrics:of_gate":                     50_000,
    "metrics:ml_confirm":                  20_000,
    "metrics:tick_time":                   10_000,
    "metrics:ml_outcome":                  20_000,
    # notifications
    "notify:telegram":                        500,
    "notify:telegram:crit":                   200,
    "notify:telegram:page":                   200,
    # events / audit
    "stream:liq_evt":                      10_000,
    "stream:liq_evt_quarantine":            5_000,
    "stream:tick_dq:quarantine":            5_000,
    "stream:tick_side:quarantine":          5_000,
    "stream:regime":                        5_000,
    "trade:fsm:audit":                     10_000,
    "stream:news_labels":                   5_000,
    "stream:signals_news":                  5_000,
    "stream:manual-signals":               2_000,
}

# ── Intervals ──────────────────────────────────────────────────────────────────
GC_REDIS_INTERVAL_S     = int(os.getenv("GC_REDIS_INTERVAL_S",     "1800"))  # 30 min
GC_PEL_INTERVAL_S       = int(os.getenv("GC_PEL_INTERVAL_S",       "7200"))  # 2 h
GC_SWAP_INTERVAL_S      = int(os.getenv("GC_SWAP_INTERVAL_S",       "3600"))  # 1 h
# Swap flush: only trigger if swap_used_pct > threshold AND MemAvailable > swap_used + headroom
GC_SWAP_FLUSH_PCT       = float(os.getenv("GC_SWAP_FLUSH_TRIGGER_PCT",  "15.0"))
GC_SWAP_HEADROOM_MB     = int(os.getenv("GC_SWAP_FREE_RAM_HEADROOM_MB", "1500"))
GC_SWAP_FLUSH_ENABLED   = os.getenv("GC_SWAP_FLUSH_ENABLED", "1") == "1"
GC_PROM_FILE            = os.getenv("GC_PROM_FILE", "/tmp/gc_redis_swap.prom")

_metrics: dict[str, float] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Redis GC
# ─────────────────────────────────────────────────────────────────────────────

async def _connect(url: str) -> aioredis.Redis | None:
    try:
        r = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=5)
        await r.ping()
        return r
    except Exception as e:
        LOG.warning("Cannot connect to %s: %s", url, e)
        return None


async def _gc_one(url: str) -> dict:
    stats = {"trimmed": 0, "purge_ok": 0, "errors": 0}
    r = await _connect(url)
    if r is None:
        stats["errors"] += 1
        return stats

    # MEMORY PURGE — return fragmented jemalloc arenas to OS
    try:
        await r.execute_command("MEMORY", "PURGE")
        stats["purge_ok"] = 1
        info = await r.info("memory")
        used = info.get("used_memory_human", "?")
        frag = info.get("mem_fragmentation_ratio", "?")
        LOG.info("[%s] MEMORY PURGE OK  used=%s  frag=%.2s", url, used, frag)
    except Exception as e:
        LOG.warning("[%s] MEMORY PURGE: %s", url, e)

    # XTRIM overlong streams
    for pattern, maxlen in _STREAM_MAXLEN.items():
        try:
            if "*" in pattern:
                keys: list[str] = []
                cur = 0
                while True:
                    cur, batch = await r.scan(cur, match=pattern, count=500, _type="stream")
                    keys.extend(batch)
                    if cur == 0:
                        break
            else:
                t = await r.type(pattern)
                keys = [pattern] if t == "stream" else []

            for key in keys:
                length = await r.xlen(key)
                if length > maxlen:
                    await r.xtrim(key, maxlen=maxlen, approximate=True)
                    delta = length - maxlen
                    stats["trimmed"] += delta
                    LOG.info("[%s] XTRIM %-52s %6d → %6d (-%d)", url, key, length, maxlen, delta)
        except Exception as e:
            LOG.warning("[%s] trim %s: %s", url, pattern, e)
            stats["errors"] += 1

    await r.aclose()
    return stats


async def gc_redis_loop() -> None:
    # run immediately on startup, then every interval
    while True:
        LOG.info("=== Redis GC ===")
        total: dict[str, float] = {"trimmed": 0, "purge_ok": 0, "errors": 0}
        for url in _REDIS_URLS:
            s = await _gc_one(url)
            for k in total:
                total[k] += s.get(k, 0)
        _metrics.update({
            "gc_redis_trimmed_entries_total": total["trimmed"],
            "gc_redis_purge_ok_total":        total["purge_ok"],
            "gc_redis_errors_total":          total["errors"],
            "gc_redis_last_run_ts":           time.time(),
        })
        _write_prom()
        await asyncio.sleep(GC_REDIS_INTERVAL_S)


# ─────────────────────────────────────────────────────────────────────────────
# PEL cleanup — ACK stale pending messages (idle > 24h)
# ─────────────────────────────────────────────────────────────────────────────

_PEL_IDLE_MS = 86_400_000  # 24 hours


async def _pel_cleanup_one(url: str) -> int:
    cleaned = 0
    r = await _connect(url)
    if r is None:
        return 0

    cur = 0
    while True:
        cur, keys = await r.scan(cur, match="stream:*", count=200, _type="stream")
        for key in keys:
            try:
                groups = await r.xinfo_groups(key)
                for g in groups:
                    if g.get("pel-count", 0) == 0:
                        continue
                    gname = g["name"]
                    pending = await r.xpending_range(key, gname, min="-", max="+", count=200)
                    stale = [
                        p["message_id"]
                        for p in pending
                        if p.get("time_since_delivered", 0) > _PEL_IDLE_MS
                    ]
                    if stale:
                        await r.xack(key, gname, *stale)
                        cleaned += len(stale)
                        LOG.info("[%s] PEL ACK %d stale msgs  %s / %s", url, len(stale), key, gname)
            except Exception:
                pass
        if cur == 0:
            break

    await r.aclose()
    return cleaned


async def pel_cleanup_loop() -> None:
    await asyncio.sleep(GC_PEL_INTERVAL_S)  # first run after initial delay
    while True:
        LOG.info("=== PEL cleanup ===")
        total = 0
        for url in _REDIS_URLS:
            total += await _pel_cleanup_one(url)
        _metrics["gc_pel_cleaned_total"] = total
        _metrics["gc_pel_last_run_ts"]   = time.time()
        _write_prom()
        await asyncio.sleep(GC_PEL_INTERVAL_S)


# ─────────────────────────────────────────────────────────────────────────────
# Swap flush
# ─────────────────────────────────────────────────────────────────────────────

_MEMINFO_PATH = os.getenv("HOST_MEMINFO_PATH", "/proc/meminfo")


def _meminfo() -> dict[str, int]:
    data: dict[str, int] = {}
    for line in Path(_MEMINFO_PATH).read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            data[parts[0].rstrip(":")] = int(parts[1])
    return data


def _swap_devices() -> list[str]:
    try:
        lines = Path("/proc/swaps").read_text().splitlines()[1:]
        return [line.split()[0] for line in lines if line.strip()]
    except Exception:
        return []


def _try_flush_swap() -> bool:
    mi = _meminfo()
    swap_total = mi.get("SwapTotal", 0)
    swap_free  = mi.get("SwapFree", 0)
    swap_used  = swap_total - swap_free
    mem_avail  = mi.get("MemAvailable", 0)

    if swap_total == 0:
        return False

    pct = 100.0 * swap_used / swap_total
    _metrics.update({
        "gc_swap_used_bytes":       swap_used * 1024,
        "gc_swap_used_pct":         pct,
        "gc_mem_available_bytes":   mem_avail * 1024,
    })
    LOG.info("Swap: %.1f%% used (%d MB)  MemAvailable: %d MB",
             pct, swap_used // 1024, mem_avail // 1024)

    if pct < GC_SWAP_FLUSH_PCT:
        return False

    headroom_kb = GC_SWAP_HEADROOM_MB * 1024
    if mem_avail < swap_used + headroom_kb:
        LOG.warning("Swap flush skipped: not enough RAM (need %d MB free, have %d MB)",
                    (swap_used + headroom_kb) // 1024, mem_avail // 1024)
        return False

    devs = _swap_devices()
    if not devs:
        LOG.warning("No swap devices in /proc/swaps")
        return False

    LOG.info("Flushing swap  pct=%.1f%%  swap_used=%d MB  devices=%s",
             pct, swap_used // 1024, devs)
    try:
        subprocess.run(["swapoff", "-a"], check=True, timeout=300)
        subprocess.run(["swapon",  "-a"], check=True, timeout=30)
        LOG.info("Swap flush OK")
        _metrics["gc_swap_flush_total"]   = _metrics.get("gc_swap_flush_total", 0) + 1
        _metrics["gc_swap_last_flush_ts"] = time.time()
        return True
    except subprocess.CalledProcessError as e:
        LOG.error("Swap flush failed: %s", e)
    except subprocess.TimeoutExpired:
        LOG.error("Swap flush timed out (swapoff >5 min — RAM too tight?)")
    return False


async def swap_loop() -> None:
    if not GC_SWAP_FLUSH_ENABLED:
        LOG.info("Swap flush disabled (GC_SWAP_FLUSH_ENABLED=0)")
        return
    while True:
        await asyncio.sleep(GC_SWAP_INTERVAL_S)
        _try_flush_swap()
        _write_prom()


# ─────────────────────────────────────────────────────────────────────────────
# Prometheus textfile
# ─────────────────────────────────────────────────────────────────────────────

_PROM_DEFS = [
    ("gc_redis_trimmed_entries_total", "counter", "Stream entries trimmed total"),
    ("gc_redis_purge_ok_total",        "counter", "MEMORY PURGE calls succeeded"),
    ("gc_redis_errors_total",          "counter", "GC errors"),
    ("gc_redis_last_run_ts",           "gauge",   "Last Redis GC unix timestamp"),
    ("gc_swap_used_bytes",             "gauge",   "Current swap used bytes"),
    ("gc_swap_used_pct",               "gauge",   "Current swap used percent"),
    ("gc_mem_available_bytes",         "gauge",   "MemAvailable bytes"),
    ("gc_swap_flush_total",            "counter", "Swap flushes performed"),
    ("gc_swap_last_flush_ts",          "gauge",   "Last swap flush unix timestamp"),
    ("gc_pel_cleaned_total",           "counter", "PEL entries ACKed"),
    ("gc_pel_last_run_ts",             "gauge",   "Last PEL cleanup unix timestamp"),
]


def _write_prom() -> None:
    lines: list[str] = []
    for name, typ, help_text in _PROM_DEFS:
        val = _metrics.get(name, 0)
        lines += [
            f"# HELP {name} {help_text}",
            f"# TYPE {name} {typ}",
            f"{name} {val:.6g}",
        ]
    try:
        Path(GC_PROM_FILE).write_text("\n".join(lines) + "\n")
    except Exception as e:
        LOG.warning("prom write: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _main() -> None:
    LOG.info(
        "gc_redis_swap start  redis=%d urls  gc_interval=%ds  swap_interval=%ds  swap_flush=%s",
        len(_REDIS_URLS), GC_REDIS_INTERVAL_S, GC_SWAP_INTERVAL_S,
        "enabled" if GC_SWAP_FLUSH_ENABLED else "disabled",
    )
    if not _REDIS_URLS:
        LOG.error("No REDIS_URL configured — exit")
        return

    # initial swap metrics snapshot
    try:
        mi = _meminfo()
        swap_used = mi.get("SwapTotal", 0) - mi.get("SwapFree", 0)
        _metrics.update({
            "gc_swap_used_bytes":     swap_used * 1024,
            "gc_swap_used_pct":       100.0 * swap_used / max(mi.get("SwapTotal", 1), 1),
            "gc_mem_available_bytes": mi.get("MemAvailable", 0) * 1024,
        })
        _write_prom()
    except Exception:
        pass

    await asyncio.gather(
        gc_redis_loop(),
        pel_cleanup_loop(),
        swap_loop(),
    )


if __name__ == "__main__":
    asyncio.run(_main())
