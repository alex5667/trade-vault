#!/usr/bin/env python3
"""
redis_lag_monitor.py — мониторинг Consumer Lag по всем Redis Streams тиков.

Показывает в реальном времени:
  - XLEN (длина стрима)
  - lag (nack'нутые / не доставленные сообщения) по каждой consumer group
  - pending (PEL: delivered but not ACK'нутые)
  - consumers (кол-во воркеров в group)
  - oldest_idle_ms (возраст самого старого PEL-сообщения, признак застрявшего воркера)

Запуск:
  # Внутри контейнера:
  docker exec -it scanner-crypto-orderflow python tools/redis_lag_monitor.py

  # Или снаружи (прямой доступ к Redis):
  REDIS_URL=redis://localhost:6379/0 python tools/redis_lag_monitor.py

  # Опции:
  --interval 5       # обновление каждые N секунд (по умолчанию: 3)
  --pattern "stream:tick_*"  # шаблон стримов (по умолчанию: stream:tick_*)
  --warn-lag 50      # порог предупреждения по lag (по умолчанию: 50 сообщений)
  --warn-pending 100 # порог предупреждения по pending (по умолчанию: 100)
  --warn-idle 5000   # порог старого PEL-сообщения, мс (по умолчанию: 5000)
  --once             # запустить один раз и выйти (удобно для cron/alert)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

try:
    import redis
except ImportError:
    print("❌ redis-py не установлен. Установите: pip install redis", file=sys.stderr)
    sys.exit(1)

# ─── ANSI цвета ────────────────────────────────────────────────────────────────
RESET  = "\033[0m"
RED    = "\033[31m"
YELLOW = "\033[33m"
GREEN  = "\033[32m"
BOLD   = "\033[1m"
CYAN   = "\033[36m"
DIM    = "\033[2m"


def _color(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + RESET


# ─── Данные ────────────────────────────────────────────────────────────────────

@dataclass
class GroupInfo:
    name: str
    consumers: int = 0
    pending: int = 0
    lag: int = 0                  # Redis ≥7.0: XINFO GROUPS lag field
    last_delivered_id: str = ""
    oldest_idle_ms: int = 0       # из XPENDING RANGE -+  COUNT 1


@dataclass
class StreamInfo:
    name: str
    xlen: int = 0
    groups: List[GroupInfo] = field(default_factory=list)
    error: Optional[str] = None


# ─── Сбор данных ───────────────────────────────────────────────────────────────

def _decode(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return str(v)


def _get_oldest_idle(r: redis.Redis, stream: str, group: str) -> int:
    """XPENDING RANGE - + COUNT 1 → возвращает time_since_delivered самого старого сообщения."""
    try:
        items = r.xpending_range(stream, group, "-", "+", 1)
        if items:
            it = items[0]
            if isinstance(it, dict):
                return int(it.get("time_since_delivered") or 0)
    except Exception:
        pass
    return 0


def collect_stream_info(r: redis.Redis, stream: str, warn_pending: int, warn_idle: int) -> StreamInfo:
    info = StreamInfo(name=stream)
    try:
        info.xlen = r.xlen(stream)
    except Exception as e:
        info.error = str(e)
        return info

    try:
        raw_groups = r.xinfo_groups(stream)
    except Exception as e:
        info.error = f"XINFO GROUPS failed: {e}"
        return info

    for g in raw_groups or []:
        if isinstance(g, dict):
            gd = {_decode(k): v for k, v in g.items()}
        else:
            # flat list: [k, v, k, v, ...]
            gd = {}
            it = list(g)
            for i in range(0, len(it) - 1, 2):
                gd[_decode(it[i])] = it[i + 1]

        name      = _decode(gd.get("name", ""))
        consumers = int(gd.get("consumers") or 0)
        pending   = int(gd.get("pel-count") or gd.get("pending") or 0)
        lag       = int(gd.get("lag") or 0)
        last_id   = _decode(gd.get("last-delivered-id") or "")

        oldest_idle = 0
        # Только если PEL > 0 и это неприятно большой → не спамим Redis лишними запросами
        if pending > 0:
            oldest_idle = _get_oldest_idle(r, stream, name)

        info.groups.append(GroupInfo(
            name=name,
            consumers=consumers,
            pending=pending,
            lag=lag,
            last_delivered_id=last_id,
            oldest_idle_ms=oldest_idle,
        ))

    return info


def collect_all(r: redis.Redis, pattern: str, warn_pending: int, warn_idle: int) -> List[StreamInfo]:
    streams = sorted(r.scan_iter(pattern, count=10000))
    result = []
    for s in streams:
        name = _decode(s)
        if name.endswith(":quarantine"):  # пропускаем quarantine-стримы
            continue
        result.append(collect_stream_info(r, name, warn_pending, warn_idle))
    return result


# ─── Вывод ─────────────────────────────────────────────────────────────────────

def fmt_lag(v: int, warn: int) -> str:
    if v == 0:
        return _color("0", GREEN)
    if v >= warn * 5:
        return _color(str(v), RED, BOLD)
    if v >= warn:
        return _color(str(v), YELLOW)
    return str(v)


def fmt_pending(v: int, warn: int) -> str:
    if v == 0:
        return _color("0", GREEN)
    if v >= warn * 5:
        return _color(str(v), RED, BOLD)
    if v >= warn:
        return _color(str(v), YELLOW)
    return str(v)


def fmt_idle(v: int, warn: int) -> str:
    if v == 0:
        return _color("—", DIM)
    sec = v / 1000
    label = f"{sec:.1f}s"
    if v >= warn * 5:
        return _color(label, RED, BOLD)
    if v >= warn:
        return _color(label, YELLOW)
    return _color(label, GREEN)


def print_table(infos: List[StreamInfo], warn_lag: int, warn_pending: int, warn_idle: int) -> None:
    # Заголовок
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{_color('  Redis Stream Consumer Lag Monitor', BOLD, CYAN)}  {_color(ts, DIM)}")
    print("─" * 110)
    hdr = f"{'STREAM':<35} {'GROUP':<28} {'CONS':>5} {'XLEN':>8} {'LAG':>8} {'PENDING':>8} {'OLDEST_IDLE':>12}"
    print(_color(hdr, BOLD))
    print("─" * 110)

    total_lag = 0
    total_pending = 0
    has_warn = False

    for si in infos:
        sym = si.name.replace("stream:tick_", "")
        if si.error:
            print(f"  {_color(si.name, RED):<35} {_color('ERROR: ' + si.error, RED)}")
            continue
        if not si.groups:
            print(f"  {sym:<35} {_color('(no consumer groups)', DIM)}")
            continue

        for i, g in enumerate(si.groups):
            stream_col = sym if i == 0 else ""
            xlen_col   = str(si.xlen) if i == 0 else ""
            lag_s      = fmt_lag(g.lag, warn_lag)
            pending_s  = fmt_pending(g.pending, warn_pending)
            idle_s     = fmt_idle(g.oldest_idle_ms, warn_idle)

            total_lag     += g.lag
            total_pending += g.pending

            if g.lag >= warn_lag or g.pending >= warn_pending or g.oldest_idle_ms >= warn_idle:
                has_warn = True

            print(f"  {stream_col:<35} {g.name:<28} {g.consumers:>5} {xlen_col:>8} {lag_s:>16} {pending_s:>16} {idle_s:>20}")

    print("─" * 110)
    total_lag_s     = fmt_lag(total_lag, warn_lag)
    total_pending_s = fmt_pending(total_pending, warn_pending)
    print(f"  {'TOTAL':<35} {'':28} {'':>5} {'':>8} {total_lag_s:>16} {total_pending_s:>16}")

    if has_warn:
        print(f"\n  {_color('⚠️  Обнаружены проблемы! Lag или Pending выше порога.', YELLOW, BOLD)}")
    else:
        print(f"\n  {_color('✅  Все очереди в норме.', GREEN)}")


# ─── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Redis Stream Consumer Lag Monitor")
    ap.add_argument("--url",          default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
                    help="Redis URL (default: REDIS_URL env или redis://redis-worker-1:6379/0)")
    ap.add_argument("--pattern",      default="stream:tick_*", help="Glob шаблон для SCAN (по умолчанию: stream:tick_*)")
    ap.add_argument("--interval",     type=float, default=3.0, help="Интервал обновления, сек (по умолчанию: 3)")
    ap.add_argument("--warn-lag",     type=int,   default=50,  help="Порог WARNING по lag (по умолчанию: 50)")
    ap.add_argument("--warn-pending", type=int,   default=100, help="Порог WARNING по pending (по умолчанию: 100)")
    ap.add_argument("--warn-idle",    type=int,   default=5000, help="Порог WARNING по oldest_idle_ms (по умолчанию: 5000)")
    ap.add_argument("--once",         action="store_true", help="Запустить один раз и выйти")
    args = ap.parse_args()

    r = redis.from_url(args.url, decode_responses=False, socket_connect_timeout=5, socket_timeout=10)
    try:
        r.ping()
    except Exception as e:
        print(f"❌ Не могу подключиться к Redis ({args.url}): {e}", file=sys.stderr)
        sys.exit(1)

    print(f"🔌 Подключён к Redis: {args.url}")
    print(f"📡 Паттерн: {args.pattern}  |  Интервал: {args.interval}s  |  "
          f"Порог lag: {args.warn_lag}  |  Порог pending: {args.warn_pending}  |  "
          f"Порог idle: {args.warn_idle}ms")

    while True:
        try:
            infos = collect_all(r, args.pattern, args.warn_pending, args.warn_idle)
            print_table(infos, args.warn_lag, args.warn_pending, args.warn_idle)
        except KeyboardInterrupt:
            print("\n👋 Выход.")
            break
        except Exception as e:
            print(f"\n❌ Ошибка сбора метрик: {e}", file=sys.stderr)

        if args.once:
            break
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n👋 Выход.")
            break


if __name__ == "__main__":
    main()
