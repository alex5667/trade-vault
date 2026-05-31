# python-worker/tools/cron_demo_order_report.py
"""
Отдельный отчёт по ордерам, отправленным на демо-счёт (is_virtual=true).

Читает Redis Stream orders:exec, фильтрует события с is_virtual=true / venue=binance_demo,
считает статистику и отправляет HTML-отчёт в Telegram (формат идентичен CryptoOrderFlow
OF Gate Reports из cron_of_reports.py).

Usage:
    cd python-worker
    REDIS_URL=redis://localhost:6379/0 \\
    python -m tools.cron_demo_order_report --mode monitor

ENV:
    REDIS_URL                   Redis URL                    (redis://localhost:6379/0)
    EXEC_STREAM                 Stream с exec-событиями     (orders:exec)
    DEMO_REPORT_SINCE_HOURS     Окно в часах                (24)
    DEMO_REPORT_MAX_SCAN        Макс. записей для скана     (200000)
    DEMO_REPORT_OK_RATE_WARN    Порог ok_rate для алерта    (0.0)
    TELEGRAM_MODE               redis | direct              (redis)
    TELEGRAM_NOTIFY_STREAM      Redis notify-стрим          (notify:telegram)
    TELEGRAM_REDIS_URL          Redis для Telegram          (= REDIS_URL)
    TELEGRAM_BOT_TOKEN          Для direct-режима
    TELEGRAM_CHAT_ID            Для direct-режима
"""
from __future__ import annotations

import argparse
import html
import json
import os
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple


# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _envs(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _envi(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, default) or default)
    except Exception:
        return int(default)


def _envf(name: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(name, default) or default)
    except Exception:
        return float(default)


# ---------------------------------------------------------------------------
# Data layer: read orders:exec stream
# ---------------------------------------------------------------------------

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _is_demo_open(fields: Dict[str, Any]) -> bool:
    """Return True if this exec event is a FILLED demo-account open."""
    # Only care about open (entry) events; skip SL/TP/modify child events
    action = str(fields.get("action") or "").lower()
    if action not in ("open", ""):
        return False

    # Primary flag: is_virtual=true
    if _truthy(fields.get("is_virtual")):
        return True

    # Secondary: venue=binance_demo
    venue = str(fields.get("venue") or "").lower()
    if venue == "binance_demo":
        return True

    return False


@dataclass
class DemoOrder:
    sid: str
    symbol: str
    side: str                  # LONG / SHORT
    exec_price: float
    qty: float
    scenario_v4: str
    of_confirm_ok: int         # should be 0 for proper demo routing
    of_confirm_ok_soft: int    # should be 1 for shadow signals
    execution_policy: str
    ts_ms: int

    @classmethod
    def from_fields(cls, fields: Dict[str, Any], stream_ts_ms: int) -> "DemoOrder":
        def _f(k: str, d: float = 0.0) -> float:
            try:
                return float(fields.get(k) or d)
            except Exception:
                return d

        def _i(k: str, d: int = 0) -> int:
            try:
                return int(float(fields.get(k) or d))
            except Exception:
                return d

        side_raw = str(fields.get("side") or fields.get("logical_side") or "").upper()
        if side_raw in ("BUY",):
            side_raw = "LONG"
        elif side_raw in ("SELL",):
            side_raw = "SHORT"

        ts = _i("ts_ms", 0) or stream_ts_ms

        return cls(
            sid=str(fields.get("sid") or ""),
            symbol=str(fields.get("symbol") or "").upper(),
            side=side_raw or "?",
            exec_price=_f("exec_price"),
            qty=_f("qty"),
            scenario_v4=str(fields.get("scenario_v4") or "na").lower(),
            of_confirm_ok=_i("of_confirm_ok", 0),
            of_confirm_ok_soft=_i("of_confirm_ok_soft", 0),
            execution_policy=str(fields.get("execution_policy") or "UNKNOWN").upper(),
            ts_ms=ts,
        )


def collect_demo_orders(
    *,
    redis_client: Any,
    stream: str,
    since_ms: int,
    max_scan: int = 200_000,
) -> List[DemoOrder]:
    """
    Читает stream в обратном порядке и собирает демо-ордера за период [since_ms, now].
    Возвращает список DemoOrder (в хронологическом порядке).
    """
    orders: List[DemoOrder] = []
    last_id = "+"
    scanned = 0

    while scanned < max_scan:
        batch = redis_client.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break

        # detect stuck cursor
        new_entries = [e for e in batch if e[0] != last_id]
        if not new_entries and last_id != "+":
            break

        for msg_id, raw_fields in batch:
            if msg_id == last_id and last_id != "+":
                continue
            scanned += 1

            # stream entry ID encodes ms timestamp: "1234567890123-0"
            try:
                stream_ts_ms = int(str(msg_id).split("-")[0])
            except Exception:
                stream_ts_ms = 0

            # Decode bytes → str if needed
            fields: Dict[str, Any] = {}
            for k, v in (raw_fields or {}).items():
                k2 = k.decode("utf-8") if isinstance(k, bytes) else k
                v2 = v.decode("utf-8") if isinstance(v, bytes) else v
                fields[k2] = v2

            # Stop if we've gone past our window
            ts = int(fields.get("ts_ms") or 0) or stream_ts_ms
            if ts and ts < since_ms:
                # xrevrange is time-descending; past window → stop
                scanned = max_scan
                break

            if not _is_demo_open(fields):
                continue

            o = DemoOrder.from_fields(fields, stream_ts_ms)
            if not o.symbol:
                continue
            orders.append(o)

        if new_entries:
            last_id = new_entries[-1][0]
        else:
            break

    # Return in chronological order
    orders.sort(key=lambda x: x.ts_ms)
    return orders


# ---------------------------------------------------------------------------
# Statistics computation
# ---------------------------------------------------------------------------

@dataclass
class DemoStats:
    n: int
    by_symbol: Dict[str, Dict[str, Any]]
    by_scenario: Dict[str, int]
    by_policy: Dict[str, int]
    ok_rate: float       # share of of_confirm_ok=1 (SHOULD be 0; >0 is misconfiguration)
    ok_soft_rate: float  # share of of_confirm_ok_soft=1 (expected ~1.0)
    long_count: int
    short_count: int
    since_hours: float


def compute_demo_stats(orders: List[DemoOrder], *, since_hours: float) -> DemoStats:
    n = len(orders)
    if n == 0:
        return DemoStats(
            n=0,
            by_symbol={},
            by_scenario={},
            by_policy={},
            ok_rate=0.0,
            ok_soft_rate=0.0,
            long_count=0,
            short_count=0,
            since_hours=since_hours,
        )

    by_sym: Dict[str, Dict[str, Any]] = {}
    by_scn: Counter = Counter()
    by_pol: Counter = Counter()
    ok_total = 0
    ok_soft_total = 0
    long_count = 0
    short_count = 0

    for o in orders:
        sym = o.symbol or "?"
        if sym not in by_sym:
            by_sym[sym] = {"n": 0, "ok": 0, "ok_soft": 0, "long": 0, "short": 0}
        by_sym[sym]["n"] += 1
        by_sym[sym]["ok"] += o.of_confirm_ok
        by_sym[sym]["ok_soft"] += o.of_confirm_ok_soft
        if o.side == "LONG":
            by_sym[sym]["long"] += 1
            long_count += 1
        elif o.side == "SHORT":
            by_sym[sym]["short"] += 1
            short_count += 1

        by_scn[o.scenario_v4 or "na"] += 1
        by_pol[o.execution_policy or "UNKNOWN"] += 1
        ok_total += o.of_confirm_ok
        ok_soft_total += o.of_confirm_ok_soft

    # Compute per-symbol rates
    for sym, v in by_sym.items():
        nn = v["n"]
        v["ok_soft_rate"] = v["ok_soft"] / nn if nn else 0.0
        v["ok_rate"] = v["ok"] / nn if nn else 0.0

    return DemoStats(
        n=n,
        by_symbol=by_sym,
        by_scenario=dict(by_scn),
        by_policy=dict(by_pol),
        ok_rate=ok_total / n if n else 0.0,
        ok_soft_rate=ok_soft_total / n if n else 0.0,
        long_count=long_count,
        short_count=short_count,
        since_hours=since_hours,
    )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def build_report_text(
    stats: DemoStats,
    *,
    mode: str,
    ts: str,
    ok_rate_warn: float = 0.0,
) -> str:
    """Build HTML report text (Telegram parse_mode=HTML)."""
    lines: List[str] = []

    # ── Header ──────────────────────────────────────────────────────────────
    lines.append(
        f"<b>Demo Order Report</b>  mode=<code>{html.escape(mode)}</code>"
        f"  ts=<code>{html.escape(ts)}</code>"
    )

    window_h = stats.since_hours
    window_label = (
        f"{int(window_h)}h" if window_h == int(window_h) else f"{window_h:.1f}h"
    )

    if stats.n == 0:
        lines.append(f"window=<code>{window_label}</code>  <i>нет демо-ордеров за период</i>")
        return "\n".join(lines)

    ok_s = f"{stats.ok_rate:.3f}"
    ok_soft_s = f"{stats.ok_soft_rate:.3f}"
    n_syms = len(stats.by_symbol)

    lines.append(
        f"n=<code>{stats.n}</code>  symbols=<code>{n_syms}</code>"
        f"  window=<code>{window_label}</code>"
    )
    lines.append(
        f"ok_rate=<code>{ok_s}</code>  ok_soft_rate=<code>{ok_soft_s}</code>"
        f"  L=<code>{stats.long_count}</code>  S=<code>{stats.short_count}</code>"
    )

    # ── Misconfiguration alert ───────────────────────────────────────────────
    if ok_rate_warn >= 0 and stats.ok_rate > ok_rate_warn:
        lines.append(
            f"\n⚠️  <b>ok_rate={ok_s} &gt; 0</b> — demo-ордера имеют of_confirm_ok=1."
            " Проверьте <code>BINANCE_CLIENT_MODE</code> routing."
        )

    # ── By symbol ────────────────────────────────────────────────────────────
    lines.append("")
    lines.append("<b>By symbol</b>")
    for sym, v in sorted(stats.by_symbol.items(), key=lambda kv: -kv[1]["n"])[:12]:
        soft_s = f"{v['ok_soft_rate']:.2f}"
        ok_sym_s = f"{v['ok_rate']:.2f}"
        lines.append(
            f"- <code>{html.escape(sym)}</code>:"
            f" n={v['n']}"
            f" ok_soft={soft_s}"
            f" ok={ok_sym_s}"
            f" L={v['long']} S={v['short']}"
        )

    # ── By scenario ──────────────────────────────────────────────────────────
    lines.append("")
    lines.append("<b>By scenario</b>")
    for scn, cnt in sorted(stats.by_scenario.items(), key=lambda kv: -kv[1])[:10]:
        lines.append(f"- <code>{html.escape(scn)}</code>: {cnt}")

    # ── By policy ────────────────────────────────────────────────────────────
    lines.append("")
    lines.append("<b>By execution policy</b>")
    for pol, cnt in sorted(stats.by_policy.items(), key=lambda kv: -kv[1]):
        lines.append(f"- <code>{html.escape(pol)}</code>: {cnt}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram delivery (identical pattern to cron_of_reports.py)
# ---------------------------------------------------------------------------

def send_report_redis(redis_url: str, stream: str, text: str) -> None:
    import redis as redis_lib
    r = redis_lib.Redis.from_url(redis_url)
    r.xadd(
        stream,
        {
            "type": "report",
            "text": text,
            "parse_mode": "HTML",
            "ts": str(int(time.time() * 1000)),
        },
        maxlen=200_000,
        approximate=True,
    )


def send_report_direct(token: str, chat_id: str, text: str) -> None:
    import requests
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=15,
    ).raise_for_status()


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_report(mode: str) -> int:
    import redis as redis_lib

    redis_url = _envs("REDIS_URL", "redis://localhost:6379/0")
    exec_stream = _envs("EXEC_STREAM", "orders:exec")
    since_hours = _envf("DEMO_REPORT_SINCE_HOURS", 24.0)
    max_scan = _envi("DEMO_REPORT_MAX_SCAN", 200_000)
    ok_rate_warn = _envf("DEMO_REPORT_OK_RATE_WARN", 0.0)

    since_ms = int(time.time() * 1000) - int(since_hours * 3_600_000)
    ts = time.strftime("%Y%m%d_%H%M%S")

    print(f"[demo_report] mode={mode} stream={exec_stream} since={since_hours}h ts={ts}")

    r = redis_lib.from_url(redis_url, decode_responses=False)

    orders = collect_demo_orders(
        redis_client=r,
        stream=exec_stream,
        since_ms=since_ms,
        max_scan=max_scan,
    )
    print(f"[demo_report] collected {len(orders)} demo orders")

    stats = compute_demo_stats(orders, since_hours=since_hours)
    text = build_report_text(stats, mode=mode, ts=ts, ok_rate_warn=ok_rate_warn)

    tg_mode = _envs("TELEGRAM_MODE", "redis").lower()
    if tg_mode == "direct":
        token = _envs("TELEGRAM_BOT_TOKEN")
        chat_id = _envs("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required for direct mode")
            return 1
        send_report_direct(token, chat_id, text)
        print("[demo_report] sent via direct Telegram API")
    else:
        notify_redis_url = _envs("TELEGRAM_REDIS_URL") or redis_url
        notify_stream = _envs("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
        send_report_redis(notify_redis_url, notify_stream, text)
        print(f"[demo_report] sent to Redis stream {notify_stream}")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Demo account order report (CryptoOrderFlow format)")
    ap.add_argument("--mode", choices=["monitor", "daily"], default="monitor")
    args = ap.parse_args()
    return run_report(args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
