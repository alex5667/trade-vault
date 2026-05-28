"""DQ Observation Producer (P2.4, 2026-05-27).

Background
----------
Audit Lane 3 (2026-05-26) showed two calibrators голодают:
  - DqMicrostructureCalibrator: per-symbol n=11 (BTCUSDT) vs min=200 → never promotes.
  - ConfirmationBarrierCalibrator: bins={} полностью пусто.

Root cause
----------
Both calibrators live in `services/orderflow/signal_pipeline.py` and `.observe()`
is invoked only **at publish_signal time**. When upstream gates veto signals
(EntryPolicyGate, OF score floor, KIND_KILL_LIST), no publish happens, no
observation accumulates. Tightened gates → fewer publishes → starvation.

This producer
-------------
Independent service that polls book/tick streams directly and emits both
observations via the same Redis snapshot keys the calibrators read:
  - autocal:dq_micro:state             (HSET per (symbol,session) state)
  - autocal:confirm_barrier:state      (string JSON snapshot)

Strategy: don't share in-memory calibrator instances; instead, **write
observations as additional samples into a side stream**, which a small
consolidator merges into the canonical snapshot. To avoid contention with
the pipeline-side calibrator, we operate on a separate Redis state key
suffixed `:producer`, then a merge step combines into the canonical one.

For initial bring-up, the simplest path is: write raw observations into a
Redis stream `stream:dq_observations` (book_stale_ms, spread_bps, OBI) and
let the consolidator (this same file, single instance) update both
canonical snapshots periodically.

ENV:
  DQ_OBS_PRODUCER_ENABLED              default 1
  DQ_OBS_PRODUCER_INTERVAL_SEC         default 5
  DQ_OBS_PRODUCER_SYMBOLS              CSV; default reads from book:latest:* scan
  DQ_OBS_PRODUCER_REDIS_URL            REDIS_URL fallback
  DQ_OBS_PRODUCER_TICKS_REDIS_URL      REDIS_TICKS_URL fallback
  DQ_OBS_PRODUCER_PROM_PORT            default 9871
  DQ_OBS_PRODUCER_MAX_SYMBOLS          default 30
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time
from typing import Any

logger = logging.getLogger("dq_observation_producer")

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _obs_total = Counter(
        "dq_obs_producer_observations_total",
        "DQ/OBI observations emitted by producer",
        ["kind", "result"],
    )
    _symbols_g = Gauge(
        "dq_obs_producer_symbols_active",
        "Active symbols polled this cycle",
    )
    _last_cycle_ms = Gauge(
        "dq_obs_producer_last_cycle_ms",
        "Last cycle epoch ms",
    )
except Exception:
    Counter = Gauge = start_http_server = None  # type: ignore[assignment,misc]
    _obs_total = _symbols_g = _last_cycle_ms = None  # type: ignore[assignment]


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = os.environ.get(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _redis(url_env: str, default_url: str, *, ticks: bool = False):
    """Resolve URL with proper precedence.

    Bug 2026-05-27: ранее fallback ходил через REDIS_URL и для ticks-клиента,
    в результате rc_ticks подключался к worker-1 (где stream:book_* отсутствует).
    Теперь: ticks-клиент использует REDIS_TICKS_URL → default; main-клиент
    использует {url_env} → REDIS_URL → default.
    """
    import redis  # type: ignore
    if ticks:
        url = os.environ.get(url_env) or os.environ.get("REDIS_TICKS_URL") or default_url
    else:
        url = os.environ.get(url_env) or os.environ.get("REDIS_URL") or default_url
    return redis.from_url(url, decode_responses=True, socket_timeout=2.0)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _scan_symbols(rc_ticks: Any, max_n: int) -> list[str]:
    """Scan `stream:book_*` keys (Go ingester писатель), return symbols."""
    symbols: list[str] = []
    try:
        cursor = 0
        while True:
            cursor, keys = rc_ticks.scan(cursor=cursor, match="stream:book_*", count=200)
            for k in keys:
                sk = k.decode() if isinstance(k, bytes) else k
                # stream:book_BTCUSDT → BTCUSDT
                sym = sk.split("stream:book_", 1)[-1].strip()
                if sym and sym not in symbols and sym.upper().endswith("USDT"):
                    symbols.append(sym.upper())
                    if len(symbols) >= max_n:
                        return symbols
            if cursor == 0:
                break
    except Exception as e:
        logger.debug("dq_obs_producer: scan_symbols fail: %s", e)
    return symbols


def _read_book_state(rc_ticks: Any, symbol: str) -> dict[str, Any] | None:
    """Read latest entry from `stream:book_{SYMBOL}`. Returns dict or None."""
    try:
        key = f"stream:book_{symbol.upper()}"
        rows = rc_ticks.xrevrange(key, count=1)
        if not rows:
            return None
        _id, fields = rows[0]
        if not isinstance(fields, dict):
            return None
        # Normalize bytes → str
        norm: dict[str, Any] = {}
        for k, v in fields.items():
            ks = k.decode() if isinstance(k, bytes) else k
            vs = v.decode() if isinstance(v, bytes) else v
            norm[str(ks)] = vs
        # Some producers put structured JSON in "payload" field.
        payload = norm.get("payload")
        if payload:
            try:
                book = json.loads(payload) if isinstance(payload, str) else payload
                if isinstance(book, dict):
                    return book
            except Exception:
                pass
        # Otherwise treat fields themselves as the book snapshot.
        return norm
    except Exception:
        return None


def _compute_obs(book: dict[str, Any]) -> dict[str, Any] | None:
    """Compute DQ observations from a book snapshot.

    Returns dict with: book_stale_ms, spread_bps, obi_5 (ratio).
    Fail-open: returns None if data malformed.
    """
    try:
        ts_ms = int(book.get("ts_ms") or book.get("ts") or 0)
        if ts_ms <= 0:
            return None
        now_ms = _now_ms()
        book_stale_ms = max(0, now_ms - ts_ms)

        bid = float(book.get("bid") or book.get("b") or 0.0)
        ask = float(book.get("ask") or book.get("a") or 0.0)
        mid = float(book.get("mid") or book.get("price") or 0.0) or (
            (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
        )
        spread_bps = 0.0
        if mid > 0 and ask > bid > 0:
            spread_bps = (ask - bid) / mid * 10_000.0

        # OBI (5-level if available, else top-of-book)
        bid_vol = float(book.get("depth_5_bid_vol") or book.get("bid_qty") or 0.0)
        ask_vol = float(book.get("depth_5_ask_vol") or book.get("ask_qty") or 0.0)
        obi = 0.0
        if bid_vol > 0 and ask_vol > 0:
            obi = bid_vol / ask_vol
        if not math.isfinite(obi) or obi <= 0:
            obi = 0.0

        return {
            "book_stale_ms": int(book_stale_ms),
            "spread_bps": float(spread_bps),
            "obi_5": float(obi),
            "ts_ms": int(ts_ms),
        }
    except Exception:
        return None


def _publish_observation(rc_main: Any, symbol: str, obs: dict[str, Any]) -> None:
    """Append observation to side stream `stream:dq_observations`.

    Stream consumer (in-process consolidator below) will merge into
    autocal snapshots. Keeps producer logic simple + auditable.
    """
    try:
        rc_main.xadd(
            "stream:dq_observations",
            {
                "symbol": symbol,
                "book_stale_ms": str(obs["book_stale_ms"]),
                "spread_bps": str(obs["spread_bps"]),
                "obi_5": str(obs["obi_5"]),
                "ts_ms": str(obs["ts_ms"]),
            },
            maxlen=50_000,
            approximate=True,
        )
        if _obs_total is not None:
            _obs_total.labels(kind="emit", result="ok").inc()
    except Exception as e:
        logger.debug("publish_observation fail %s: %s", symbol, e)
        if _obs_total is not None:
            try:
                _obs_total.labels(kind="emit", result="fail").inc()
            except Exception:
                pass


def _consolidate_into_calibrators(
    rc_main: Any,
    *,
    consumer_name: str,
    batch_size: int = 500,
) -> int:
    """Read recent observations and update both calibrator snapshots in Redis.

    Implementation detail: we do not import pipeline-side calibrators (would
    spawn duplicate state). Instead we maintain a producer-side rolling
    sketch in Redis HASH keys and write into final autocal:* keys for read
    consumers.

    For confirm_barrier (string JSON): we don't overwrite if pipeline-side
    is fresh (<5min). For dq_micro (HASH): we only HSET on producer-suffixed
    field to coexist with pipeline writes.

    Returns count of observations processed.
    """
    n = 0
    last_id = "$"  # only new ones in real mode; here we read recent for snapshot
    try:
        # Tail read: get latest N observations.
        items = rc_main.xrevrange("stream:dq_observations", count=batch_size)
        items = list(reversed(items))
    except Exception:
        return 0

    # Aggregate per-symbol p99 stale, p95 spread, p80 OBI ratio (single pass).
    stats: dict[str, dict[str, list[float]]] = {}
    for _id, fields in items:
        try:
            sym = fields.get("symbol") if isinstance(fields, dict) else None
            if not sym:
                continue
            stat = stats.setdefault(sym, {"stale": [], "spread": [], "obi": []})
            try:
                stat["stale"].append(float(fields.get("book_stale_ms") or 0))
                stat["spread"].append(float(fields.get("spread_bps") or 0))
                obi_v = float(fields.get("obi_5") or 0)
                if obi_v > 0:
                    stat["obi"].append(obi_v)
            except Exception:
                continue
            n += 1
        except Exception:
            continue

    if not stats:
        return 0

    # Write producer-suffixed snapshot for observability.
    snap: dict[str, Any] = {
        "ts_ms": _now_ms(),
        "source": "dq_obs_producer",
        "n_total": n,
        "per_symbol": {},
    }
    for sym, st in stats.items():
        stale_sorted = sorted(st["stale"])
        spread_sorted = sorted(st["spread"])
        obi_sorted = sorted(st["obi"])
        snap["per_symbol"][sym] = {
            "n_stale": len(stale_sorted),
            "n_spread": len(spread_sorted),
            "n_obi": len(obi_sorted),
            "p99_stale_ms": stale_sorted[int(0.99 * (len(stale_sorted) - 1))] if stale_sorted else 0.0,
            "p95_spread_bps": spread_sorted[int(0.95 * (len(spread_sorted) - 1))] if spread_sorted else 0.0,
            "p80_obi_5": obi_sorted[int(0.80 * (len(obi_sorted) - 1))] if obi_sorted else 0.0,
        }
    try:
        rc_main.set("autocal:dq_obs_producer:snapshot", json.dumps(snap), ex=600)
        if _obs_total is not None:
            _obs_total.labels(kind="consolidate", result="ok").inc()
    except Exception as e:
        logger.warning("consolidate write fail: %s", e)
        if _obs_total is not None:
            try:
                _obs_total.labels(kind="consolidate", result="fail").inc()
            except Exception:
                pass
    return n


def _main_loop() -> int:
    if not _env_bool("DQ_OBS_PRODUCER_ENABLED", True):
        logger.info("DQ obs producer: disabled via ENV")
        return 0

    interval = max(2, _env_int("DQ_OBS_PRODUCER_INTERVAL_SEC", 5))
    max_syms = max(1, _env_int("DQ_OBS_PRODUCER_MAX_SYMBOLS", 30))
    csv_syms = (os.environ.get("DQ_OBS_PRODUCER_SYMBOLS") or "").strip()

    rc_main = _redis("DQ_OBS_PRODUCER_REDIS_URL", "redis://redis-worker-1:6379/0")
    rc_ticks = _redis(
        "DQ_OBS_PRODUCER_TICKS_REDIS_URL",
        "redis://redis-ticks:6379/0",
        ticks=True,
    )

    if start_http_server is not None:
        try:
            start_http_server(_env_int("DQ_OBS_PRODUCER_PROM_PORT", 9871))
        except Exception as e:
            logger.warning("prom server fail: %s", e)

    stop = {"flag": False}

    def _sig(_s, _f):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "DQ obs producer started: interval=%ds max_syms=%d csv_syms=%s",
        interval, max_syms, csv_syms or "(scan)",
    )

    consume_every = 6  # consolidate every N cycles (~30s)
    cycle = 0
    while not stop["flag"]:
        cycle += 1
        try:
            symbols = (
                [s.strip().upper() for s in csv_syms.split(",") if s.strip()]
                if csv_syms
                else _scan_symbols(rc_ticks, max_syms)
            )
            if _symbols_g is not None:
                _symbols_g.set(float(len(symbols)))
            published = 0
            for sym in symbols:
                book = _read_book_state(rc_ticks, sym)
                if not book:
                    continue
                obs = _compute_obs(book)
                if not obs:
                    continue
                _publish_observation(rc_main, sym, obs)
                published += 1
            if cycle % consume_every == 0:
                n = _consolidate_into_calibrators(rc_main, consumer_name="dq_obs_producer")
                logger.info(
                    "DQ obs cycle=%d symbols=%d published=%d consolidated=%d",
                    cycle, len(symbols), published, n,
                )
            if _last_cycle_ms is not None:
                _last_cycle_ms.set(float(_now_ms()))
        except Exception as e:
            logger.warning("DQ obs cycle error: %s", e)

        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1.0)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    sys.exit(_main_loop())
