"""LONG-cooldown manager (P1.D, 2026-05-27).

Background
----------
After 3+ consecutive LONG losses на одном символе, gating должен временно
запретить новые LONG, пока цена не «успокоится». Это стандартный best-practice
risk-control (anti-revenge-trade); реализован в большинстве prop-firms.

What this writer does
---------------------
1. Subscribes to `stream:trade_close_events` (XADD-stream from trade_monitor)
   — or polls `trades:closed` updates via Postgres tail (configurable).
2. For each `direction=LONG, r_multiple≤0` event:
     - HINCRBY `risk:cooldown:long:{SYM}` count 1
     - HSET ... last_loss_ms = ts_ms
     - if streak ≥ threshold → HSET active=1, expires_at_ms=now+TTL
3. For each WIN (r_multiple>0): RESET count=0, active=0.
4. Auto-expire by TTL — reader checks now vs expires_at_ms.

Source preference order:
   STREAM `stream:trade_close_events` (preferred — lowest latency)
   FALLBACK: poll Postgres `trades_closed` every COOLDOWN_LONG_POLL_SEC.

ENV:
  COOLDOWN_LONG_ENABLED          default 1
  COOLDOWN_LONG_AFTER_LOSSES     default 3
  COOLDOWN_LONG_TTL_SEC          default 1800 (30 min)
  COOLDOWN_LONG_REDIS_URL        fallback REDIS_URL
  COOLDOWN_LONG_PG_DSN           fallback ANALYTICS_DB_DSN
  COOLDOWN_LONG_SOURCE           stream|pg  default stream
  COOLDOWN_LONG_POLL_SEC         default 10 (pg path)
  COOLDOWN_LONG_PROM_PORT        default 9874
  COOLDOWN_LONG_STREAM           default stream:trade_close_events
  COOLDOWN_LONG_GROUP            default long-cooldown-mgr
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any

logger = logging.getLogger("long_cooldown_manager")

_KEY_PREFIX = "risk:cooldown:long:"

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _events_total = Counter(
        "long_cooldown_events_total",
        "LONG cooldown updates",
        ["result"],
    )
    _active_g = Gauge(
        "long_cooldown_active_symbols",
        "Number of symbols currently in LONG cooldown",
    )
except Exception:
    Counter = Gauge = start_http_server = None  # type: ignore[assignment,misc]
    _events_total = _active_g = None  # type: ignore[assignment]


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


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis():
    import redis  # type: ignore
    url = (
        os.environ.get("COOLDOWN_LONG_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )
    return redis.from_url(url, decode_responses=True, socket_timeout=2.0)


def _update_on_loss(rc: Any, symbol: str, ts_ms: int, threshold: int, ttl_sec: int) -> None:
    """Increment streak, arm cooldown if threshold reached."""
    key = _KEY_PREFIX + (symbol or "").upper()
    try:
        new_count = rc.hincrby(key, "count", 1)
        pipe = rc.pipeline()
        pipe.hset(key, mapping={
            "last_loss_ms": str(ts_ms),
            "last_event": "loss",
        })
        if int(new_count) >= int(threshold):
            expires = _now_ms() + int(ttl_sec) * 1000
            pipe.hset(key, mapping={
                "active": "1",
                "expires_at_ms": str(expires),
                "armed_count": str(new_count),
            })
            pipe.expire(key, ttl_sec * 2)  # cleanup after TTL doubled
        else:
            pipe.hset(key, "active", "0")
            pipe.expire(key, ttl_sec * 2)
        pipe.execute()
        if _events_total is not None:
            _events_total.labels(result="loss_recorded").inc()
        logger.info(
            "long_cooldown: LOSS symbol=%s streak=%s armed=%s",
            symbol, new_count, "yes" if int(new_count) >= int(threshold) else "no",
        )
    except Exception as e:
        logger.warning("long_cooldown: update_on_loss fail %s: %s", symbol, e)
        if _events_total is not None:
            try:
                _events_total.labels(result="error").inc()
            except Exception:
                pass


def _update_on_win(rc: Any, symbol: str, ts_ms: int) -> None:
    """Reset streak on a win."""
    key = _KEY_PREFIX + (symbol or "").upper()
    try:
        rc.hset(key, mapping={
            "count": "0",
            "active": "0",
            "last_win_ms": str(ts_ms),
            "last_event": "win",
        })
        if _events_total is not None:
            _events_total.labels(result="win_reset").inc()
        logger.debug("long_cooldown: WIN reset symbol=%s", symbol)
    except Exception as e:
        logger.warning("long_cooldown: update_on_win fail %s: %s", symbol, e)


def _count_active(rc: Any) -> int:
    """Best-effort scan for active=1 keys (capped)."""
    n = 0
    try:
        cursor = 0
        scanned = 0
        while True:
            cursor, keys = rc.scan(cursor=cursor, match=_KEY_PREFIX + "*", count=200)
            scanned += len(keys)
            if keys:
                pipe = rc.pipeline()
                for k in keys:
                    pipe.hget(k, "active")
                actives = pipe.execute()
                for a in actives:
                    if (a or "0").strip() in ("1", "true", "True"):
                        n += 1
            if cursor == 0 or scanned > 1000:
                break
    except Exception:
        pass
    return n


def _process_event(rc: Any, ev: dict[str, Any], threshold: int, ttl_sec: int) -> None:
    """Decode a single trade-close event and update Redis."""
    try:
        direction = str(ev.get("direction") or "").strip().upper()
        if direction not in ("LONG", "BUY"):
            return
        symbol = (str(ev.get("symbol") or "")).strip().upper()
        if not symbol:
            return
        r_mult_raw = ev.get("r_multiple")
        try:
            r_mult = float(r_mult_raw) if r_mult_raw is not None else None
        except Exception:
            r_mult = None
        if r_mult is None:
            return
        ts_ms = int(ev.get("ts_ms") or ev.get("exit_ts_ms") or _now_ms())
        if r_mult <= 0.0:
            _update_on_loss(rc, symbol, ts_ms, threshold, ttl_sec)
        else:
            _update_on_win(rc, symbol, ts_ms)
    except Exception as e:
        logger.debug("long_cooldown: process_event fail: %s", e)


# ───────────────────────── source 1: Redis stream tail ─────────────────────────
def _run_stream(rc: Any, threshold: int, ttl_sec: int) -> None:
    stream = os.environ.get("COOLDOWN_LONG_STREAM", "stream:trade_close_events")
    group = os.environ.get("COOLDOWN_LONG_GROUP", "long-cooldown-mgr")
    consumer = os.environ.get("HOSTNAME", "consumer-1")
    try:
        rc.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception:
        pass  # already exists

    logger.info("long_cooldown: tailing stream=%s group=%s", stream, group)
    while True:
        try:
            resp = rc.xreadgroup(
                group, consumer, {stream: ">"}, count=50, block=2000,
            )
            if not resp:
                continue
            for _stream_key, entries in resp:
                for msg_id, fields in entries:
                    try:
                        # Two shapes: flat fields or {"payload": json}
                        if isinstance(fields, dict) and "payload" in fields:
                            try:
                                ev = json.loads(fields["payload"])
                            except Exception:
                                ev = fields
                        else:
                            ev = fields
                        _process_event(rc, ev if isinstance(ev, dict) else {}, threshold, ttl_sec)
                    finally:
                        try:
                            rc.xack(stream, group, msg_id)
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("long_cooldown: stream loop error: %s", e)
            time.sleep(1.0)


# ───────────────────────── source 2: Postgres tail ─────────────────────────
def _run_pg(rc: Any, threshold: int, ttl_sec: int, poll_sec: int) -> None:
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception as e:
        logger.error("long_cooldown PG mode: psycopg2 not installed: %s", e)
        return

    dsn = os.environ.get("COOLDOWN_LONG_PG_DSN") or os.environ.get("ANALYTICS_DB_DSN")
    if not dsn:
        logger.error("long_cooldown PG mode: ANALYTICS_DB_DSN missing")
        return

    last_ts_state_key = "risk:cooldown:long:_last_seen_ts_ms"
    try:
        last_seen = int(rc.get(last_ts_state_key) or 0)
    except Exception:
        last_seen = 0
    # 2026-05-27 P1.D fix: ограничиваем initial backfill коротким окном,
    # чтобы исторические лоссы прошлых суток не возводили streak до сотен и не
    # триггерили armed=1 ретроспективно. Default 1h backfill через ENV.
    backfill_sec = max(60, _env_int("COOLDOWN_LONG_INITIAL_BACKFILL_SEC", 3600))
    if last_seen <= 0:
        last_seen = _now_ms() - backfill_sec * 1000

    logger.info(
        "long_cooldown PG mode: tailing trades_closed from ts_ms=%d (initial_backfill=%ds)",
        last_seen, backfill_sec,
    )

    while True:
        try:
            with psycopg2.connect(dsn) as conn:
                conn.autocommit = True
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT direction, symbol, r_multiple,
                               COALESCE(EXTRACT(EPOCH FROM exit_ts) * 1000, 0)::bigint AS ts_ms
                        FROM trades_closed
                        WHERE r_multiple IS NOT NULL
                          AND COALESCE(EXTRACT(EPOCH FROM exit_ts) * 1000, 0)::bigint > %s
                        ORDER BY exit_ts ASC
                        LIMIT 500
                        """,
                        (last_seen,),
                    )
                    rows = cur.fetchall()
            new_seen = last_seen
            for row in rows:
                ev = {
                    "direction": row["direction"],
                    "symbol": row["symbol"],
                    "r_multiple": row["r_multiple"],
                    "ts_ms": int(row["ts_ms"]),
                }
                _process_event(rc, ev, threshold, ttl_sec)
                new_seen = max(new_seen, int(row["ts_ms"]))
            if new_seen > last_seen:
                last_seen = new_seen
                try:
                    rc.set(last_ts_state_key, str(last_seen), ex=7 * 86400)
                except Exception:
                    pass
            if _active_g is not None:
                _active_g.set(float(_count_active(rc)))
        except Exception as e:
            logger.warning("long_cooldown PG cycle error: %s", e)

        time.sleep(poll_sec)


def _main_loop() -> int:
    if not _env_bool("COOLDOWN_LONG_ENABLED", True):
        logger.info("long_cooldown: disabled (COOLDOWN_LONG_ENABLED=0)")
        return 0

    threshold = max(1, _env_int("COOLDOWN_LONG_AFTER_LOSSES", 3))
    ttl_sec = max(60, _env_int("COOLDOWN_LONG_TTL_SEC", 1800))
    source = (os.environ.get("COOLDOWN_LONG_SOURCE") or "pg").strip().lower()
    poll_sec = max(2, _env_int("COOLDOWN_LONG_POLL_SEC", 10))

    if start_http_server is not None:
        try:
            start_http_server(_env_int("COOLDOWN_LONG_PROM_PORT", 9874))
        except Exception as e:
            logger.warning("prom server fail: %s", e)

    rc = _redis()

    stop = {"flag": False}

    def _sig(_s, _f):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "long_cooldown started: source=%s threshold=%d ttl_sec=%d poll_sec=%d",
        source, threshold, ttl_sec, poll_sec,
    )

    if source == "stream":
        _run_stream(rc, threshold, ttl_sec)
    else:
        _run_pg(rc, threshold, ttl_sec, poll_sec)
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    sys.exit(_main_loop())
