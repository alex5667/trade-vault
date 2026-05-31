"""
signal_outcome_resolver.py — Phase 0 triple-barrier label resolver.

Reads open signal_outcome records (label IS NULL) from TimescaleDB, replays
ticks from stream:tick_{SYMBOL} on Redis, and fills in the triple-barrier label.

Design:
  * Idempotent: UPDATE only when label IS NULL → safe to restart/replay.
  * Conservative: when TP and SL both appear in the same tick → SL wins.
  * Runs outside the hot path — poll interval configurable (default 60 s).
  * Uses existing core.triple_barrier.label_path() for barrier logic.
  * quality_flags bit 0 set when tick data was incomplete for full TTL window.
  * SHADOW mode by default (SIGNAL_OUTCOME_ENABLED=0 → skips DB writes).

ENV:
  SIGNAL_OUTCOME_ENABLED       = 0              master switch
  SO_RESOLVER_REDIS_URL        = (redis-ticks URL)
  SO_RESOLVER_DB_DSN           = (from TRADES_DB_DSN)
  SO_RESOLVER_PORT             = 9911
  SO_RESOLVER_POLL_SEC         = 60
  SO_RESOLVER_BATCH_SIZE       = 100            open records per poll
  SO_RESOLVER_MIN_AGE_MS       = 5000           don't resolve records < 5s old
  SO_RESOLVER_TICK_MAXLEN      = 50000          max ticks read per symbol per resolve
  SO_RESOLVER_TICK_REDIS_URL   = redis://redis-ticks:6379/0

Prometheus metrics (port SO_RESOLVER_PORT):
  so_resolver_resolved_total{label}
  so_resolver_skipped_total{reason}
  so_resolver_label_lag_ms     (histogram p50/p95/p99)
  so_resolver_open_gauge        (current open count)
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Iterator

log = logging.getLogger("so_resolver")


# ─── ENV helpers ─────────────────────────────────────────────────────────────

def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d

def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# ─── Tick stream reading ──────────────────────────────────────────────────────

def _stream_id_ms(raw_id: Any) -> int:
    """Extract ms timestamp from Redis stream ID '1234567890123-0'."""
    try:
        s = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        return int(s.split("-")[0])
    except Exception:
        return 0


def _tick_price(fields: dict[str, str]) -> float:
    """Extract trade price from tick stream fields.

    Go worker publishes: p (short), price (long), fallback bid average.
    For triple-barrier labeling we use last-trade-price (not mid).
    Long  exits at bid (conservative for longs).
    Short exits at ask (conservative for shorts).
    We use the trade price as proxy — bid/ask available in book stream
    but tick stream carries trade price.
    """
    px = _safe_float(fields.get("p") or fields.get("price") or fields.get("px"), 0.0)
    if px > 0:
        return px
    # fallback: derive from bid/ask if tick stream contains them
    bid = _safe_float(fields.get("bid"), 0.0)
    ask = _safe_float(fields.get("ask"), 0.0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return 0.0


def fetch_ticks(
    rc: Any,
    symbol: str,
    start_ms: int,
    end_ms: int,
    maxlen: int = 50_000,
) -> list[tuple[int, float]]:
    """Return sorted (ts_ms, price) pairs from stream:tick_{symbol}[start_ms, end_ms].

    Paginates automatically for large windows.
    """
    stream_key = f"stream:tick_{symbol}"
    results: list[tuple[int, float]] = []
    cursor = f"{start_ms}-0"
    end_id = f"{end_ms}-9999999"

    while True:
        try:
            entries = rc.xrange(stream_key, min=cursor, max=end_id, count=min(maxlen, 5000))
        except Exception as e:
            log.debug("xrange error %s: %s", stream_key, e)
            break

        if not entries:
            break

        for raw_id, fields in entries:
            ts = _stream_id_ms(raw_id)
            if ts < start_ms or ts > end_ms:
                continue
            px = _tick_price(
                {k.decode() if isinstance(k, bytes) else k:
                 v.decode() if isinstance(v, bytes) else v
                 for k, v in fields.items()}
            )
            if px > 0:
                results.append((ts, px))

        if len(entries) < 5000:
            break

        # Advance cursor past last seen ID
        last_id = entries[-1][0]
        last_ts = _stream_id_ms(last_id)
        cursor = f"{last_ts + 1}-0"

        if len(results) >= maxlen:
            break

    return results


# ─── Barrier resolution ───────────────────────────────────────────────────────

def resolve_record(record: dict, ticks: list[tuple[int, float]]) -> dict | None:
    """
    Run triple-barrier labeling on a single open signal_outcome record.

    Returns update dict {label, realized_r, realized_bps, mfe_r, mae_r,
    resolved_time_ms, quality_flags, entry_px_fallback_reason} or None if
    record is unrecoverable (no entry_px AND no ticks).

    Converts R-based barriers to bps for label_path(), converts back.
    Conservative: SL wins on same-tick TP+SL conflict (built into label_path).

    entry_px contract (Plan 3 / Step 1):
      * prefer explicit record["entry_px"] (computed at decision time);
      * fallback to first tick price ONLY with explicit reason flag;
      * fallback "no_path" → return None and let caller skip.
    """
    try:
        from core.triple_barrier import BarrierSpec, label_path, pick_entry_price_v2

        raw_entry_px = record.get("entry_px")
        reason_flags: list[str] = []
        entry_px, fallback_reason = pick_entry_price_v2(
            entry_px_expected=raw_entry_px,
            path=ticks,
            reason_flags=reason_flags,
        )

        if entry_px <= 0:
            # both explicit and fallback failed → unrecoverable
            return dict(_unrecoverable=True, entry_px_fallback_reason=fallback_reason or "no_path")

        r_unit_px = float(record["r_unit_px"])
        tp_r      = float(record["tp_r"])
        sl_r      = float(record["sl_r"])
        ttl_ms    = int(record["ttl_ms"])
        side      = int(record["side"])
        decision_ms = int(record["decision_time_ms"])
        qf_base   = int(record.get("quality_flags") or 0)

        if r_unit_px <= 0:
            return None

        direction = "LONG" if side > 0 else "SHORT"

        # Convert R-unit to bps: 1R = r_unit_px / entry_px * 10_000 bps
        sl_bps = r_unit_px / entry_px * 10_000.0
        tp_bps = tp_r * sl_bps  # tp_r × 1R in bps

        spec = BarrierSpec(h_ms=ttl_ms, tp_bps=tp_bps, sl_bps=sl_bps, cost_bps=0.0)

        result = label_path(
            ts0_ms=decision_ms,
            direction=direction,
            entry_px=entry_px,
            path=ticks,
            spec=spec,
        )

        from core.triple_barrier import BarrierOutcome
        if result.outcome == BarrierOutcome.TP_HIT:
            label = 1
        elif result.outcome == BarrierOutcome.SL_HIT:
            label = -1
        else:
            # TIMEOUT or NO_TICKS
            label = 0

        realized_r    = result.realized_close_bps / sl_bps if sl_bps > 0 else 0.0
        realized_bps  = result.realized_close_bps
        mfe_r         = result.mfe_bps / sl_bps if sl_bps > 0 else 0.0
        mae_r         = result.mae_bps / sl_bps if sl_bps > 0 else 0.0

        # quality_flags bit 0: tick data incomplete (fewer ticks than expected or NO_TICKS)
        qf = qf_base
        if result.outcome == BarrierOutcome.NO_TICKS or len(ticks) < 2:
            qf |= 1

        fill_px = entry_px + result.realized_close_bps * entry_px / 10000.0 * (1.0 if direction == "LONG" else -1.0)

        return dict(
            label=label,
            realized_r=realized_r,
            realized_bps=realized_bps,
            mfe_r=mfe_r,
            mae_r=mae_r,
            resolved_time_ms=result.hit_ms,
            quality_flags=qf,
            fill_px=fill_px,
            exec_slippage_bps=0.0,
            entry_px_fallback_reason=fallback_reason or "",
        )

    except Exception as e:
        log.debug("resolve_record error: %s", e)
        return None


# ─── DB helpers ──────────────────────────────────────────────────────────────

_FETCH_OPEN_SQL = """
    SELECT
        sid, decision_time_ms, symbol, side,
        entry_px, r_unit_px, tp_r, sl_r, ttl_ms, quality_flags
    FROM signal_outcome
    WHERE label IS NULL
      AND decision_time_ms < %s
    ORDER BY decision_time_ms ASC
    LIMIT %s
"""

_UPDATE_SQL = """
    UPDATE signal_outcome SET
        label            = %s,
        realized_r       = %s,
        realized_bps     = %s,
        mfe_r            = %s,
        mae_r            = %s,
        resolved_time_ms = %s,
        quality_flags    = %s,
        fill_px          = %s,
        exec_slippage_bps = %s
    WHERE sid = %s
      AND decision_time_ms = %s
      AND label IS NULL
"""


def fetch_open_records(conn: Any, cutoff_ms: int, batch: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FETCH_OPEN_SQL, (cutoff_ms, batch))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def apply_updates(conn: Any, updates: list[tuple]) -> int:
    """Batch UPDATE resolved records. Returns count attempted."""
    if not updates:
        return 0
    with conn.cursor() as cur:
        from psycopg2.extras import execute_batch
        execute_batch(cur, _UPDATE_SQL, updates, page_size=100)
    conn.commit()
    return len(updates)


# ─── Main service ─────────────────────────────────────────────────────────────

def main() -> None:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    import redis  # type: ignore

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    enabled      = _env_bool("SIGNAL_OUTCOME_ENABLED", False)
    db_dsn       = _env("SO_RESOLVER_DB_DSN", _env("TRADES_DB_DSN", ""))
    tick_url     = _env("SO_RESOLVER_TICK_REDIS_URL", _env("REDIS_TICKS_URL", "redis://redis-ticks:6379/0"))
    port         = _env_int("SO_RESOLVER_PORT", 9911)
    poll_sec     = _env_int("SO_RESOLVER_POLL_SEC", 60)
    batch_size   = _env_int("SO_RESOLVER_BATCH_SIZE", 100)
    min_age_ms   = _env_int("SO_RESOLVER_MIN_AGE_MS", 5_000)
    tick_maxlen  = _env_int("SO_RESOLVER_TICK_MAXLEN", 50_000)

    log.info(
        "so_resolver starting | enabled=%s port=%d poll=%ds batch=%d",
        enabled, port, poll_sec, batch_size,
    )

    rc_ticks = redis.from_url(tick_url, decode_responses=False)  # raw bytes for ts parsing

    start_http_server(port)
    c_resolved = Counter("so_resolver_resolved_total",   "Records resolved",    ["label"])
    c_skipped  = Counter("so_resolver_skipped_total",    "Records skipped",     ["reason"])
    c_err      = Counter("so_resolver_error_total",      "Resolver errors",     [])
    g_open     = Gauge("so_resolver_open_gauge",         "Open records count",  [])
    c_entry_fb = Counter(
        "tb_entry_px_fallback_total",
        "Triple-barrier resolver entry_px fallbacks (explicit→tick or unrecoverable)",
        ["symbol", "reason"],
    )
    h_lag      = Histogram(
        "so_resolver_label_lag_ms",
        "Lag from decision_time to resolution (ms)",
        buckets=[60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000],
    )

    conn = None

    def _get_conn():
        nonlocal conn
        if conn is None or conn.closed:
            import psycopg2
            conn = psycopg2.connect(db_dsn)
        return conn

    while True:
        try:
            time.sleep(poll_sec)

            if not db_dsn:
                log.debug("SO_RESOLVER_DB_DSN not set; skipping")
                continue

            now_ms   = int(time.time() * 1000)
            cutoff   = now_ms - min_age_ms

            try:
                db_conn = _get_conn()
                open_records = fetch_open_records(db_conn, cutoff, batch_size)
            except Exception as e:
                c_err.inc()
                log.warning("so_resolver fetch_open error: %s", e)
                conn = None
                continue

            g_open.set(len(open_records))
            if not open_records:
                continue

            updates: list[tuple] = []

            for rec in open_records:
                symbol       = rec["symbol"]
                decision_ms  = int(rec["decision_time_ms"])
                ttl_ms       = int(rec["ttl_ms"])
                end_ms       = decision_ms + ttl_ms + 5_000  # small buffer past barrier

                try:
                    ticks = fetch_ticks(rc_ticks, symbol, decision_ms, end_ms, maxlen=tick_maxlen)
                except Exception as e:
                    log.debug("fetch_ticks error %s: %s", symbol, e)
                    c_skipped.labels(reason="tick_fetch_error").inc()
                    continue

                if not ticks:
                    # Ticks not yet available or stream empty — will retry next poll
                    c_skipped.labels(reason="no_ticks_yet").inc()
                    continue

                resolved = resolve_record(rec, ticks)
                if resolved is None:
                    c_skipped.labels(reason="resolve_failed").inc()
                    continue

                if resolved.get("_unrecoverable"):
                    reason = str(resolved.get("entry_px_fallback_reason") or "no_path")
                    c_entry_fb.labels(symbol=symbol, reason=reason).inc()
                    c_skipped.labels(reason="entry_px_unrecoverable").inc()
                    continue

                fb_reason = str(resolved.get("entry_px_fallback_reason") or "")
                if fb_reason:
                    c_entry_fb.labels(symbol=symbol, reason=fb_reason).inc()

                label_str = {1: "tp", -1: "sl", 0: "timeout"}.get(resolved["label"], "unknown")
                c_resolved.labels(label=label_str).inc()
                h_lag.observe(now_ms - decision_ms)

                updates.append((
                    resolved["label"],
                    resolved["realized_r"],
                    resolved["realized_bps"],
                    resolved["mfe_r"],
                    resolved["mae_r"],
                    resolved["resolved_time_ms"],
                    resolved["quality_flags"],
                    resolved["fill_px"],
                    resolved["exec_slippage_bps"],
                    rec["sid"],
                    decision_ms,
                ))

            if updates and enabled:
                try:
                    db_conn = _get_conn()
                    n = apply_updates(db_conn, updates)
                    log.info("so_resolver: resolved %d records", n)
                except Exception as e:
                    c_err.inc()
                    log.warning("so_resolver DB update error (fail-open): %s", e)
                    try:
                        if conn and not conn.closed:
                            conn.rollback()
                    except Exception:
                        pass
                    conn = None
            elif updates:
                log.debug("so_resolver SHADOW: would resolve %d records (SIGNAL_OUTCOME_ENABLED=0)", len(updates))

        except Exception as e:
            c_err.inc()
            log.warning("so_resolver poll error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
