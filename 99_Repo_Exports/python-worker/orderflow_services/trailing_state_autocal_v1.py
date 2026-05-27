#!/usr/bin/env python3
"""trailing_state_autocal_v1.py

Auto-calibrator for the TrailingStateWorker (Phase B).

What it does
------------
1. Every ``poll_sec`` (default 60 s) reads the ``events:trailing:state`` audit stream
   written by ``TrailingStateWorker``.
2. Accumulates per-symbol statistics over a rolling ``window_ms`` window:
   - ``n_sl_moves``       — SL actually moved (would_move events)
   - ``n_shadow_moves``   — moves computed in shadow mode (no real command sent)
   - ``n_errors``         — error / DLQ events
   - ``n_positions``      — unique positions observed
   - ``avg_delta_bps``    — mean |new_sl - old_sl| / price × 10_000 (robust median)
   - ``duplicate_rate``   — fraction of duplicate-command attempts blocked by SETNX
3. When ``min_samples`` observations are reached for all active symbols AND
   ``promote_dwell_min`` minutes have elapsed, runs sanity checks:
   - error_rate < ``promote_max_error_rate`` (default 0.05)
   - avg_delta_bps > ``promote_min_delta_bps`` (default 2.0) — moves are non-trivial
   - duplicate_rate < ``promote_max_dup_rate`` (default 0.02)
4. On pass: publishes snapshot with ``shadow=false`` to ``autocal:trailing_state:state``.
   ``TrailingStateWorker`` reads this key within its next refresh cycle (≤60 s) and
   switches to live mode — no container restart required.
5. Sends a detailed Telegram notification (``notify:telegram``).

Rollback (no restart needed)
-----------------------------
  docker exec redis-worker-1 redis-cli SET autocal:trailing_state:state '{"shadow":true}'

Or set ``TS_CAL_ENFORCE=0`` + restart this container → publishes shadow=true snapshot.

ENV
---
  TS_CAL_REDIS_URL            Feed Redis URL (default REDIS_URL or redis://redis-worker-1:6379/0)
  TS_CAL_TICKS_REDIS_URL      Tick Redis URL for reading events:trailing:state (same default)
  TS_CAL_PORT                 Prometheus port (default 9922)
  TS_CAL_POLL_SEC             Polling interval (default 60)
  TS_CAL_SNAPSHOT_TTL         Redis TTL for autocal key (default 7200)
  TS_CAL_WINDOW_MS            Rolling window for stats (default 43200000 = 12 h)
  TS_CAL_MIN_SAMPLES          Min observations before calibration (default 50)
  TS_CAL_ENFORCE              0|1 — start in enforce mode (default 0)
  TS_CAL_NOTIFY_STREAM        Redis stream for Telegram (default notify:telegram)

Auto-promote
  TS_CAL_AUTO_PROMOTE         0|1 — enable auto-promotion to shadow=false (default 0)
  TS_CAL_PROMOTE_DWELL_MIN    Minutes all symbols ready before promote (default 30)
  TS_CAL_PROMOTE_MAX_ERROR    Sanity: max error rate (default 0.05 = 5%)
  TS_CAL_PROMOTE_MIN_DELTA_BPS  Sanity: min avg SL delta bps (default 2.0)
  TS_CAL_PROMOTE_MAX_DUP_RATE   Sanity: max duplicate-command rate (default 0.02)
  TS_CAL_SYMBOLS              Comma-separated symbols to track (default BTCUSDT,ETHUSDT)
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, REGISTRY, start_http_server  # type: ignore

from core.redis_keys import RS, RK
from core.redis_client import get_redis

logger = logging.getLogger("ts-autocal")

# ── ENV helpers ──────────────────────────────────────────────────────────────

def _e(name: str, default: str = "") -> str:
    return os.getenv(name, default) or default

def _ei(name: str, default: int) -> int:
    try:
        return int(_e(name)) if _e(name) else default
    except ValueError:
        return default

def _ef(name: str, default: float) -> float:
    try:
        return float(_e(name)) if _e(name) else default
    except ValueError:
        return default

def _eb(name: str, default: bool) -> bool:
    raw = _e(name)
    return (raw.strip().lower() in ("1", "true", "yes")) if raw else default


# ── Prometheus (idempotent) ──────────────────────────────────────────────────

def _g(name: str, doc: str, labels: list[str] | None = None) -> Gauge:
    try:
        return Gauge(name, doc, labels or [])
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise

def _c(name: str, doc: str, labels: list[str] | None = None) -> Counter:
    try:
        return Counter(name, doc, labels or [])
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise

def _h(name: str, doc: str, buckets: list[float], labels: list[str] | None = None) -> Histogram:
    try:
        return Histogram(name, doc, labels or [], buckets=buckets)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


_g_shadow              = _g("ts_autocal_shadow",            "1 if currently shadow mode, 0 if enforce")
_g_n_samples           = _g("ts_autocal_n_samples",         "Observations in rolling window", ["symbol"])
_g_n_sl_moves          = _g("ts_autocal_n_sl_moves",        "SL move events", ["symbol"])
_g_error_rate          = _g("ts_autocal_error_rate",        "Error rate (0-1)", ["symbol"])
_g_dup_rate            = _g("ts_autocal_dup_rate",          "Duplicate command rate (0-1)", ["symbol"])
_g_avg_delta_bps       = _g("ts_autocal_avg_delta_bps",     "Median |SL delta| bps", ["symbol"])
_g_n_with_delta        = _g("ts_autocal_n_with_delta",      "Events with non-zero delta_bps (old_sl+new_sl+price present)", ["symbol"])
_g_ready_age_sec       = _g("ts_autocal_ready_age_sec",     "Seconds since all symbols reached min_samples")
_g_enforce             = _g("ts_autocal_enforce",           "1 if auto-promote is configured")
_c_polls               = _c("ts_autocal_polls_total",       "Poll iterations")
_c_obs                 = _c("ts_autocal_obs_total",         "Observations ingested")
_c_promote             = _c("ts_autocal_promote_total",     "Auto-promote attempts", ["result"])
_c_errors              = _c("ts_autocal_errors_total",      "Processing errors", ["kind"])
_h_poll_ms             = _h("ts_autocal_poll_ms",           "Poll duration", [5, 10, 50, 100, 500])


# ── Stats accumulator ────────────────────────────────────────────────────────

@dataclass
class _Sample:
    ts_ms: int
    # "sl_move" | "shadow_move" | "error" | "duplicate" | "other"
    # NOTE: "other" is the explicit fallback for unknown/no-price-data events;
    # it does NOT count toward n_sl_moves() to avoid masking delta=0 situations.
    event_type: str
    symbol: str
    delta_bps: float = 0.0


@dataclass
class _SymbolStats:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=5000))

    @property
    def n(self) -> int:
        return len(self.buf)

    def n_of(self, etype: str) -> int:
        return sum(1 for s in self.buf if s.event_type == etype)

    def error_rate(self) -> float:
        n = self.n
        return self.n_of("error") / n if n > 0 else 0.0

    def dup_rate(self) -> float:
        n = self.n
        return self.n_of("duplicate") / n if n > 0 else 0.0

    def median_delta_bps(self) -> float:
        deltas = sorted(s.delta_bps for s in self.buf if s.delta_bps > 0)
        if not deltas:
            return 0.0
        mid = len(deltas) // 2
        if len(deltas) % 2 == 0:
            return (deltas[mid - 1] + deltas[mid]) / 2.0
        return deltas[mid]

    def n_sl_moves(self) -> int:
        """Events classified as genuine SL moves (with price data)."""
        return self.n_of("sl_move")

    def n_with_delta(self) -> int:
        """Events that carried non-zero delta_bps (i.e. had old_sl/new_sl/price)."""
        return sum(1 for s in self.buf if s.delta_bps > 0)

    def evict_old(self, cutoff_ms: int) -> None:
        while self.buf and self.buf[0].ts_ms < cutoff_ms:
            self.buf.popleft()


# ── Stream reader ─────────────────────────────────────────────────────────────

def _read_audit_stream(
    r: Any,
    cursor: str,
    bins: dict[str, _SymbolStats],
    window_ms: int,
) -> tuple[str, int]:
    """Read new events from events:trailing:state, ingest into bins.

    Returns (new_cursor, n_ingested).
    """
    stream_key = "events:trailing:state"
    n = 0
    try:
        results = r.xread({stream_key: cursor}, count=500)  # type: ignore[arg-type]
    except Exception as exc:
        logger.warning("xread %s failed: %s", stream_key, exc)
        return cursor, 0

    for _stream, messages in (results or []):  # type: ignore[union-attr]
        for msg_id, fields in messages:
            cursor = msg_id
            try:
                symbol = fields.get("symbol", "")
                if not symbol:
                    continue
                event_type = (fields.get("event_type") or fields.get("to_state") or "").lower()
                ts_ms_raw = fields.get("ts_ms") or fields.get("ts") or 0
                ts_ms = int(float(ts_ms_raw)) if ts_ms_raw else int(time.time() * 1000)

                # Map event_type → sample type
                # IMPORTANT: the "else" branch uses "other" (not "sl_move") so that
                # plain state-transition events without price fields are NOT counted
                # as SL moves and do NOT inflate n_sl_moves / mask delta=0.
                if "error" in event_type or "dlq" in event_type:
                    stype = "error"
                elif "duplicate" in event_type:
                    stype = "duplicate"
                elif "shadow" in event_type or "would_move" in event_type:
                    stype = "shadow_move"
                elif "sl_move" in event_type or "hwm_trail" in event_type or "trailing_active" in event_type:
                    stype = "sl_move"
                else:
                    stype = "other"  # state-transition / unrecognised — does NOT count as sl_move

                # Parse delta bps if present
                delta_bps = 0.0
                try:
                    old_sl = float(fields.get("old_sl") or 0)
                    new_sl = float(fields.get("new_sl") or 0)
                    price = float(fields.get("price") or fields.get("entry_price") or 0)
                    if old_sl > 0 and new_sl > 0 and price > 0:
                        delta_bps = abs(new_sl - old_sl) / price * 10_000
                except Exception:
                    pass

                if symbol not in bins:
                    bins[symbol] = _SymbolStats()
                bins[symbol].buf.append(_Sample(ts_ms=ts_ms, event_type=stype, symbol=symbol, delta_bps=delta_bps))
                n += 1
            except Exception as exc:
                logger.debug("ingest msg %s: %s", msg_id, exc)

    # Evict old samples
    cutoff = int(time.time() * 1000) - window_ms
    for stats in bins.values():
        stats.evict_old(cutoff)

    return cursor, n


# ── Sanity checks ────────────────────────────────────────────────────────────

def _sanity_ok(
    bins: dict[str, _SymbolStats],
    min_samples: int,
    max_error_rate: float,
    min_delta_bps: float,
    max_dup_rate: float,
) -> tuple[bool, str]:
    for sym, stats in bins.items():
        if stats.n < min_samples:
            return False, f"{sym}: only {stats.n} < {min_samples} samples"
        er = stats.error_rate()
        if er > max_error_rate:
            return False, f"{sym}: error_rate={er:.3f} > {max_error_rate}"
        dr = stats.dup_rate()
        if dr > max_dup_rate:
            return False, f"{sym}: dup_rate={dr:.3f} > {max_dup_rate}"
        nd = stats.n_with_delta()
        if nd == 0 and min_delta_bps > 0:
            # No SL_MOVE events with price data at all — classify separately
            # so the operator knows it's a data-availability issue, not a
            # threshold issue.  Promote is still blocked.
            return False, (
                f"{sym}: n_with_delta=0 — no SL_MOVE events carry "
                f"old_sl/new_sl/price (check TrailingStateWorker tick routing "
                f"or verify BTC positions reach TRAILING_ACTIVE with price ticks)"
            )
        md = stats.median_delta_bps()
        if md < min_delta_bps:
            return False, f"{sym}: median_delta_bps={md:.2f} < {min_delta_bps}"
    return True, "ok"


# ── Prometheus update ─────────────────────────────────────────────────────────

def _update_metrics(bins: dict[str, _SymbolStats], min_samples: int) -> int:
    """Update gauges, return count of calibrated symbols."""
    calibrated = 0
    for sym, stats in bins.items():
        _g_n_samples.labels(symbol=sym).set(stats.n)
        _g_n_sl_moves.labels(symbol=sym).set(stats.n_sl_moves())
        _g_n_with_delta.labels(symbol=sym).set(stats.n_with_delta())
        _g_error_rate.labels(symbol=sym).set(stats.error_rate())
        _g_dup_rate.labels(symbol=sym).set(stats.dup_rate())
        _g_avg_delta_bps.labels(symbol=sym).set(stats.median_delta_bps())
        if stats.n >= min_samples:
            calibrated += 1
    return calibrated


# ── Telegram ──────────────────────────────────────────────────────────────────

def _send_telegram(r: Any, *, notify_stream: str, text: str) -> None:
    try:
        r.xadd(
            notify_stream,
            {
                "type": "report",
                "subtype": "trailing_state_autocal",
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
            },
            maxlen=50_000,
        )
        logger.info("Telegram notification sent")
    except Exception as exc:
        logger.warning("Telegram notify failed: %s", exc)


# ── Promote ───────────────────────────────────────────────────────────────────

def _do_promote(
    r: Any,
    bins: dict[str, _SymbolStats],
    *,
    snapshot_ttl: int,
    notify_stream: str,
    n_calibrated: int,
    dwell_min: int,
) -> None:
    """Publish shadow=false snapshot to Redis and notify Telegram."""
    now_ms = int(time.time() * 1000)
    snap = {
        "shadow": False,
        "promoted": True,
        "promoted_ms": now_ms,
        "n_symbols": n_calibrated,
        "dwell_min": dwell_min,
        "bins": {
            sym: {
                "n": stats.n,
                "n_sl_moves": stats.n_sl_moves(),
                "error_rate": round(stats.error_rate(), 4),
                "dup_rate": round(stats.dup_rate(), 4),
                "median_delta_bps": round(stats.median_delta_bps(), 2),
            }
            for sym, stats in bins.items()
        },
    }
    r.set(RK.AUTOCAL_TRAILING_STATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)

    lines = []
    for sym, stats in sorted(bins.items()):
        lines.append(
            f"  • <b>{sym}</b>: n={stats.n}  moves={stats.n_sl_moves()}"
            f"  δ={stats.median_delta_bps():.1f}bps"
            f"  err={stats.error_rate():.1%}  dup={stats.dup_rate():.1%}"
        )

    text = (
        f"<b>✅ TrailingState Autocal — LIVE PROMOTED</b>\n\n"
        f"Все <b>{n_calibrated}</b> символов достигли min_samples "
        f"(dwell {dwell_min} мин). Sanity пройден.\n\n"
        f"<b>TrailingStateWorker переключён в LIVE режим</b> "
        f"(shadow=false). SL-команды теперь отправляются в gateway.\n\n"
        f"Статистика:\n" + "\n".join(lines) + "\n\n"
        f"Подхват в течение ≤60 с (без рестарта).\n\n"
        f"<b>Rollback:</b>\n"
        f"<code>docker exec redis-worker-1 redis-cli SET "
        f"{RK.AUTOCAL_TRAILING_STATE} '{{\"shadow\":true}}'</code>"
    )
    _send_telegram(r, notify_stream=notify_stream, text=text)
    logger.info("AUTO-PROMOTE: shadow=false published | n_symbols=%d", n_calibrated)
    _c_promote.labels(result="success").inc()


def _notify_ready_dwell(
    r: Any,
    bins: dict[str, _SymbolStats],
    *,
    notify_stream: str,
    n_calibrated: int,
    dwell_min: int,
    ready_age_sec: float,
) -> None:
    """First notification when dwell starts."""
    lines = []
    for sym, stats in sorted(bins.items()):
        if stats.n > 0:
            nd = stats.n_with_delta()
            delta_warn = " ⚠️ нет ценовых данных" if nd == 0 else ""
            lines.append(
                f"  • <b>{sym}</b>: n={stats.n}  moves={stats.n_sl_moves()}"
                f"  δ={stats.median_delta_bps():.1f}bps"
                f"  priced={nd}{delta_warn}"
            )

    text = (
        f"<b>⏳ TrailingState Autocal — dwell timer start</b>\n\n"
        f"<b>{n_calibrated}</b> символ(ов) достигли min_samples.\n"
        f"Taймер: <b>{dwell_min} мин</b> (пройдено: {ready_age_sec/60:.1f} мин).\n\n"
        f"Текущая статистика:\n" + "\n".join(lines) + "\n\n"
        f"Promote произойдёт автоматически при прохождении sanity checks."
    )
    _send_telegram(r, notify_stream=notify_stream, text=text)


def _notify_sanity_fail(
    r: Any,
    *,
    notify_stream: str,
    reason: str,
) -> None:
    text = (
        f"<b>⚠️ TrailingState Autocal — sanity check FAILED</b>\n\n"
        f"Promote заблокирован:\n<code>{reason}</code>\n\n"
        f"Калибратор продолжит накапливать статистику."
    )
    _send_telegram(r, notify_stream=notify_stream, text=text)


# ── Reader: TrailingStateWorker checks this key ──────────────────────────────

def publish_shadow_snapshot(r: Any, *, shadow: bool, snapshot_ttl: int) -> None:
    """Publish a minimal snapshot for TrailingStateWorker to read."""
    snap = {
        "shadow": shadow,
        "promoted": not shadow,
        "published_ms": int(time.time() * 1000),
    }
    r.set(RK.AUTOCAL_TRAILING_STATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=_e("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port           = _ei("TS_CAL_PORT",                  9922)
    poll_sec       = _ei("TS_CAL_POLL_SEC",              60)
    snapshot_ttl   = _ei("TS_CAL_SNAPSHOT_TTL",          7200)
    window_ms      = _ei("TS_CAL_WINDOW_MS",             43_200_000)   # 12 h
    min_samples    = _ei("TS_CAL_MIN_SAMPLES",           50)
    enforce        = _eb("TS_CAL_ENFORCE",               False)
    notify_stream  = _e( "TS_CAL_NOTIFY_STREAM",         RS.NOTIFY_TELEGRAM)
    symbols_raw    = _e( "TS_CAL_SYMBOLS",               "BTCUSDT,ETHUSDT")
    target_symbols = {s.strip() for s in symbols_raw.split(",") if s.strip()}

    auto_promote   = _eb("TS_CAL_AUTO_PROMOTE",          False)
    dwell_min      = _ei("TS_CAL_PROMOTE_DWELL_MIN",     30)
    max_error_rate = _ef("TS_CAL_PROMOTE_MAX_ERROR",     0.05)
    min_delta_bps  = _ef("TS_CAL_PROMOTE_MIN_DELTA_BPS", 2.0)
    max_dup_rate   = _ef("TS_CAL_PROMOTE_MAX_DUP_RATE",  0.02)

    logger.info(
        "trailing_state_autocal_v1 starting | port=%d poll_sec=%d enforce=%s "
        "auto_promote=%s dwell_min=%d window_h=%.1f min_samples=%d symbols=%s",
        port, poll_sec, enforce, auto_promote, dwell_min,
        window_ms / 3_600_000, min_samples, ",".join(sorted(target_symbols)),
    )

    start_http_server(port)
    _g_enforce.set(1.0 if auto_promote else 0.0)
    _g_shadow.set(0.0 if enforce else 1.0)
    _g_ready_age_sec.set(0.0)

    r = get_redis()

    # Restore previous promote state (survives restart)
    try:
        raw = r.get(RK.AUTOCAL_TRAILING_STATE)
        if raw:
            prev = json.loads(raw if isinstance(raw, str) else raw.decode())  # type: ignore[union-attr]
            if prev.get("promoted") and not enforce:
                enforce = True
                logger.info("Restored enforce=True from previous snapshot")
                _g_shadow.set(0.0)
    except Exception as exc:
        logger.warning("Could not restore previous snapshot: %s", exc)

    # Publish initial shadow state
    publish_shadow_snapshot(r, shadow=not enforce, snapshot_ttl=snapshot_ttl)

    bins: dict[str, _SymbolStats] = {}
    # `$` is a blocking-only sentinel — without block= it returns nothing.
    # Read the current last-generated-id and use it as the initial cursor so
    # subsequent non-blocking xread() calls pick up genuinely new events.
    cursor = "0-0"
    try:
        info = r.xinfo_stream("events:trailing:state")
        cursor = str(info.get("last-generated-id", "0-0")) if isinstance(info, dict) else "0-0"
        logger.info("Initial cursor seeded from XINFO STREAM: %s", cursor)
    except Exception as _exc:
        # Stream doesn't exist yet or other error → start from beginning.
        # First poll may pull historical entries but evict_old() drops anything
        # beyond window_ms, so memory stays bounded.
        logger.info("XINFO STREAM unavailable (%s) — cursor=0-0", _exc)
    _stop = False
    _ready_since = 0.0
    _dwell_notified = False
    _last_sanity_fail_notified = ""

    def _handle_signal(sig: int, _: Any) -> None:
        nonlocal _stop
        logger.info("Received signal %d — stopping", sig)
        _stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    next_poll = time.time()

    while not _stop:
        now = time.time()
        if now < next_poll:
            time.sleep(min(0.5, next_poll - now))
            continue

        next_poll = now + poll_sec
        t0 = time.time()
        _c_polls.inc()

        try:
            cursor, n_obs = _read_audit_stream(r, cursor, bins, window_ms)
            _c_obs.inc(n_obs)
        except Exception as exc:
            logger.error("read_audit_stream failed: %s", exc)
            _c_errors.labels(kind="read").inc()
            continue

        # Filter only target symbols
        active_bins = {sym: stats for sym, stats in bins.items() if sym in target_symbols}
        n_active = len(target_symbols)

        calibrated = _update_metrics(active_bins, min_samples)

        # ── Auto-promote ──────────────────────────────────────────────────────
        if auto_promote and not enforce and n_active > 0:
            if calibrated >= n_active:
                if _ready_since == 0.0:
                    _ready_since = time.time()
                    logger.info(
                        "All %d/%d symbols calibrated — dwell timer starts (%d min)",
                        calibrated, n_active, dwell_min,
                    )

                ready_age_sec = time.time() - _ready_since
                _g_ready_age_sec.set(ready_age_sec)

                # Notify once when dwell starts
                if not _dwell_notified and ready_age_sec >= 60:
                    _notify_ready_dwell(
                        r, active_bins,
                        notify_stream=notify_stream,
                        n_calibrated=calibrated,
                        dwell_min=dwell_min,
                        ready_age_sec=ready_age_sec,
                    )
                    _dwell_notified = True

                if ready_age_sec >= dwell_min * 60:
                    ok, reason = _sanity_ok(
                        active_bins, min_samples,
                        max_error_rate=max_error_rate,
                        min_delta_bps=min_delta_bps,
                        max_dup_rate=max_dup_rate,
                    )
                    if ok:
                        _do_promote(
                            r, active_bins,
                            snapshot_ttl=snapshot_ttl,
                            notify_stream=notify_stream,
                            n_calibrated=calibrated,
                            dwell_min=dwell_min,
                        )
                        enforce = True
                        _g_shadow.set(0.0)
                        # NOTE: _c_promote.labels(result="success") already
                        # incremented inside _do_promote() — do NOT add it here.
                    else:
                        if reason != _last_sanity_fail_notified:
                            _notify_sanity_fail(r, notify_stream=notify_stream, reason=reason)
                            _last_sanity_fail_notified = reason
                        logger.warning("Sanity check blocked promote: %s", reason)
                        _c_promote.labels(result="blocked_sanity").inc()
                        # Reset dwell so we wait another full dwell period
                        _ready_since = time.time()
                        _dwell_notified = False
                else:
                    _c_promote.labels(result="dwell_pending").inc()
                    logger.debug("Dwell pending: %.1f / %d min", ready_age_sec / 60, dwell_min)
            else:
                if _ready_since > 0.0:
                    logger.info(
                        "Calibrated count dropped (%d/%d) — resetting dwell timer",
                        calibrated, n_active,
                    )
                    _ready_since = 0.0
                    _dwell_notified = False
                    _last_sanity_fail_notified = ""
                _g_ready_age_sec.set(0.0)

        # Publish snapshot (always — even in shadow mode)
        try:
            now_ms = int(time.time() * 1000)
            snap = {
                "shadow": not enforce,
                "promoted": enforce,
                "published_ms": now_ms,
                "bins": {
                    sym: {
                        "n": stats.n,
                        "n_sl_moves": stats.n_sl_moves(),
                        "error_rate": round(stats.error_rate(), 4),
                        "dup_rate": round(stats.dup_rate(), 4),
                        "median_delta_bps": round(stats.median_delta_bps(), 2),
                    }
                    for sym, stats in active_bins.items()
                },
            }
            r.set(RK.AUTOCAL_TRAILING_STATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)
        except Exception as exc:
            logger.error("snapshot publish failed: %s", exc)
            _c_errors.labels(kind="snapshot").inc()

        elapsed_ms = (time.time() - t0) * 1000
        _h_poll_ms.observe(elapsed_ms)
        logger.info(
            "poll done | obs=%d calibrated=%d/%d enforce=%s elapsed_ms=%.1f",
            n_obs, calibrated, n_active, enforce, elapsed_ms,
        )

    logger.info("trailing_state_autocal_v1 stopped")


if __name__ == "__main__":
    main()
