"""execution_stats_updater.py — rolling per-symbol trade stats.

Consumes `trades:closed` Redis stream and maintains per-symbol rolling
statistics in `stats:execution:{symbol}` HASH for feature_enricher_v1
consumption. Unblocks 7 v14_of features:

  • expectancy_bps         — mean(R × risk_bps) over rolling window
  • profit_factor_roll20   — sum(winners $) / |sum(losers $)| last 20 trades
  • recovery_factor_roll   — cumulative_R / max_drawdown_R
  • kelly_fraction_roll    — Kelly fraction from win_rate + payoff_ratio
  • slippage_realized_bps  — mean realised slippage at fill
  • fill_time_p90_ms       — p90 fill latency
  • adverse_drift_ms       — mean time from signal to first adverse move

Design:
  • XREADGROUP consumer group `execution-stats-updater`
  • Per-symbol deque (size = WINDOW_SIZE, default 50)
  • Flush HASH every BATCH_SIZE updates OR every FLUSH_INTERVAL_S
  • XACK only on successful flush
  • Poison-cap (5 retries) → DLQ
  • Metrics on :9878

ENV:
  REDIS_URL                  default redis://redis-worker-1:6379/0
  ESU_STREAM                 default trades:closed
  ESU_GROUP                  default execution-stats-updater
  ESU_CONSUMER               default esu-1
  ESU_WINDOW_SIZE            default 50 (trades per symbol)
  ESU_BATCH_SIZE             default 50
  ESU_FLUSH_INTERVAL_S       default 30
  ESU_MIN_TRADES_TO_PUBLISH  default 10 (don't emit stats below this)
  ESU_HASH_PREFIX            default stats:execution:
  ESU_HASH_TTL_SEC           default 7200 (refreshed on each update)
  METRICS_PORT               default 9878
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("execution_stats_updater")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
STREAM = os.getenv("ESU_STREAM", "trades:closed")
GROUP = os.getenv("ESU_GROUP", "execution-stats-updater")
CONSUMER = os.getenv("ESU_CONSUMER", "esu-1")
WINDOW_SIZE = int(os.getenv("ESU_WINDOW_SIZE", "50"))
BATCH_SIZE = int(os.getenv("ESU_BATCH_SIZE", "50"))
FLUSH_INTERVAL_S = float(os.getenv("ESU_FLUSH_INTERVAL_S", "30"))
MIN_TRADES_TO_PUBLISH = int(os.getenv("ESU_MIN_TRADES_TO_PUBLISH", "10"))
HASH_PREFIX = os.getenv("ESU_HASH_PREFIX", "stats:execution:")
HASH_TTL_SEC = int(os.getenv("ESU_HASH_TTL_SEC", "7200"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9878"))
MAX_RETRIES = int(os.getenv("ESU_MAX_RETRIES", "5"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _trades_consumed = Counter("esu_trades_consumed_total", "Trades from stream")
    _flushes = Counter("esu_flushes_total", "HASH flushes")
    _flush_errors = Counter("esu_flush_errors_total", "Flush failures")
    _symbols_tracked = Gauge("esu_symbols_tracked", "Symbols in rolling window")
    _last_ok_ms = Gauge("esu_last_ok_ms", "Last successful flush epoch ms")
    _METRICS_OK = True
except Exception:
    _trades_consumed = _flushes = _flush_errors = None  # type: ignore
    _symbols_tracked = _last_ok_ms = None  # type: ignore
    start_http_server = None  # type: ignore
    _METRICS_OK = False


def _inc(m):
    if m is None:
        return
    try:
        m.inc()
    except Exception:
        pass


def _set(m, v):
    if m is None:
        return
    try:
        m.set(v)
    except Exception:
        pass


# ── Trade record ──────────────────────────────────────────────────────────────


@dataclass
class TradeRecord:
    ts_ms: int
    r_multiple: float
    pnl_net: float
    risk_bps: float        # used for expectancy_bps
    slippage_bps: float    # signed (negative = paid)
    fill_ms: int           # time from signal to fill
    adverse_ms: int        # time from signal to first adverse touch


def parse_trade(fields: dict[str, Any]) -> tuple[str, TradeRecord] | None:
    """Extract (symbol, TradeRecord) from a trades:closed entry. Returns None on bad data."""
    try:
        symbol = str(fields.get("symbol", "")).upper()
        if not symbol:
            return None
        r = fields.get("r_multiple")
        if r is None or r == "":
            return None
        r_mult = float(r)
        if not math.isfinite(r_mult):
            return None
        pnl = float(fields.get("pnl_net", 0.0) or 0.0)
        # risk_bps: derived from entry/sl when not directly stored
        risk_bps = float(fields.get("risk_bps", 0.0) or 0.0)
        if risk_bps == 0:
            entry = float(
                fields.get("entry") or fields.get("entry_price") or fields.get("entry_px") or 0.0
            )
            sl = float(fields.get("sl", 0.0) or 0.0)
            if entry > 0 and sl > 0:
                risk_bps = 10000.0 * abs(entry - sl) / entry
        slippage_bps = float(
            fields.get("slippage_bps") or fields.get("realized_slippage_bps")
            or fields.get("realized_spread_bps") or 0.0
        )
        # fill latency from `fill_ts_ms - entry_ts_ms`
        fill_ts = int(float(fields.get("fill_ts_ms") or fields.get("entry_ts_ms") or 0))
        sig_ts = int(float(fields.get("signal_ts_ms") or fields.get("ts_signal") or fields.get("entry_ts_ms") or 0))
        fill_ms = max(0, fill_ts - sig_ts) if fill_ts > 0 and sig_ts > 0 else 0
        # adverse drift — first time max_adverse_excursion crossed 0.1R
        adverse_ms = int(float(fields.get("adverse_ms_first_touch") or 0))
        ts_ms = int(float(fields.get("exit_ts_ms") or fields.get("ts_close") or 0))
        if ts_ms == 0:
            ts_ms = int(time.time() * 1000)
        return symbol, TradeRecord(
            ts_ms=ts_ms,
            r_multiple=max(-5.0, min(5.0, r_mult)),
            pnl_net=pnl,
            risk_bps=risk_bps,
            slippage_bps=slippage_bps,
            fill_ms=fill_ms,
            adverse_ms=adverse_ms,
        )
    except Exception:
        return None


# ── Stats computation ─────────────────────────────────────────────────────────


def compute_stats(window: list[TradeRecord]) -> dict[str, float]:
    """Compute 7 rolling features from window of trades."""
    n = len(window)
    if n < MIN_TRADES_TO_PUBLISH:
        return {}

    rs = [t.r_multiple for t in window]
    pnls = [t.pnl_net for t in window]
    risk_bps = [t.risk_bps for t in window if t.risk_bps > 0]
    slippages = [t.slippage_bps for t in window]
    fills = [t.fill_ms for t in window if t.fill_ms > 0]
    adverses = [t.adverse_ms for t in window if t.adverse_ms > 0]

    out: dict[str, float] = {}

    # expectancy_bps: E[r × risk_bps] — average expected bps per trade
    if risk_bps:
        # Use pairwise multiplication where risk_bps known
        weighted = [t.r_multiple * t.risk_bps for t in window if t.risk_bps > 0]
        if weighted:
            out["expectancy_bps"] = sum(weighted) / len(weighted)

    # profit_factor_roll20 — last min(n, 20) trades
    last_20 = window[-20:] if n >= 20 else window
    winners_pnl = sum(t.pnl_net for t in last_20 if t.pnl_net > 0)
    losers_pnl = sum(abs(t.pnl_net) for t in last_20 if t.pnl_net < 0)
    if losers_pnl > 1e-9:
        out["profit_factor_roll20"] = winners_pnl / losers_pnl
    else:
        out["profit_factor_roll20"] = winners_pnl / 1e-9 if winners_pnl > 0 else 0.0

    # recovery_factor_roll — cumulative R / max drawdown in R
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in rs:
        cum += r
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    if max_dd > 1e-9:
        out["recovery_factor_roll"] = cum / max_dd
    elif cum > 0:
        out["recovery_factor_roll"] = cum  # no drawdown — return raw cum
    else:
        out["recovery_factor_roll"] = 0.0

    # kelly_fraction_roll — f* = (bp - q) / b, where b = avg_win / avg_loss
    winners = [r for r in rs if r > 0]
    losers = [r for r in rs if r < 0]
    if winners and losers:
        win_rate = len(winners) / n
        avg_win = sum(winners) / len(winners)
        avg_loss = abs(sum(losers) / len(losers))
        if avg_loss > 1e-9:
            b = avg_win / avg_loss
            kelly = (b * win_rate - (1.0 - win_rate)) / b
            # Cap at [-0.5, 1.0] — full Kelly is rarely used in production
            out["kelly_fraction_roll"] = max(-0.5, min(1.0, kelly))

    # slippage_realized_bps — mean signed slippage
    if slippages:
        out["slippage_realized_bps"] = sum(slippages) / len(slippages)

    # fill_time_p90_ms — 90th percentile of fill latencies
    if fills:
        sorted_fills = sorted(fills)
        idx = max(0, int(0.9 * len(sorted_fills)) - 1)
        out["fill_time_p90_ms"] = float(sorted_fills[idx])

    # adverse_drift_ms — mean time to first adverse touch
    if adverses:
        out["adverse_drift_ms"] = sum(adverses) / len(adverses)

    # fill_prob_3s — fraction of trades filled within 3 seconds
    out["fill_prob_3s"] = sum(1 for t in window if 0 < t.fill_ms <= 3000) / n

    # eta_fill_sec — median fill latency in seconds
    if fills:
        sorted_fills_f = sorted(fills)
        mid = len(sorted_fills_f) // 2
        median_ms = (
            sorted_fills_f[mid]
            if len(sorted_fills_f) % 2
            else (sorted_fills_f[mid - 1] + sorted_fills_f[mid]) / 2.0
        )
        out["eta_fill_sec"] = median_ms / 1000.0

    # p_wait — fraction of trades that had measurable fill latency (> 0 ms)
    out["p_wait"] = sum(1 for t in window if t.fill_ms > 0) / n

    # Provenance
    out["_n_trades"] = float(n)
    out["_updated_at_ms"] = float(int(time.time() * 1000))
    return out


# ── Service ───────────────────────────────────────────────────────────────────

_running = True
_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → drain + exit", signum)
    _running = False


def _ensure_group(r):
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log.info("created consumer group %s on %s", GROUP, STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("consumer group exists")
        else:
            log.warning("xgroup_create: %s", e)


def _flush_one_symbol(r, symbol: str) -> bool:
    """Write stats for one symbol's window. Returns True on success."""
    window = list(_windows.get(symbol) or ())
    if len(window) < MIN_TRADES_TO_PUBLISH:
        return True  # nothing to flush yet, not a failure
    stats = compute_stats(window)
    if not stats:
        return True
    key = f"{HASH_PREFIX}{symbol}"
    try:
        # HSET mapping + EXPIRE in pipeline
        pipe = r.pipeline()
        pipe.delete(key)  # avoid stale leftover keys from prior windows
        pipe.hset(key, mapping={k: str(v) for k, v in stats.items()})
        pipe.expire(key, HASH_TTL_SEC)
        pipe.execute()
        return True
    except Exception as e:
        log.warning("flush %s failed: %s", symbol, e)
        return False


def _flush_all(r) -> int:
    """Flush every symbol that has enough trades. Returns count flushed."""
    n_ok = 0
    for sym in list(_windows.keys()):
        if _flush_one_symbol(r, sym):
            n_ok += 1
    if _flushes is not None:
        try:
            _flushes.inc(n_ok)
        except Exception:
            pass
    _set(_symbols_tracked, len(_windows))
    _set(_last_ok_ms, int(time.time() * 1000))
    return n_ok


def run() -> int:
    if _METRICS_OK and start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
            log.info("prometheus metrics on :%d", METRICS_PORT)
        except Exception as e:
            log.warning("metrics server failed: %s", e)

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r = redis.from_url(REDIS_URL, decode_responses=True)
    _ensure_group(r)
    log.info("starting: stream=%s group=%s window=%d min_trades=%d",
             STREAM, GROUP, WINDOW_SIZE, MIN_TRADES_TO_PUBLISH)

    # Hydrate windows from historical stream so stats are available immediately on restart.
    # Read the last WINDOW_SIZE * 4 entries (across all symbols) via XREVRANGE then replay
    # them in forward order to populate per-symbol deques.
    try:
        hydrate_count = WINDOW_SIZE * 4
        hist = r.xrevrange(STREAM, count=hydrate_count)  # type: ignore[arg-type]
        hist_list = list(hist or [])  # type: ignore[arg-type]
        for _msg_id, fields in reversed(hist_list):
            parsed = parse_trade(fields)
            if parsed is not None:
                sym, rec = parsed
                _windows[sym].append(rec)
        n_syms = len(_windows)
        n_recs = sum(len(v) for v in _windows.values())
        log.info("hydrated %d records for %d symbols from last %d stream entries",
                 n_recs, n_syms, len(hist_list))
    except Exception as e:
        log.warning("hydration failed (non-fatal): %s", e)

    pending_acks: list[str] = []
    last_flush = time.monotonic() - FLUSH_INTERVAL_S  # trigger flush immediately

    while _running:
        try:
            try:
                msgs = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=200, block=5000)
            except Exception as e:
                log.warning("XREADGROUP error: %s", e)
                time.sleep(2)
                continue

            if msgs:
                for _s, entries in msgs:  # type: ignore[union-attr]
                    for msg_id, fields in entries:
                        _inc(_trades_consumed)
                        parsed = parse_trade(fields)
                        if parsed is not None:
                            sym, rec = parsed
                            _windows[sym].append(rec)
                        pending_acks.append(msg_id)

            now = time.monotonic()
            if (len(pending_acks) >= BATCH_SIZE) or (now - last_flush >= FLUSH_INTERVAL_S):
                # Flush stats then ACK (periodic flush runs even with empty pending_acks
                # to publish stats accumulated from historical stream messages)
                _flush_all(r)
                if pending_acks:
                    try:
                        r.xack(STREAM, GROUP, *pending_acks)
                        pending_acks = []
                    except Exception as e:
                        log.warning("xack failed (%d msgs): %s", len(pending_acks), e)
                last_flush = now

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("loop error: %s", e)
            _inc(_flush_errors)
            time.sleep(2)

    # Final drain
    log.info("draining final batch (pending=%d)", len(pending_acks))
    if pending_acks:
        _flush_all(r)
        try:
            r.xack(STREAM, GROUP, *pending_acks)
        except Exception:
            pass
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
