"""Outcome tracker for confidence-gated-out signals.

Pipeline:
  1. XREADGROUP on stream:signals:gated_out — pull new shadow signals.
  2. Hold each signal in an in-memory PEL until ts_ms + horizon_ms elapses.
  3. XRANGE on stream:tick_{SYMBOL} between (entry_ts, entry_ts + horizon_ms),
     compute realised path: high, low, close, ret_bps, tp_hit, sl_hit, r_mult.
  4. XADD synthetic outcome to stream:signals:gated_out_outcomes.
  5. XACK the input message.

PEL recovery: on startup we replay pending entries via XREADGROUP id="0" so
restarts don't lose in-flight signals.

Fail-open by design: any single signal evaluation error is logged + skipped.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge
from redis.asyncio import Redis

from core.redis_keys import RedisStreams as RS

log = logging.getLogger("gated_out_outcome_tracker")

INPUT_STREAM = os.getenv("SIGNAL_GATED_OUT_STREAM", RS.SIGNAL_GATED_OUT)
OUTPUT_STREAM = os.getenv("SIGNAL_GATED_OUT_OUTCOMES_STREAM", RS.SIGNAL_GATED_OUT_OUTCOMES)
GROUP = os.getenv("GATED_OUT_TRACKER_GROUP", "gated_out_tracker_v1")
CONSUMER = os.getenv("GATED_OUT_TRACKER_CONSUMER", "tracker_01")
HORIZON_MS = int(os.getenv("GATED_OUT_HORIZON_MS", str(30 * 60 * 1000)))  # 30 min default
POLL_BLOCK_MS = int(os.getenv("GATED_OUT_TRACKER_POLL_BLOCK_MS", "5000"))
EVAL_INTERVAL_SEC = int(os.getenv("GATED_OUT_TRACKER_EVAL_INTERVAL_SEC", "30"))
TICK_STREAM_TPL = os.getenv("TICK_STREAM_TPL", RS.TICK_TPL)
OUTPUT_MAXLEN = int(os.getenv("SIGNAL_GATED_OUT_OUTCOMES_MAXLEN", "200000"))
PENDING_LIMIT = int(os.getenv("GATED_OUT_PENDING_LIMIT", "50000"))

# Outcome interpretation thresholds.
# y=1 iff signed return >= TP threshold (bps) OR tp_hit==1 before sl_hit.
DEFAULT_TP_BPS = float(os.getenv("GATED_OUT_DEFAULT_TP_BPS", "15"))
DEFAULT_SL_BPS = float(os.getenv("GATED_OUT_DEFAULT_SL_BPS", "10"))

# Prometheus
g_pending = Gauge("gated_out_tracker_pending", "Pending signals awaiting horizon expiry")
g_evaluated_total = Counter("gated_out_tracker_evaluated_total", "Signals evaluated", ["result"])
g_skipped_total = Counter("gated_out_tracker_skipped_total", "Signals skipped", ["reason"])
g_eval_latency_seconds = Gauge("gated_out_tracker_eval_latency_seconds", "Last full eval pass duration")
g_input_lag_seconds = Gauge("gated_out_tracker_input_lag_seconds", "Age of oldest pending signal")
g_last_outcome_ts_ms = Gauge("gated_out_tracker_last_outcome_ts_ms", "Timestamp of last outcome written")


class PendingSignal:
    __slots__ = ("msg_id", "sid", "symbol", "direction", "entry", "sl", "tp_bps",
                 "sl_bps", "ts_ms", "confidence", "min_conf", "expire_ms")

    def __init__(
        self, msg_id: str, sid: str, symbol: str, direction: str,
        entry: float, sl: float, tp_bps: float, sl_bps: float,
        ts_ms: int, confidence: float, min_conf: float, expire_ms: int,
    ) -> None:
        self.msg_id = msg_id
        self.sid = sid
        self.symbol = symbol
        self.direction = direction
        self.entry = entry
        self.sl = sl
        self.tp_bps = tp_bps
        self.sl_bps = sl_bps
        self.ts_ms = ts_ms
        self.confidence = confidence
        self.min_conf = min_conf
        self.expire_ms = expire_ms


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _derive_tp_sl_bps(entry: float, sl: float, indicators: dict[str, Any]) -> tuple[float, float]:
    """Best-effort tp_bps/sl_bps derivation from payload."""
    sl_bps = DEFAULT_SL_BPS
    if entry > 0 and sl > 0:
        sl_bps = abs(entry - sl) / entry * 1e4
    tp_bps = DEFAULT_TP_BPS
    rr = _f(indicators.get("tp_rr") or indicators.get("rr"), 0.0)
    if rr > 0:
        tp_bps = sl_bps * rr
    return max(1.0, tp_bps), max(1.0, sl_bps)


def _parse_signal(msg_id: str, fields: dict[str, Any]) -> PendingSignal | None:
    """Parse a gated_out XADD into a PendingSignal."""
    try:
        raw = fields.get("payload")
        if not raw:
            return None
        d = json.loads(raw)
        sid = str(d.get("signal_id") or "")
        symbol = str(d.get("symbol") or "")
        direction = str(d.get("direction") or "").upper()
        ts_ms = int(d.get("ts_ms") or 0)
        entry = _f(d.get("entry"))
        sl = _f(d.get("sl"))
        indicators = d.get("indicators") or {}
        if not (sid and symbol and direction in ("LONG", "SHORT") and ts_ms > 0 and entry > 0):
            return None
        tp_bps, sl_bps = _derive_tp_sl_bps(entry, sl, indicators if isinstance(indicators, dict) else {})
        return PendingSignal(
            msg_id=msg_id, sid=sid, symbol=symbol, direction=direction,
            entry=entry, sl=sl, tp_bps=tp_bps, sl_bps=sl_bps, ts_ms=ts_ms,
            confidence=_f(d.get("confidence")), min_conf=_f(d.get("min_conf")),
            expire_ms=ts_ms + HORIZON_MS,
        )
    except Exception as e:
        log.warning("parse failed for msg %s: %s", msg_id, e)
        return None


async def _ensure_group(r: Redis) -> None:
    try:
        await r.xgroup_create(INPUT_STREAM, GROUP, id="$", mkstream=True)
        log.info("created consumer group %s on %s", GROUP, INPUT_STREAM)
    except Exception as e:
        # BUSYGROUP if already exists — fine
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create note: %s", e)


async def _fetch_ticks(r: Redis, symbol: str, ts_lo: int, ts_hi: int) -> list[tuple[int, float]]:
    """Returns sorted [(ts_ms, price)] in [ts_lo, ts_hi] from stream:tick_{symbol}."""
    stream = TICK_STREAM_TPL.format(symbol=symbol)
    out: list[tuple[int, float]] = []
    try:
        # XRANGE accepts {ts_ms} as id-prefix
        chunks = await r.xrange(stream, min=f"{ts_lo}-0", max=f"{ts_hi}-+", count=10000)
    except Exception as e:
        log.warning("xrange %s [%d..%d] failed: %s", stream, ts_lo, ts_hi, e)
        return out
    for entry_id, fields in chunks:
        try:
            ts = int(str(entry_id).split("-", 1)[0])
            # Tick payloads vary: prefer 'price', fall back to 'p' / 'last' / 'mid'.
            price_raw = fields.get("price") or fields.get("p") or fields.get("last") or fields.get("mid")
            if price_raw is None:
                # Some tick streams encode JSON in 'payload'
                pay = fields.get("payload")
                if pay:
                    try:
                        d = json.loads(pay)
                        price_raw = d.get("price") or d.get("p") or d.get("last")
                    except Exception:
                        price_raw = None
            if price_raw is None:
                continue
            px = _f(price_raw)
            if px > 0:
                out.append((ts, px))
        except Exception:
            continue
    out.sort(key=lambda t: t[0])
    return out


def _evaluate_path(p: PendingSignal, path: list[tuple[int, float]]) -> dict[str, Any] | None:
    """Compute outcome from price path."""
    if not path:
        return None
    sign = 1.0 if p.direction == "LONG" else -1.0
    high = max(px for _, px in path)
    low = min(px for _, px in path)
    close_ts, close_px = path[-1]

    tp_px = p.entry * (1 + sign * p.tp_bps / 1e4)
    sl_px = p.entry * (1 - sign * p.sl_bps / 1e4)

    tp_hit = False
    sl_hit = False
    hit_ts = close_ts
    hit_px = close_px
    for ts, px in path:
        if p.direction == "LONG":
            if px >= tp_px and not tp_hit:
                tp_hit = True
                hit_ts = ts
                hit_px = tp_px
                break
            if px <= sl_px and not sl_hit:
                sl_hit = True
                hit_ts = ts
                hit_px = sl_px
                break
        else:
            if px <= tp_px and not tp_hit:
                tp_hit = True
                hit_ts = ts
                hit_px = tp_px
                break
            if px >= sl_px and not sl_hit:
                sl_hit = True
                hit_ts = ts
                hit_px = sl_px
                break

    if tp_hit:
        ret_bps = p.tp_bps
        r_mult = p.tp_bps / max(1e-6, p.sl_bps)
        y = 1
    elif sl_hit:
        ret_bps = -p.sl_bps
        r_mult = -1.0
        y = 0
    else:
        # No barrier hit — use close
        ret_bps = sign * (close_px - p.entry) / p.entry * 1e4
        r_mult = ret_bps / max(1e-6, p.sl_bps)
        y = 1 if ret_bps > 0 else 0
        hit_ts = close_ts
        hit_px = close_px

    return {
        "v": 1,
        "sid": p.sid,
        "symbol": p.symbol,
        "direction": p.direction,
        "entry": p.entry,
        "ts_ms": p.ts_ms,
        "ts_close_ms": int(hit_ts),
        "horizon_ms": HORIZON_MS,
        "close_price": float(hit_px),
        "high": float(high),
        "low": float(low),
        "ret_bps": float(ret_bps),
        "r_mult": float(r_mult),
        "y": int(y),
        "y_edge": int(y),  # alias for joinability with labels:tb consumers
        "tp_hit": 1 if tp_hit else 0,
        "sl_hit": 1 if sl_hit else 0,
        "tp_bps": p.tp_bps,
        "sl_bps": p.sl_bps,
        "confidence": p.confidence,
        "min_conf": p.min_conf,
        "primary": 1,  # joinability flag
        "gated_out": 1,
    }


async def _emit_outcome(r: Redis, payload: dict[str, Any]) -> None:
    try:
        await r.xadd(
            OUTPUT_STREAM,
            {"payload": json.dumps(payload, ensure_ascii=False, default=str)},
            maxlen=OUTPUT_MAXLEN,
            approximate=True,
        )
        g_last_outcome_ts_ms.set(time.time() * 1000)
    except Exception as e:
        log.warning("xadd outcome failed: %s", e)


async def _replay_pel(r: Redis, pending: dict[str, PendingSignal]) -> None:
    """On startup, recover signals from the consumer group's PEL."""
    try:
        chunks = await r.xreadgroup(
            groupname=GROUP, consumername=CONSUMER,
            streams={INPUT_STREAM: "0"},
            count=PENDING_LIMIT, block=None,
        )
    except Exception as e:
        log.warning("PEL replay failed: %s", e)
        return
    for _stream, entries in chunks or []:
        for msg_id, fields in entries:
            sig = _parse_signal(str(msg_id), fields)
            if sig:
                pending[str(msg_id)] = sig
            else:
                # Bad entry — ack to avoid replay loop.
                try:
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                except Exception:
                    pass
                g_skipped_total.labels(reason="parse").inc()
    log.info("PEL replay loaded %d pending", len(pending))


async def _ingest_loop(r: Redis, pending: dict[str, PendingSignal]) -> None:
    """Continuously pull new entries from the group."""
    while True:
        try:
            if len(pending) >= PENDING_LIMIT:
                await asyncio.sleep(1.0)
                continue
            chunks = await r.xreadgroup(
                groupname=GROUP, consumername=CONSUMER,
                streams={INPUT_STREAM: ">"},
                count=200, block=POLL_BLOCK_MS,
            )
            for _stream, entries in chunks or []:
                for msg_id, fields in entries:
                    sig = _parse_signal(str(msg_id), fields)
                    if sig:
                        pending[str(msg_id)] = sig
                    else:
                        try:
                            await r.xack(INPUT_STREAM, GROUP, msg_id)
                        except Exception:
                            pass
                        g_skipped_total.labels(reason="parse").inc()
        except Exception as e:
            log.warning("ingest loop error: %s", e)
            await asyncio.sleep(1.0)


async def _eval_loop(r: Redis, pending: dict[str, PendingSignal]) -> None:
    """Periodically process pending signals whose horizon has expired."""
    while True:
        await asyncio.sleep(EVAL_INTERVAL_SEC)
        t0 = time.time()
        now_ms = int(t0 * 1000)
        ready = [(mid, sig) for mid, sig in pending.items() if sig.expire_ms <= now_ms]
        if pending:
            oldest = min(sig.ts_ms for sig in pending.values())
            g_input_lag_seconds.set(max(0.0, (now_ms - oldest) / 1000.0))
        for msg_id, sig in ready:
            try:
                path = await _fetch_ticks(r, sig.symbol, sig.ts_ms, sig.expire_ms)
                payload = _evaluate_path(sig, path)
                if payload is None:
                    g_skipped_total.labels(reason="no_ticks").inc()
                else:
                    await _emit_outcome(r, payload)
                    g_evaluated_total.labels(result=("win" if payload["y"] == 1 else "loss")).inc()
                # Always XACK to drain — even on no_ticks; otherwise stuck forever.
                await r.xack(INPUT_STREAM, GROUP, msg_id)
            except Exception as e:
                log.warning("eval failed for %s: %s", msg_id, e)
                g_skipped_total.labels(reason="exception").inc()
                # Don't xack — leave for next pass.
                continue
            pending.pop(msg_id, None)
        g_pending.set(len(pending))
        g_eval_latency_seconds.set(time.time() - t0)


async def run(redis_url: str) -> None:
    r = Redis.from_url(redis_url, decode_responses=True)
    await _ensure_group(r)
    pending: dict[str, PendingSignal] = {}
    await _replay_pel(r, pending)
    log.info("starting ingest+eval loops (horizon=%dms, eval_every=%ds)",
             HORIZON_MS, EVAL_INTERVAL_SEC)
    await asyncio.gather(
        _ingest_loop(r, pending),
        _eval_loop(r, pending),
    )
