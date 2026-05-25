"""book_rate_ema_producer.py — per-symbol book event-rate EMAs.

Subscribes to `stream:book_{SYMBOL}` (full top-N snapshots) and
`stream:tick_{SYMBOL}` (trade fills) and computes 4 v14_of features:

  • depth_pull_ratio       — cancel_rate / add_rate
  • cancel_to_fill_ratio   — cancel_rate / fill_rate (trade rate)
  • maker_cancel_ratio     — cancel_rate / add_rate (alias, kept separate)
  • book_refresh_rate_hz   — book updates per second EMA

Book add/cancel detection uses **diff of consecutive snapshots**:
  - price level with qty > prev qty, or new level  → add
  - price level with qty < prev qty, or disappeared → cancel

Trade rate uses `stream:tick_{SYMBOL}` — each entry is one fill.

ENV:
  REDIS_URL                book + tick ticks redis (default redis-ticks)
  BRE_PUBLISH_URL          snapshot target (default redis-worker-1)
  BRE_SYMBOLS              comma-separated
  BRE_HALF_LIFE_SEC        default 60
  BRE_INTERVAL_S           default 10
  BRE_TTL_SEC              default 60
  METRICS_PORT             default 9884
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("book_rate_ema_producer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
PUBLISH_URL = os.getenv("BRE_PUBLISH_URL",
                       os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "BRE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
HALF_LIFE_SEC = float(os.getenv("BRE_HALF_LIFE_SEC", "60"))
INTERVAL_S = int(os.getenv("BRE_INTERVAL_S", "10"))
HASH_PREFIX = os.getenv("BRE_HASH_PREFIX", "book_rates:")
TTL_SEC = int(os.getenv("BRE_TTL_SEC", "60"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9884"))
CVD_SERIES_SIZE = int(os.getenv("BRE_CVD_SERIES_SIZE", "25"))
HL_WINDOW_MS = int(os.getenv("BRE_HL_WINDOW_MS", str(60_000)))

_DECAY_PER_SEC = math.log(2.0) / max(1.0, HALF_LIFE_SEC)

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _events = Counter("bre_events_total", "Book events", ["symbol", "type"])
    _publishes = Counter("bre_publishes_total", "Snapshots published")
    _last_ok = Gauge("bre_last_ok_ms", "Last publish ts ms")
except Exception:
    _events = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _inc(m, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).inc()
    except Exception:
        pass


@dataclass
class EmaState:
    """Continuous-time EMA — decay between updates by elapsed wall-clock."""
    value: float = 0.0
    last_update_ms: int = 0

    def update(self, increment: float, now_ms: int) -> None:
        if self.last_update_ms == 0:
            self.value = increment
            self.last_update_ms = now_ms
            return
        elapsed_sec = max(0.0, (now_ms - self.last_update_ms) / 1000.0)
        factor = math.exp(-_DECAY_PER_SEC * elapsed_sec)
        self.value = self.value * factor + increment
        self.last_update_ms = now_ms

    def decayed(self, now_ms: int) -> float:
        if self.last_update_ms == 0:
            return 0.0
        elapsed_sec = max(0.0, (now_ms - self.last_update_ms) / 1000.0)
        factor = math.exp(-_DECAY_PER_SEC * elapsed_sec)
        return self.value * _DECAY_PER_SEC * factor


@dataclass
class BookState:
    """Per-symbol state: per-side EMAs, snapshot diffing, CVD, rolling high/low."""
    add_bid: EmaState = field(default_factory=EmaState)
    cancel_bid: EmaState = field(default_factory=EmaState)
    add_ask: EmaState = field(default_factory=EmaState)
    cancel_ask: EmaState = field(default_factory=EmaState)
    trade: EmaState = field(default_factory=EmaState)
    update: EmaState = field(default_factory=EmaState)
    prev_bids: dict[str, float] = field(default_factory=dict)
    prev_asks: dict[str, float] = field(default_factory=dict)
    # CVD: running accumulator + rolling list of snapshots per publish interval
    cvd_accum: float = 0.0
    cvd_snap: deque = field(default_factory=lambda: deque(maxlen=CVD_SERIES_SIZE))
    # Rolling price window: (ts_ms, price, notional)
    price_window: deque = field(default_factory=deque)
    # Persistent price EMA (30s half-life) for ema_px_30s feature
    price_ema: float = 0.0


def parse_levels(raw: Any) -> dict[str, float]:
    """Parse bids/asks JSON into {price_str: qty_float}. Empty dict on failure."""
    try:
        levels = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(levels, list):
            return {}
        out: dict[str, float] = {}
        for level in levels:
            try:
                px = str(level[0])
                qty = float(level[1])
                if qty >= 0:
                    out[px] = qty
            except (IndexError, TypeError, ValueError):
                continue
        return out
    except Exception:
        return {}


def diff_levels(
    prev: dict[str, float], curr: dict[str, float]
) -> tuple[int, int]:
    """Compare consecutive book snapshots. Returns (n_adds, n_cancels).

    No previous state (first snapshot) → (0, 0); no useful diff possible.
    Logic:
      qty increased or new level  → add
      qty decreased or level gone → cancel
    """
    if not prev:
        return 0, 0
    n_add = 0
    n_cancel = 0
    for px, qty in curr.items():
        prev_qty = prev.get(px, 0.0)
        if qty > prev_qty + 1e-9:
            n_add += 1
        elif qty < prev_qty - 1e-9:
            n_cancel += 1
    for px in prev:
        if px not in curr:
            n_cancel += 1  # level left top-N
    return n_add, n_cancel


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def run() -> int:
    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(REDIS_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)

    state: dict[str, BookState] = {s: BookState() for s in SYMBOLS}
    book_ids: dict[str, str] = {s: "$" for s in SYMBOLS}
    tick_ids: dict[str, str] = {s: "$" for s in SYMBOLS}

    log.info("starting: symbols=%s half_life=%.0fs interval=%ds",
             SYMBOLS, HALF_LIFE_SEC, INTERVAL_S)
    last_publish = time.monotonic()
    global _running
    _running = True

    while _running:
        try:
            streams: dict[str, str] = {}
            for s in SYMBOLS:
                streams[f"stream:book_{s}"] = book_ids[s]
                streams[f"stream:tick_{s}"] = tick_ids[s]

            try:
                resp = r_read.xread(streams, count=500, block=2000)  # type: ignore[arg-type]
            except Exception as e:
                log.debug("XREAD: %s", e)
                resp = []

            now_ms = int(time.time() * 1000)

            for stream_key, entries in (resp or []):
                if "book_" in stream_key:
                    sym = stream_key.split("book_", 1)[-1]
                    if sym not in state:
                        continue
                    st = state[sym]
                    for entry_id, fields in entries:
                        book_ids[sym] = entry_id
                        ev_ts = int(entry_id.split("-")[0])
                        f = fields if isinstance(fields, dict) else {}
                        curr_bids = parse_levels(f.get("bids"))
                        curr_asks = parse_levels(f.get("asks"))
                        if not curr_bids and not curr_asks:
                            continue
                        n_add_bid, n_cancel_bid = diff_levels(st.prev_bids, curr_bids)
                        n_add_ask, n_cancel_ask = diff_levels(st.prev_asks, curr_asks)
                        if n_add_bid:
                            st.add_bid.update(float(n_add_bid), ev_ts)
                            _inc(_events, sym, "add_bid")
                        if n_cancel_bid:
                            st.cancel_bid.update(float(n_cancel_bid), ev_ts)
                            _inc(_events, sym, "cancel_bid")
                        if n_add_ask:
                            st.add_ask.update(float(n_add_ask), ev_ts)
                            _inc(_events, sym, "add_ask")
                        if n_cancel_ask:
                            st.cancel_ask.update(float(n_cancel_ask), ev_ts)
                            _inc(_events, sym, "cancel_ask")
                        st.update.update(1.0, ev_ts)
                        st.prev_bids = curr_bids
                        st.prev_asks = curr_asks

                elif "tick_" in stream_key:
                    sym = stream_key.split("tick_", 1)[-1]
                    if sym not in state:
                        continue
                    st = state[sym]
                    for entry_id, fields in entries:
                        tick_ids[sym] = entry_id
                        ev_ts = int(entry_id.split("-")[0])
                        f = fields if isinstance(fields, dict) else {}
                        try:
                            price = float(f.get("price") or f.get("p") or 0)
                        except (TypeError, ValueError):
                            continue
                        st.trade.update(1.0, ev_ts)
                        _inc(_events, sym, "trade")
                        # CVD accumulation
                        qty = float(f.get("quantity") or f.get("qty") or f.get("q") or 0)
                        side = (f.get("side") or f.get("S") or "").upper()
                        if qty > 0:
                            st.cvd_accum += qty if side.startswith("B") else -qty
                        # Rolling price window: (ts_ms, price, qty)
                        if price > 0:
                            st.price_window.append((ev_ts, price, qty))

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                pub_now_ms = int(time.time() * 1000)
                for sym, st in state.items():
                    add_bid = st.add_bid.decayed(pub_now_ms)
                    cancel_bid = st.cancel_bid.decayed(pub_now_ms)
                    add_ask = st.add_ask.decayed(pub_now_ms)
                    cancel_ask = st.cancel_ask.decayed(pub_now_ms)
                    trade_rate = st.trade.decayed(pub_now_ms)
                    refresh_rate = st.update.decayed(pub_now_ms)
                    add_rate = add_bid + add_ask
                    cancel_rate = cancel_bid + cancel_ask

                    if add_rate + cancel_rate + trade_rate + refresh_rate <= 0:
                        continue

                    feats: dict[str, Any] = {
                        "added_bid_rate_ema": add_bid,
                        "added_ask_rate_ema": add_ask,
                        "cancel_bid_rate_ema": cancel_bid,
                        "cancel_ask_rate_ema": cancel_ask,
                        "trade_rate_ema": trade_rate,
                        "book_update_rate_ema": refresh_rate,
                        "book_refresh_rate_hz": refresh_rate,
                    }
                    if add_rate > 1e-9:
                        feats["depth_pull_ratio"] = cancel_rate / add_rate
                        feats["maker_cancel_ratio"] = cancel_rate / add_rate
                    if trade_rate > 1e-9:
                        feats["cancel_to_fill_ratio"] = cancel_rate / trade_rate
                    # Rolling 1m price window: high/low/close/vwap/price_10s_ago/ema
                    cutoff = pub_now_ms - HL_WINDOW_MS
                    while st.price_window and st.price_window[0][0] < cutoff:
                        st.price_window.popleft()
                    if st.price_window:
                        prices = [p for _, p, _ in st.price_window]
                        qtys = [q for _, _, q in st.price_window]
                        feats["high_1m"] = max(prices)
                        feats["low_1m"] = min(prices)
                        feats["close_px"] = prices[-1]
                        total_qty = sum(qtys)
                        if total_qty > 0:
                            feats["vwap_1m"] = sum(p * q for p, q in zip(prices, qtys)) / total_qty
                        # Price ~10s ago
                        cutoff_10s = pub_now_ms - 10_000
                        ago_entries = [p for ts, p, _ in st.price_window if ts <= cutoff_10s]
                        if ago_entries:
                            feats["price_10s_ago"] = ago_entries[-1]
                        # Persistent 30s EMA of price
                        alpha = 1.0 - math.exp(-math.log(2.0) / 30.0 * INTERVAL_S)
                        if st.price_ema == 0.0:
                            st.price_ema = prices[-1]
                        else:
                            st.price_ema = st.price_ema * (1 - alpha) + prices[-1] * alpha
                        feats["ema_px_30s"] = st.price_ema
                    # CVD series: snapshot current accumulator value each interval
                    st.cvd_snap.append(st.cvd_accum)
                    if len(st.cvd_snap) >= 2:
                        feats["cvd_series"] = list(st.cvd_snap)
                    feats["ts_ms"] = pub_now_ms
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                        _inc(_publishes)
                    except Exception as e:
                        log.warning("publish %s failed: %s", sym, e)

                if _last_ok is not None:
                    try:
                        _last_ok.set(pub_now_ms)
                    except Exception:
                        pass
                last_publish = now

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(1)

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
