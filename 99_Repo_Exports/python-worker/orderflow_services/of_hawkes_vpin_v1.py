#!/usr/bin/env python3
"""of_hawkes_vpin_v1.py — ADR-0005b / v7_of Hawkes intensity + VPIN toxicity service.

Subscribes to stream:tick_{symbol} and computes per-symbol:
  - VPIN-like toxicity: volume-signed imbalance EMA → vpin_tox_ema, vpin_tox_z
  - Hawkes-like split intensities: EMA event-rates by type →
      hawkes_taker_buy_lam, hawkes_taker_sell_lam,
      hawkes_cancel_bid_lam, hawkes_cancel_ask_lam,
      hawkes_limit_add_lam, hawkes_dt_s,
      hawkes_taker_lam (=buy+sell), hawkes_cancel_lam (=bid+ask), hawkes_churn_lam
  - L3 add-rate EMAs: added_bid_rate_ema, added_ask_rate_ema, added_total_rate_ema
  - Hawkes S-states (raw EMA accumulators for linear model interpretability):
      hawkes_S_taker_buy, hawkes_S_taker_sell,
      hawkes_S_cancel_bid, hawkes_S_cancel_ask, hawkes_S_limit_add

Publishes to:
  ctx:hawkes:{symbol}  (Redis HASH, TTL 30s)

Algorithm (O(1) per tick, no batching):
  λ_i(t) = S_i * decay_factor + event_count * alpha
  where decay_factor = exp(-beta * dt_s) — exponential kernel
  S_i is the self-exciting state accumulated between ticks.

  VPIN:  vpin_tox = EMA(|buy_qty - sell_qty| / (buy_qty + sell_qty + ε))
  Robust-z: vpin_tox_z = (vpin_tox_ema - hist_median) / (1.4826 * hist_mad + ε)

Status: PRODUCTION SKELETON.
  - Algorithm parameters are intentionally conservative (beta=1/30, alpha=0.5).
  - Full MLE calibration deferred to v7_of Phase 2 (ADR-0005b).

ENV
  HAWKES_SYMBOLS          comma-separated symbols (default: BTCUSDT,ETHUSDT,SOLUSDT)
  HAWKES_BETA             decay rate 1/s (default 0.0333 = 30s half-life)
  HAWKES_ALPHA            excitation coefficient (default 0.5)
  HAWKES_VPIN_ALPHA       EMA α for VPIN (default 0.05, ~20-bar EMA)
  HAWKES_TTL_SEC          Redis TTL seconds (default 30)
  HAWKES_PORT             Prometheus port (default 9148)
  REDIS_URL               (default redis://redis-worker-1:6379/0)
"""
from __future__ import annotations

import collections
import logging
import math
import os
import signal
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("of_hawkes_vpin")

_ENV_FLOAT = lambda k, d: float(os.getenv(k) or d)  # noqa: E731
_ENV_INT   = lambda k, d: int(os.getenv(k) or d)      # noqa: E731


# ---------------------------------------------------------------------------
# Per-symbol Hawkes state
# ---------------------------------------------------------------------------

class HawkesVPINState:
    """Exponential Hawkes kernel + VPIN-like toxicity for one symbol.

    All intensities in events/second.
    """

    HIST_LEN = 60  # samples kept for robust-z of VPIN

    __slots__ = (
        "symbol", "beta", "alpha", "vpin_alpha",
        "_last_ts_s",
        # S-states (raw Hawkes accumulators)
        "_S_tb", "_S_ts", "_S_cb", "_S_ca", "_S_la",
        # limit_add bid/ask split S-states
        "_S_la_bid", "_S_la_ask",
        # VPIN EMA + history for robust-z
        "_vpin_ema", "_vpin_hist",
        # timed VPIN history for 1m/5m means and slope
        "_vpin_timed_hist",
    )

    def __init__(self, symbol: str, beta: float, alpha: float, vpin_alpha: float) -> None:
        self.symbol = symbol
        self.beta = beta        # decay rate 1/s
        self.alpha = alpha      # excitation per event (normalized)
        self.vpin_alpha = vpin_alpha
        self._last_ts_s: float = 0.0
        self._S_tb = self._S_ts = self._S_cb = self._S_ca = self._S_la = 0.0
        self._S_la_bid: float = 0.0
        self._S_la_ask: float = 0.0
        self._vpin_ema: float = 0.5  # start at 0.5 (neutral)
        self._vpin_hist: collections.deque[float] = collections.deque(maxlen=self.HIST_LEN)
        # maxlen=600: 600s at 1 tick/s worst case; enough for 5m window
        self._vpin_timed_hist: collections.deque[tuple[float, float]] = collections.deque(maxlen=600)

    def update(
        self,
        ts_s: float,
        *,
        taker_buy_qty: float = 0.0,
        taker_sell_qty: float = 0.0,
        cancel_bid_qty: float = 0.0,
        cancel_ask_qty: float = 0.0,
        limit_add_qty: float = 0.0,
        limit_add_bid_qty: float = 0.0,
        limit_add_ask_qty: float = 0.0,
    ) -> dict[str, float]:
        dt = max(ts_s - self._last_ts_s, 0.0) if self._last_ts_s > 0 else 0.0
        self._last_ts_s = ts_s

        # Exponential decay: S_i(t) = S_i(t-dt) * exp(-beta * dt) + alpha * events_i
        decay = math.exp(-self.beta * dt) if dt > 0 else 1.0
        self._S_tb = self._S_tb * decay + self.alpha * _nonneg(taker_buy_qty)
        self._S_ts = self._S_ts * decay + self.alpha * _nonneg(taker_sell_qty)
        self._S_cb = self._S_cb * decay + self.alpha * _nonneg(cancel_bid_qty)
        self._S_ca = self._S_ca * decay + self.alpha * _nonneg(cancel_ask_qty)
        self._S_la = self._S_la * decay + self.alpha * _nonneg(limit_add_qty)

        # limit_add bid/ask split — use explicit fields when provided, else taker-weighted proxy.
        # Proxy: passive buyers absorb taker sells → bid-side adds correlate with taker_sell_qty.
        _la_total = _nonneg(limit_add_qty)
        if limit_add_bid_qty > 0.0 or limit_add_ask_qty > 0.0:
            _la_bid = _nonneg(limit_add_bid_qty)
            _la_ask = _nonneg(limit_add_ask_qty)
        else:
            _tb = _nonneg(taker_buy_qty)
            _ts = _nonneg(taker_sell_qty)
            _tv_sum = _tb + _ts + 1e-9
            _la_bid = _la_total * (_ts / _tv_sum)
            _la_ask = _la_total * (_tb / _tv_sum)
        self._S_la_bid = self._S_la_bid * decay + self.alpha * _la_bid
        self._S_la_ask = self._S_la_ask * decay + self.alpha * _la_ask

        # Intensities: λ_i = β * S_i (units: events/s)
        lam_tb = self.beta * self._S_tb
        lam_ts = self.beta * self._S_ts
        lam_cb = self.beta * self._S_cb
        lam_ca = self.beta * self._S_ca
        lam_la = self.beta * self._S_la
        lam_la_bid = self.beta * self._S_la_bid
        lam_la_ask = self.beta * self._S_la_ask

        # VPIN: |buy - sell| / total (volume-signed imbalance per bucket)
        total_v = _nonneg(taker_buy_qty) + _nonneg(taker_sell_qty)
        if total_v > 0:
            vpin_sample = abs(taker_buy_qty - taker_sell_qty) / total_v
            self._vpin_ema = self._vpin_ema + self.vpin_alpha * (vpin_sample - self._vpin_ema)
        self._vpin_hist.append(self._vpin_ema)
        self._vpin_timed_hist.append((ts_s, self._vpin_ema))

        # Robust-z for VPIN
        vpin_z = 0.0
        if len(self._vpin_hist) >= 5:
            hist = list(self._vpin_hist)
            med = _median(hist)
            mad = _mad(hist, med)
            if mad > 1e-9:
                vpin_z = (self._vpin_ema - med) / (1.4826 * mad)
                vpin_z = max(-10.0, min(10.0, vpin_z))

        # Rolling 1m/5m VPIN means and slope from timed history
        vpin_1m = vpin_5m = self._vpin_ema
        vpin_slope = 0.0
        if self._vpin_timed_hist:
            cutoff_1m = ts_s - 60.0
            cutoff_5m = ts_s - 300.0
            vals_1m = [v for t, v in self._vpin_timed_hist if t >= cutoff_1m]
            vals_5m = [v for t, v in self._vpin_timed_hist if t >= cutoff_5m]
            if vals_1m:
                vpin_1m = sum(vals_1m) / len(vals_1m)
            if vals_5m:
                vpin_5m = sum(vals_5m) / len(vals_5m)
            # slope: (current - value 60s ago) / dt, clamped ±0.1/s
            old_entries = [(t, v) for t, v in self._vpin_timed_hist if t <= cutoff_1m]
            if old_entries and dt > 0:
                vpin_60s_ago = old_entries[-1][1]
                raw_slope = (self._vpin_ema - vpin_60s_ago) / 60.0
                vpin_slope = max(-0.1, min(0.1, raw_slope))

        # limit_add imbalance
        _lam_la_denom = lam_la_bid + lam_la_ask + 1e-9
        lam_la_imb = (lam_la_bid - lam_la_ask) / _lam_la_denom

        return {
            "hawkes_dt_s":                dt,
            "hawkes_taker_buy_lam":       lam_tb,
            "hawkes_taker_sell_lam":      lam_ts,
            "hawkes_cancel_bid_lam":      lam_cb,
            "hawkes_cancel_ask_lam":      lam_ca,
            "hawkes_limit_add_lam":       lam_la,
            # legacy aggregates
            "hawkes_taker_lam":           lam_tb + lam_ts,
            "hawkes_cancel_lam":          lam_cb + lam_ca,
            "hawkes_churn_lam":           lam_cb + lam_ca + lam_la,
            # add-rate EMAs
            "added_bid_rate_ema":         lam_la_bid,
            "added_ask_rate_ema":         lam_la_ask,
            "added_total_rate_ema":       lam_la,
            # VPIN toxicity
            "vpin_tox_ema":               self._vpin_ema,
            "vpin_tox_z":                 vpin_z,
            # new rolling VPIN fields
            "vpin_tox_1m":                vpin_1m,
            "vpin_tox_5m":                vpin_5m,
            "vpin_tox_slope":             vpin_slope,
            # new limit_add side-split fields
            "hawkes_limit_add_bid_lam":   lam_la_bid,
            "hawkes_limit_add_ask_lam":   lam_la_ask,
            "hawkes_limit_add_imbalance": lam_la_imb,
            # raw S-states
            "hawkes_S_taker_buy":         self._S_tb,
            "hawkes_S_taker_sell":        self._S_ts,
            "hawkes_S_cancel_bid":        self._S_cb,
            "hawkes_S_cancel_ask":        self._S_ca,
            "hawkes_S_limit_add":         self._S_la,
        }


def _nonneg(x: Any) -> float:
    try:
        f = float(x)
        return f if f > 0 and math.isfinite(f) else 0.0
    except Exception:
        return 0.0


def _median(a: list[float]) -> float:
    s = sorted(a)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def _mad(a: list[float], med: float) -> float:
    devs = sorted(abs(x - med) for x in a)
    n = len(devs)
    if n == 0:
        return 0.0
    return devs[n // 2] if n % 2 else (devs[n // 2 - 1] + devs[n // 2]) / 2.0


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _sf(v: Any) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else 0.0
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    syms_env = os.getenv("HAWKES_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    symbols = [s.strip().upper() for s in syms_env.split(",") if s.strip()]
    beta = _ENV_FLOAT("HAWKES_BETA", 0.0333)          # 1/30s half-life
    alpha = _ENV_FLOAT("HAWKES_ALPHA", 0.5)
    vpin_alpha = _ENV_FLOAT("HAWKES_VPIN_ALPHA", 0.05)
    ttl_sec = _ENV_INT("HAWKES_TTL_SEC", 30)
    port = _ENV_INT("HAWKES_PORT", 9148)
    group = os.getenv("HAWKES_GROUP", "of-hawkes-vpin")
    consumer = os.getenv("HAWKES_CONSUMER", "of-hawkes-vpin-1")
    batch = _ENV_INT("HAWKES_BATCH", 500)

    logger.info("Starting Hawkes/VPIN service: symbols=%s beta=%.4f alpha=%.4f", symbols, beta, alpha)

    redis_client = get_redis()
    
    import redis as _redis
    redis_ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
    redis_ticks_client = _redis.Redis.from_url(redis_ticks_url, decode_responses=True)

    states: dict[str, HawkesVPINState] = {
        sym: HawkesVPINState(sym, beta, alpha, vpin_alpha)
        for sym in symbols
    }

    # Prometheus
    g_vpin = Gauge("hawkes_vpin_tox_ema", "VPIN EMA toxicity", ["symbol"])
    g_lam_taker = Gauge("hawkes_taker_lam", "Taker Hawkes intensity", ["symbol", "side"])
    c_ticks = Counter("hawkes_ticks_total", "Ticks processed", ["symbol"])

    streams: dict[str, str] = {}
    for sym in symbols:
        sk = f"stream:tick_{sym}"
        streams[sk] = ">"
        try:
            redis_ticks_client.xgroup_create(sk, group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.warning("xgroup_create failed for %s: %s", sk, e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    while not stop["flag"]:
        try:
            resp = redis_ticks_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams=streams,  # type: ignore[arg-type]
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        if not resp:
            continue

        for stream_key, messages in resp:  # type: ignore[union-attr]
            sk = _decode(stream_key)
            symbol = sk.replace("stream:tick_", "").upper()
            state = states.get(symbol)
            if state is None:
                continue

            ack_ids = []
            for msg_id, fields in messages:
                ack_ids.append(msg_id)
                try:
                    fields = {_decode(k): _decode(v) for k, v in fields.items()}
                    ts_ms = _sf(fields.get("ts_ms") or str(get_ny_time_millis()))
                    ts_s = ts_ms / 1000.0

                    out = state.update(
                        ts_s,
                        taker_buy_qty=_sf(fields.get("taker_buy_qty") or fields.get("buy_qty") or "0"),
                        taker_sell_qty=_sf(fields.get("taker_sell_qty") or fields.get("sell_qty") or "0"),
                        cancel_bid_qty=_sf(fields.get("cancel_bid_qty") or "0"),
                        cancel_ask_qty=_sf(fields.get("cancel_ask_qty") or "0"),
                        limit_add_qty=_sf(fields.get("limit_add_qty") or fields.get("added_total") or "0"),
                        limit_add_bid_qty=_sf(fields.get("add_bid_qty") or "0"),
                        limit_add_ask_qty=_sf(fields.get("add_ask_qty") or "0"),
                    )

                    out["ts_ms"] = ts_ms
                    try:
                        redis_client.hset(f"ctx:hawkes:{symbol}", mapping={
                            k: f"{v:.8f}" for k, v in out.items()
                        })
                        redis_client.expire(f"ctx:hawkes:{symbol}", ttl_sec)
                    except Exception as re:
                        logger.debug("Redis HSET hawkes %s failed: %s", symbol, re)

                    g_vpin.labels(symbol=symbol).set(out["vpin_tox_ema"])
                    g_lam_taker.labels(symbol=symbol, side="buy").set(out["hawkes_taker_buy_lam"])
                    g_lam_taker.labels(symbol=symbol, side="sell").set(out["hawkes_taker_sell_lam"])
                    c_ticks.labels(symbol=symbol).inc()
                except Exception as e:
                    logger.warning("Failed to process tick %s: %s", msg_id, e)

            if ack_ids:
                try:
                    redis_client.xack(sk, group, *ack_ids)
                except Exception:
                    pass

    logger.info("Hawkes/VPIN service stopped")


if __name__ == "__main__":
    main()
