"""regime_transition_producer.py — P1 #14-15 regime transition features.

Reads `signals:of:inputs` stream to track per-symbol regime history.
Computes two features and writes `ctx:regime_transition:{SYMBOL}` every INTERVAL_S.

Features produced:
  regime_transition_code  — Categorical code for the most recent regime change:
                            0=none/stable  1=range→trend  2=trend→range
                            3=range→squeeze  4=squeeze→range  5=other
  failed_breakout_count_30m — Count of range→trend→range round-trips in 30 min
                              (breakout that failed to hold).

State on restart: 30 min warmup during which failed_breakout_count may be 0.

ENV:
  RTP_READ_URL      source redis for signals:of:inputs (default redis-worker-1:6379/0)
  RTP_PUBLISH_URL   snapshot write target (default redis-worker-1:6379/0)
  RTP_SYMBOLS       comma-separated symbols
  RTP_INTERVAL_S    publish cadence (default 30)
  RTP_TTL_SEC       snapshot TTL (default 180)
  METRICS_PORT      Prometheus port (default 9888)
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
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("regime_transition_producer")

READ_URL = os.getenv("RTP_READ_URL",
                     os.getenv("REDIS_WORKER_1_URL", "redis://redis-worker-1:6379/0"))
PUBLISH_URL = os.getenv("RTP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "RTP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
INTERVAL_S = float(os.getenv("RTP_INTERVAL_S", "30"))
TTL_SEC = int(os.getenv("RTP_TTL_SEC", "180"))
HASH_PREFIX = "ctx:regime_transition:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9888"))
_30M_MS = 30 * 60 * 1000

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _events_total = Counter("rtp_regime_events_total", "Regime transitions tracked", ["symbol"])
    _publishes = Counter("rtp_publishes_total", "Snapshots published")
    _last_ok = Gauge("rtp_last_ok_ms", "Last publish ts ms")
except Exception:
    _events_total = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore

# Regime transition code mapping
_TRANSITION_CODES = {
    ("range", "trending_bull"): 1,
    ("range", "trending_bear"): 1,
    ("trending_bull", "range"): 2,
    ("trending_bear", "range"): 2,
    ("range", "squeeze"): 3,
    ("squeeze", "range"): 4,
}


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _norm_regime(r: Any) -> str:
    s = str(r or "").lower().strip()
    if "trend" in s or "bull" in s or "bear" in s:
        return "trending_bull" if "bull" in s else ("trending_bear" if "bear" in s else "trending_bull")
    if "range" in s or "chop" in s or "meanrev" in s:
        return "range"
    if "squeeze" in s:
        return "squeeze"
    if "expansion" in s:
        return "expansion"
    return "unknown"


class _RegimeState:
    """Per-symbol regime change tracker."""

    def __init__(self) -> None:
        # (ts_ms, regime_str) — newest last
        self._history: deque[tuple[int, str]] = deque(maxlen=500)
        self._last_regime: str = "unknown"
        self._last_code: int = 0
        self._last_transition_ms: int = 0
        # (ts_ms, from_regime, to_regime) for breakout detection
        self._transitions: deque[tuple[int, str, str]] = deque(maxlen=200)
        # (ts_s, ofi_val) for vol/price divergence
        self._ofi_history: deque[tuple[float, float]] = deque(maxlen=60)
        self._price_history: deque[tuple[float, float]] = deque(maxlen=60)

    def observe(self, regime: str, ts_ms: int, ofi: float = 0.0, price_momentum: float = 0.0) -> None:
        norm = _norm_regime(regime)
        if norm == "unknown":
            return
        self._history.append((ts_ms, norm))
        if ofi != 0.0:
            self._ofi_history.append((ts_ms / 1000.0, ofi))
        if price_momentum != 0.0:
            self._price_history.append((ts_ms / 1000.0, price_momentum))
        if norm != self._last_regime and self._last_regime != "unknown":
            code = _TRANSITION_CODES.get((self._last_regime, norm), 5)
            self._last_code = code
            self._last_transition_ms = ts_ms
            self._transitions.append((ts_ms, self._last_regime, norm))
        self._last_regime = norm

    def compute(self, now_ms: int) -> dict[str, float]:
        out: dict[str, float] = {"regime_transition_code": float(self._last_code)}

        # failed_breakout_count_30m: range→trend→range round-trips within 30 min
        cutoff = now_ms - _30M_MS
        window_trans = [(t, f, o) for t, f, o in self._transitions if t >= cutoff]
        failed = 0
        for i in range(len(window_trans) - 1):
            t0, f0, o0 = window_trans[i]
            t1, f1, o1 = window_trans[i + 1]
            if (f0 == "range" and "trend" in o0
                    and f1 == o0 and o1 == "range"
                    and (t1 - t0) <= 15 * 60 * 1000):
                failed += 1
        out["failed_breakout_count_30m"] = float(failed)

        # ── P2 Group D: extended regime features ─────────────────────────────

        # regime_transition_age_ms: ms since last regime change
        if self._last_transition_ms > 0:
            out["regime_transition_age_ms"] = float(now_ms - self._last_transition_ms)

        # Compute transition probabilities from recent history (last 20 transitions)
        recent_t = list(self._transitions)[-20:]
        n_trans = len(recent_t)
        if n_trans >= 3:
            trend_to_range = sum(
                1 for _, f, o in recent_t if "trend" in f and o == "range"
            )
            range_to_expansion = sum(
                1 for _, f, o in recent_t if f == "range" and o == "expansion"
            )
            out["trend_to_chop_prob"] = trend_to_range / n_trans
            out["chop_to_expansion_prob"] = range_to_expansion / n_trans

        # expansion_exhaustion_score: how long the expansion has persisted / failed
        # Higher = more likely exhausted. Based on failed_breakout_count.
        if "failed_breakout_count_30m" in out:
            out["expansion_exhaustion_score"] = min(1.0, out["failed_breakout_count_30m"] / 3.0)

        # range_break_attempt_count_30m: count of range→other transitions in 30m
        range_breaks = sum(
            1 for t, f, o in window_trans if f == "range" and o != "range"
        )
        out["range_break_attempt_count_30m"] = float(range_breaks)

        # vol_ofi_regime_agree: OFI direction alignment with regime direction
        if self._ofi_history:
            t_cut = now_ms / 1000.0 - 10.0
            recent_ofi = [v for t, v in self._ofi_history if t >= t_cut]
            if recent_ofi:
                mean_ofi = sum(recent_ofi) / len(recent_ofi)
                if "trend" in self._last_regime and "bull" in self._last_regime:
                    out["vol_ofi_regime_agree"] = max(0.0, min(1.0, mean_ofi + 0.5))
                elif "trend" in self._last_regime and "bear" in self._last_regime:
                    out["vol_ofi_regime_agree"] = max(0.0, min(1.0, -mean_ofi + 0.5))
                else:
                    # range/squeeze/expansion: agree when OFI is small
                    out["vol_ofi_regime_agree"] = max(0.0, 1.0 - abs(mean_ofi))

        # vol_price_divergence_score: OFI direction vs price momentum divergence
        if self._ofi_history and self._price_history:
            t_cut = now_ms / 1000.0 - 10.0
            recent_ofi = [v for t, v in self._ofi_history if t >= t_cut]
            recent_px = [v for t, v in self._price_history if t >= t_cut]
            if recent_ofi and recent_px:
                ofi_sign = 1.0 if sum(recent_ofi) > 0 else -1.0
                px_sign = 1.0 if sum(recent_px) > 0 else -1.0
                # Divergence = 1.0 when signs differ, 0.0 when aligned
                out["vol_price_divergence_score"] = 0.0 if ofi_sign == px_sign else 1.0

        return out


def _extract_regime(fields: dict) -> tuple[str | None, str | None, int, float, float]:
    """Extract symbol, regime, OFI and price_momentum from a signals:of:inputs entry."""
    raw = fields.get("payload") or fields.get("data")
    if not raw:
        return None, None, 0, 0.0, 0.0
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None, None, 0, 0.0, 0.0
    sym = None
    regime = None
    ts_ms = 0
    ofi = 0.0
    price_momentum = 0.0
    for candidate in (payload, payload.get("data") or {}, payload.get("indicators") or {}):
        if not isinstance(candidate, dict):
            continue
        if not sym:
            sym = (candidate.get("symbol") or candidate.get("s") or "").upper() or None
        if not regime:
            regime = (candidate.get("regime") or candidate.get("market_regime")
                      or (candidate.get("indicators") or {}).get("regime"))
        if not ts_ms:
            ts_ms = int(_f(candidate.get("ts_ms") or candidate.get("event_time_ms"), 0))
        if not ofi:
            ofi_raw = (candidate.get("ofi") or candidate.get("ofi_ml_norm")
                       or (candidate.get("indicators") or {}).get("ofi"))
            if ofi_raw is not None:
                ofi = _f(ofi_raw)
        if not price_momentum:
            pm_raw = (candidate.get("momentum_10s") or candidate.get("price_to_ema_bps")
                      or (candidate.get("indicators") or {}).get("momentum_10s"))
            if pm_raw is not None:
                price_momentum = _f(pm_raw)
    return sym, regime, ts_ms, ofi, price_momentum


def _main() -> int:
    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(READ_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)

    states: dict[str, _RegimeState] = {}
    last_publish = time.monotonic()
    last_id = "$"
    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info("regime_transition_producer: symbols=%s", SYMBOLS)

    while _running:
        try:
            try:
                resp = r_read.xread({"signals:of:inputs": last_id}, count=200, block=1000)
            except Exception:
                resp = []

            for _sk, entries in (resp or []):
                for eid, fields in entries:
                    last_id = eid
                    sym, regime, ts_ms, ofi, price_momentum = _extract_regime(fields)
                    if not sym or not regime or sym not in SYMBOLS:
                        continue
                    if sym not in states:
                        states[sym] = _RegimeState()
                    states[sym].observe(
                        regime, ts_ms or int(time.time() * 1000),
                        ofi=ofi, price_momentum=price_momentum,
                    )
                    if _events_total is not None:
                        _events_total.labels(symbol=sym).inc()

            now_m = time.monotonic()
            if now_m - last_publish >= INTERVAL_S:
                now_ms = int(time.time() * 1000)
                for sym in SYMBOLS:
                    state = states.get(sym)
                    if state is None:
                        continue
                    feats = state.compute(now_ms)
                    feats["ts_ms"] = now_ms
                    feats["quality_status"] = "OK"
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)
                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(int(time.time() * 1000))
                last_publish = now_m

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(1)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
