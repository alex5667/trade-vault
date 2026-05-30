"""cost_dynamics_producer.py — P1 #13 cost_widening_5s_bps.

Reads `tca:ema:{symbol}:{kind}:{session}` snapshots (from tca_priors_exporter)
and `ctx:deriv:{symbol}` for spread, then computes the 5-second change in
effective spread cost. Writes `ctx:cost_dynamics:{symbol}` every INTERVAL_S.

Features produced:
  cost_widening_5s_bps — Change in eff_spread_bps_ema over last 5 seconds.
                         Positive = widening (cost increasing), negative = tightening.

ENV:
  CDP_PUBLISH_URL   snapshot write target  (default redis-worker-1:6379/0)
  CDP_SYMBOLS       comma-separated symbols
  CDP_INTERVAL_S    publish cadence (default 30)
  CDP_TTL_SEC       snapshot TTL (default 120)
  CDP_WINDOW_S      look-back window for widening (default 10)
  METRICS_PORT      Prometheus port (default 9887)
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
log = logging.getLogger("cost_dynamics_producer")

PUBLISH_URL = os.getenv("CDP_PUBLISH_URL",
                        os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
# Sources (tca:ema, ctx:deriv) live in the main Redis (port 6379) not the
# worker-1 publish target. Allow an explicit override; default to REDIS_URL
# (which points at main) so the producer reads where the data actually is.
READ_URL = os.getenv("CDP_READ_URL",
                     os.getenv("REDIS_URL", PUBLISH_URL))
from core.symbols_config_v1 import get_crypto_symbols  # type: ignore  # noqa: E402
SYMBOLS = get_crypto_symbols(aliases=("CDP_SYMBOLS",))
INTERVAL_S = float(os.getenv("CDP_INTERVAL_S", "30"))
TTL_SEC = int(os.getenv("CDP_TTL_SEC", "120"))
WINDOW_S = float(os.getenv("CDP_WINDOW_S", "10"))
HASH_PREFIX = "ctx:cost_dynamics:"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9887"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _publishes = Counter("cdp_publishes_total", "Snapshots published")
    _last_ok = Gauge("cdp_last_ok_ms", "Last publish ts ms")
except Exception:
    _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


# ── Cost model defaults ───────────────────────────────────────────────────────
# Fee/impact/R-targets used to decompose post-cost EV for downstream
# consumers. Override via env if needed without retrofitting the schema.
_DEFAULT_FEE_BPS = float(os.getenv("CDP_DEFAULT_FEE_BPS", "3.0"))      # Binance USD-M blended
_DEFAULT_IMPACT_BPS = float(os.getenv("CDP_DEFAULT_IMPACT_BPS", "0.5"))  # canonical small-size impact
_DEFAULT_TP_R_BPS = float(os.getenv("CDP_DEFAULT_TP_R_BPS", "10.0"))   # typical 1R target
_DEFAULT_SL_R_BPS = float(os.getenv("CDP_DEFAULT_SL_R_BPS", "10.0"))
# Expected hold ≈ 5 min; funding 8h cycle → fraction per min = 1/480.
_DEFAULT_HOLD_MIN = float(os.getenv("CDP_DEFAULT_HOLD_MIN", "5.0"))


class _CostState:
    """Rolling cost history for one symbol.

    Observes spread time-series and emits the full ``p1_d_cost_dynamics`` +
    ``p2_c_cost_decomposition`` feature set declared in
    ``v15_of_shadow_watchlist_v1``. Every cycle returns the complete schema
    (zeros until enough samples accumulate) so coverage tracks producer
    liveness.
    """

    def __init__(self, maxlen: int = 60) -> None:
        # (wall_s, spread_bps)
        self._history: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def observe(self, spread_bps: float, wall_s: float) -> None:
        if spread_bps > 0:
            self._history.append((wall_s, spread_bps))

    def compute(self, window_s: float, funding_bps_per_8h: float = 0.0) -> dict[str, float]:
        # Always emit the full schema. zero defaults until at least 2 samples
        # in the window arrive — same liveness contract as the other producers.
        out: dict[str, float] = {
            "cost_widening_5s_bps": 0.0,
            "ev_after_fee_bps": 0.0,
            "ev_after_spread_bps": 0.0,
            "ev_after_impact_bps": 0.0,
            "tp1_net_after_cost_bps": 0.0,
            "sl_net_after_cost_bps": 0.0,
            "expected_hold_cost_bps": 0.0,
            "cost_regime_z": 0.0,
        }
        if len(self._history) < 2:
            return out

        now_s = self._history[-1][0]
        cutoff = now_s - window_s
        window = [(t, s) for t, s in self._history if t >= cutoff]
        if len(window) >= 2:
            oldest_spread = window[0][1]
            newest_spread = window[-1][1]
            out["cost_widening_5s_bps"] = newest_spread - oldest_spread

        # Current spread = most recent observation
        cur_spread = self._history[-1][1]
        half_spread = 0.5 * cur_spread

        # Cost components are emitted as negative EV deductions so a
        # downstream consumer can sum them with the entry edge to get net EV.
        fee_cost = _DEFAULT_FEE_BPS
        spread_cost = half_spread
        impact_cost = _DEFAULT_IMPACT_BPS
        # Funding cost over the expected hold window. ctx:deriv emits 8h funding.
        hold_funding_cost = abs(funding_bps_per_8h) * (_DEFAULT_HOLD_MIN / 480.0)

        out["ev_after_fee_bps"] = -fee_cost
        out["ev_after_spread_bps"] = -spread_cost
        out["ev_after_impact_bps"] = -impact_cost
        out["expected_hold_cost_bps"] = hold_funding_cost

        total_cost = fee_cost + spread_cost + impact_cost + hold_funding_cost
        out["tp1_net_after_cost_bps"] = _DEFAULT_TP_R_BPS - total_cost
        out["sl_net_after_cost_bps"] = -_DEFAULT_SL_R_BPS - total_cost

        # cost_regime_z: z-score of current spread vs the rolling history.
        spreads = [s for _, s in self._history]
        n = len(spreads)
        if n >= 5:
            mean = sum(spreads) / n
            var = sum((s - mean) ** 2 for s in spreads) / n
            std = math.sqrt(var) if var > 0 else 0.0
            if std > 1e-9:
                out["cost_regime_z"] = (cur_spread - mean) / std

        return out


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
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True) if READ_URL != PUBLISH_URL else r_read
    r = r_read  # backward compat: most calls use the read client
    states: dict[str, _CostState] = {s: _CostState() for s in SYMBOLS}
    last_publish = time.monotonic()
    _running = True

    def _sig(signum, _frame):
        nonlocal _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    log.info("cost_dynamics_producer: symbols=%s window=%.0fs", SYMBOLS, WINDOW_S)

    while _running:
        try:
            # Poll spread from tca:ema or ctx:deriv snapshots
            for sym in SYMBOLS:
                spread_bps = 0.0
                # Try TCA EMA snapshot first (most accurate)
                for kind in ("default", "iceberg", "delta_spike"):
                    for session in ("all", "us", "europe", "asia"):
                        raw = r.hget(f"tca:ema:{sym}:{kind}:{session}", "eff_spread_bps_ema")
                        if raw:
                            v = _f(raw)
                            if v > 0:
                                spread_bps = v
                                break
                    if spread_bps > 0:
                        break
                # Fallback: derive from deriv ctx (spread approximated from funding/basis)
                if spread_bps <= 0:
                    raw_deriv = r.get(f"ctx:deriv:{sym}")
                    if raw_deriv:
                        try:
                            d = json.loads(raw_deriv)
                            # basis_bps and spread proxy can be negative
                            # (contango/backwardation); only the magnitude matters
                            # for cost-widening dynamics. abs() so a healthy basis
                            # signal isn't silently dropped by `if > 0` below.
                            spread_bps = abs(_f(
                                d.get("spread_bps")
                                or d.get("basis_bps")
                                or 0
                            ))
                        except Exception:
                            pass
                if spread_bps > 0:
                    states[sym].observe(spread_bps, time.time())

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                for sym, state in states.items():
                    # Funding from ctx:deriv (8h funding rate in bps).
                    funding_bps_per_8h = 0.0
                    try:
                        raw_deriv = r.get(f"ctx:deriv:{sym}")
                        if raw_deriv:
                            d = json.loads(raw_deriv)
                            funding_bps_per_8h = _f(
                                d.get("funding_rate_bps")
                                or d.get("funding_bps")
                                or 0
                            )
                    except Exception:
                        pass

                    feats = state.compute(WINDOW_S, funding_bps_per_8h=funding_bps_per_8h)
                    # compute() always returns the full schema — publish
                    # unconditionally so coverage tracks producer liveness.
                    feats["ts_ms"] = int(time.time() * 1000)
                    feats["quality_status"] = "OK"
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                    except Exception as e:
                        log.warning("publish %s: %s", sym, e)
                if _publishes is not None:
                    _publishes.inc()
                if _last_ok is not None:
                    _last_ok.set(int(time.time() * 1000))
                last_publish = now

            time.sleep(5)

        except Exception as e:
            log.exception("loop: %s", e)
            time.sleep(2)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
