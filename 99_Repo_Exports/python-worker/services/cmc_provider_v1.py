"""cmc_provider_v1.py — Global crypto market metrics provider.

Fetches CoinGecko /api/v3/global (free, no auth) every CMC_INTERVAL_S seconds
and writes a HSET to `runtime:provider:coinmarketcap:global` on main Redis.

This unblocks v14_of/v15_of features:
  cmc_btc_dom_pct         BTC market dominance %
  cmc_total_mcap_usd      total crypto market cap (trillions USD)
  cmc_total_volume_usd    total 24h volume (billions USD)
  cmc_active_cryptos      count of active cryptocurrencies

Also maintains `btc_dominance_momentum` (Δbps vs. previous snapshot) consumed
by v13_of btc_dominance_momentum feature.

Redis key (HASH):
  runtime:provider:coinmarketcap:global
  Fields:
    btc_dominance_pct        float — e.g. 54.37
    total_market_cap_usd     float — raw USD (divided by 1e12 in consumers)
    total_volume_24h_usd     float — raw USD (divided by 1e9 in consumers)
    active_cryptocurrencies  int   — e.g. 14000
    btc_dominance_momentum   float — Δ vs. prev snapshot, in basis points per hour
    ts_ms                    int   — publish epoch_ms
    source                   str   — "coingecko"
    source_url               str   — API endpoint used

ENV:
  REDIS_URL            main redis (default redis://redis:6379/0)
  CMC_PROVIDER_REDIS_URL  override for main redis
  CMC_INTERVAL_S       default 300 (5 min)
  CMC_TTL_SEC          default 3600 (1 h safety TTL)
  CMC_HTTP_TIMEOUT_S   default 10
  METRICS_PORT         default 9922
  LOG_LEVEL            default INFO
  CMC_API_URL          override CoinGecko endpoint (for testing/alternative source)
"""
from __future__ import annotations

import json
import logging
import os
import signal as _signal
import sys
import time
import urllib.request

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cmc_provider_v1")

REDIS_URL = os.getenv(
    "CMC_PROVIDER_REDIS_URL",
    os.getenv("REDIS_URL", "redis://redis:6379/0"),
)
API_URL = os.getenv(
    "CMC_API_URL",
    "https://api.coingecko.com/api/v3/global",
)
REDIS_KEY = "runtime:provider:coinmarketcap:global"
INTERVAL_S = int(os.getenv("CMC_INTERVAL_S", "300"))
TTL_SEC = int(os.getenv("CMC_TTL_SEC", "3600"))
HTTP_TIMEOUT_S = float(os.getenv("CMC_HTTP_TIMEOUT_S", "10"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9922"))

try:
    from prometheus_client import Counter, Gauge, start_http_server as _start_http

    _fetches_total = Counter("cmc_provider_fetches_total", "API fetch attempts", ["result"])
    _btc_dom = Gauge("cmc_provider_btc_dominance_pct", "BTC dominance %")
    _total_mcap = Gauge("cmc_provider_total_mcap_usd_t", "Total market cap, trillions USD")
    _total_vol = Gauge("cmc_provider_total_volume_24h_usd_b", "Total 24h volume, billions USD")
    _active_coins = Gauge("cmc_provider_active_cryptocurrencies", "Active crypto count")
    _last_ok_ts = Gauge("cmc_provider_last_ok_ts_ms", "Last successful fetch ts ms")
    _METRICS_OK = True
except Exception:
    _fetches_total = _btc_dom = _total_mcap = _total_vol = _active_coins = _last_ok_ts = None  # type: ignore
    _start_http = None  # type: ignore
    _METRICS_OK = False


def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_global() -> dict | None:
    """GET CoinGecko /api/v3/global. Returns parsed data dict or None on failure."""
    try:
        req = urllib.request.Request(
            API_URL,
            headers={
                "User-Agent": "scanner-cmc-provider/1.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = resp.read().decode("utf-8", "ignore")
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            log.warning("unexpected response type: %s", type(parsed))
            return None
        data = parsed.get("data")
        if not isinstance(data, dict):
            log.warning("missing 'data' key in response")
            return None
        return data
    except Exception as exc:
        log.warning("fetch_global failed: %s", exc)
        return None


def build_snapshot(data: dict, prev_dom_pct: float | None) -> dict:
    """Map CoinGecko /global response to the HASH fields consumers expect."""
    mcp = data.get("market_cap_percentage") or {}
    btc_dom = _safe_float(mcp.get("btc"))

    total_mcap = _safe_float(
        (data.get("total_market_cap") or {}).get("usd")
    )
    total_vol = _safe_float(
        (data.get("total_volume") or {}).get("usd")
    )
    active = int(_safe_float(data.get("active_cryptocurrencies")))

    # btc_dominance_momentum: Δ bps per hour vs previous snapshot
    btc_dom_momentum = 0.0
    if prev_dom_pct is not None and INTERVAL_S > 0:
        delta_bps = (btc_dom - prev_dom_pct) * 100.0
        hours_elapsed = INTERVAL_S / 3600.0
        btc_dom_momentum = delta_bps / max(hours_elapsed, 1e-6)

    now_ms = int(time.time() * 1000)
    return {
        "btc_dominance_pct":        str(round(btc_dom, 6)),
        "total_market_cap_usd":     str(round(total_mcap, 2)),
        "total_volume_24h_usd":     str(round(total_vol, 2)),
        "active_cryptocurrencies":  str(active),
        "btc_dominance_momentum":   str(round(btc_dom_momentum, 6)),
        "ts_ms":                    str(now_ms),
        "source":                   "coingecko",
        "source_url":               API_URL,
    }


def publish(r, snapshot: dict) -> bool:
    try:
        r.hset(REDIS_KEY, mapping=snapshot)
        r.expire(REDIS_KEY, TTL_SEC)
        return True
    except Exception as exc:
        log.error("redis HSET failed: %s", exc)
        return False


def _update_metrics(snapshot: dict) -> None:
    if not _METRICS_OK:
        return
    try:
        _btc_dom.set(float(snapshot["btc_dominance_pct"]))  # type: ignore[union-attr]
        _total_mcap.set(float(snapshot["total_market_cap_usd"]) / 1e12)  # type: ignore[union-attr]
        _total_vol.set(float(snapshot["total_volume_24h_usd"]) / 1e9)  # type: ignore[union-attr]
        _active_coins.set(float(snapshot["active_cryptocurrencies"]))  # type: ignore[union-attr]
        _last_ok_ts.set(float(snapshot["ts_ms"]))  # type: ignore[union-attr]
    except Exception:
        pass


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def run() -> int:
    if _METRICS_OK and _start_http is not None:
        try:
            _start_http(METRICS_PORT)
            log.info("prometheus on :%d", METRICS_PORT)
        except Exception as exc:
            log.warning("metrics server start failed: %s", exc)

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis as _redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r = _redis.from_url(REDIS_URL, decode_responses=True)
    log.info(
        "starting: api=%s key=%s interval=%ds ttl=%ds",
        API_URL, REDIS_KEY, INTERVAL_S, TTL_SEC,
    )

    prev_dom_pct: float | None = None

    while _running:
        data = fetch_global()
        if data is None:
            if _fetches_total is not None:
                _fetches_total.labels("error").inc()  # type: ignore[union-attr]
            log.warning("fetch failed, will retry in %ds", INTERVAL_S)
        else:
            snapshot = build_snapshot(data, prev_dom_pct)
            if publish(r, snapshot):
                btc_dom = float(snapshot["btc_dominance_pct"])
                mcap_t = float(snapshot["total_market_cap_usd"]) / 1e12
                vol_b = float(snapshot["total_volume_24h_usd"]) / 1e9
                active = snapshot["active_cryptocurrencies"]
                log.info(
                    "published: btc_dom=%.2f%% mcap=%.2fT vol=%.2fB active=%s",
                    btc_dom, mcap_t, vol_b, active,
                )
                _update_metrics(snapshot)
                if _fetches_total is not None:
                    _fetches_total.labels("ok").inc()  # type: ignore[union-attr]
                prev_dom_pct = btc_dom
            else:
                if _fetches_total is not None:
                    _fetches_total.labels("publish_error").inc()  # type: ignore[union-attr]

        # Interruptible sleep
        slept = 0
        while _running and slept < INTERVAL_S:
            time.sleep(min(5, INTERVAL_S - slept))
            slept += 5

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
