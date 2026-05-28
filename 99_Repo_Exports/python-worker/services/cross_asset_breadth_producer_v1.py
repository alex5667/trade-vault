"""Cross-Asset Breadth Producer (P1.B, 2026-05-27).

Background
----------
Audit Lane B 2026-05-27: `market_breadth_ret_5m`, `cg_rel_strength_btc_1h`,
`symbol_rel_strength_vs_btc_1m` имеют coverage 0/514 в payload. HTF_LONG_BIAS
gate работает в режиме fail-open потому что нечем считать.

What this producer does
-----------------------
Periodically (every BREADTH_INTERVAL_SEC) сканирует kline streams нескольких
top-symbols, считает aggregate breadth metrics + per-symbol relative strength
к BTC, и пишет в Redis HASH `ctx:breadth:global` и `ctx:breadth:{SYMBOL}`.

Read path
---------
Existing v14_of feature_bridge / external_features_payload_v1 — для интеграции
нужен side patch чтобы вычитывать `ctx:breadth:*` ключи в indicators. Сейчас
producer публикует, ниже добавляется отдельный reader-helper `breadth_reader.py`
+ wire в payload builder.

Computed metrics
----------------
  ctx:breadth:global  HASH:
    market_breadth_ret_5m       — fraction of symbols with ret_5m > 0 (−1..1: 2*frac-1)
    n_symbols                   — count active
    ts_ms

  ctx:breadth:{SYMBOL}  HASH:
    btc_ret_5m                  — BTC 5min return (mirror)
    btc_ret_1m                  — BTC 1min return
    symbol_rel_strength_vs_btc_1m  — (sym_ret_1m - btc_ret_1m)
    cg_rel_strength_btc_1h      — sym_ret_1h - btc_ret_1h (BTCUSDT excluded)
    ts_ms

ENV:
  BREADTH_PRODUCER_ENABLED      default 1
  BREADTH_INTERVAL_SEC          default 30
  BREADTH_SYMBOLS               CSV; default scan stream:kline_1m:* (top 30)
  BREADTH_MAX_SYMBOLS           default 30
  BREADTH_PROM_PORT             default 9876
  BREADTH_REDIS_URL             fallback REDIS_URL
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time
from typing import Any

logger = logging.getLogger("cross_asset_breadth_producer")

_GLOBAL_KEY = "ctx:breadth:global"
_PER_SYM_PREFIX = "ctx:breadth:"

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _cycles_total = Counter(
        "breadth_producer_cycles_total",
        "Producer cycles",
        ["result"],
    )
    _last_cycle_ms = Gauge(
        "breadth_producer_last_cycle_ms",
        "Last cycle epoch ms",
    )
    _active_syms = Gauge(
        "breadth_producer_active_symbols",
        "Symbols with valid data this cycle",
    )
    _market_breadth_g = Gauge(
        "breadth_producer_market_breadth_ret_5m",
        "Market breadth -1..1 (last computation)",
    )
except Exception:
    Counter = Gauge = start_http_server = None  # type: ignore[assignment,misc]
    _cycles_total = _last_cycle_ms = _active_syms = _market_breadth_g = None  # type: ignore[assignment]


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
        os.environ.get("BREADTH_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )
    return redis.from_url(url, decode_responses=True, socket_timeout=2.0)


def _read_shared_klines(rc: Any, n_fetch: int = 5000) -> dict[str, list[dict[str, float]]]:
    """Read recent N entries from shared `candles:data` stream; group per symbol.

    Schema (verified 2026-05-27):
      tf=1m  ts=<ms>  symbol=<SYM>  payload=<json{open,high,low,close,...}>

    Returns map sym → list of klines (oldest → newest) for tf=1m only.
    """
    key = os.environ.get("BREADTH_SHARED_STREAM_KEY", "candles:data")
    per_sym: dict[str, list[dict[str, float]]] = {}
    try:
        rows = rc.xrevrange(key, count=n_fetch)
    except Exception as e:
        logger.debug("breadth: shared XRANGE fail: %s", e)
        return per_sym
    if not rows:
        return per_sym
    for _id, fields in rows:
        if not isinstance(fields, dict):
            continue
        tf = str(fields.get("tf") or "").strip()
        if tf and tf != "1m":
            continue
        sym = str(fields.get("symbol") or "").strip().upper()
        if not sym or not sym.endswith("USDT"):
            continue
        payload_raw = fields.get("payload")
        if not payload_raw:
            continue
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            if not isinstance(payload, dict):
                continue
            kl = {
                "open_ms": float(payload.get("openTime") or payload.get("open_time") or fields.get("ts") or 0),
                "close": float(payload.get("close") or payload.get("c") or 0),
                "open": float(payload.get("open") or payload.get("o") or 0),
                "high": float(payload.get("high") or payload.get("h") or 0),
                "low": float(payload.get("low") or payload.get("l") or 0),
            }
            if kl["close"] > 0 and kl["open"] > 0:
                per_sym.setdefault(sym, []).append(kl)
        except Exception:
            continue
    # reverse each (we collected newest→oldest, need oldest→newest)
    # AND bucket by minute — Go ingester writes update events with wall-clock
    # open_ms; canonical 1m bar = floor(open_ms / 60000). Last write per bucket
    # gives the most-recent close known to that minute.
    for s in list(per_sym.keys()):
        seen: dict[int, dict[str, float]] = {}
        for kl in per_sym[s]:
            try:
                raw_ts = float(kl.get("open_ms") or 0)
            except Exception:
                continue
            if raw_ts <= 0:
                continue
            minute_bucket = int(raw_ts // 60_000)
            # last write wins (preserves closing snapshot for the minute)
            seen[minute_bucket] = kl
        ordered = [seen[b] for b in sorted(seen.keys())]
        per_sym[s] = ordered
    return per_sym


def _pct_ret(start_close: float, end_close: float) -> float:
    if start_close <= 0 or end_close <= 0:
        return 0.0
    r = (end_close - start_close) / start_close
    if not math.isfinite(r):
        return 0.0
    return float(r)


def _compute_returns(klines: list[dict[str, float]]) -> dict[str, float]:
    """Compute ret_1m / ret_5m / ret_1h from a sequence (oldest→newest, 1m bars)."""
    out = {"ret_1m": 0.0, "ret_5m": 0.0, "ret_1h": 0.0, "n": len(klines)}
    if len(klines) < 2:
        return out
    last_close = klines[-1]["close"]
    if last_close <= 0:
        return out
    # ret_1m: last vs previous
    if len(klines) >= 2:
        out["ret_1m"] = _pct_ret(klines[-2]["close"], last_close)
    # ret_5m: last vs 5 bars ago
    if len(klines) >= 6:
        out["ret_5m"] = _pct_ret(klines[-6]["close"], last_close)
    # ret_1h: last vs 60 bars ago (best-effort; uses oldest if <60)
    look_back = min(60, len(klines) - 1)
    out["ret_1h"] = _pct_ret(klines[-look_back - 1]["close"], last_close)
    return out


def _write_global(rc: Any, market_breadth: float, n: int) -> None:
    try:
        rc.hset(_GLOBAL_KEY, mapping={
            "market_breadth_ret_5m": str(round(market_breadth, 6)),
            "n_symbols": str(n),
            "ts_ms": str(_now_ms()),
            "source": "cross_asset_breadth_producer",
        })
        rc.expire(_GLOBAL_KEY, 600)
    except Exception as e:
        logger.debug("breadth: write_global fail: %s", e)


def _write_per_symbol(
    rc: Any, sym: str, sym_rets: dict[str, float], btc_rets: dict[str, float],
) -> None:
    sym_u = sym.upper()
    rel_1m = sym_rets["ret_1m"] - btc_rets["ret_1m"]
    rel_1h = sym_rets["ret_1h"] - btc_rets["ret_1h"]
    mapping = {
        "btc_ret_5m": str(round(btc_rets["ret_5m"], 8)),
        "btc_ret_1m": str(round(btc_rets["ret_1m"], 8)),
        "btc_ret_1h": str(round(btc_rets["ret_1h"], 8)),
        "sym_ret_1m": str(round(sym_rets["ret_1m"], 8)),
        "sym_ret_5m": str(round(sym_rets["ret_5m"], 8)),
        "sym_ret_1h": str(round(sym_rets["ret_1h"], 8)),
        "symbol_rel_strength_vs_btc_1m": str(round(rel_1m, 8)),
        "cg_rel_strength_btc_1h": str(round(rel_1h, 8)),
        "ts_ms": str(_now_ms()),
        "source": "cross_asset_breadth_producer",
    }
    try:
        rc.hset(_PER_SYM_PREFIX + sym_u, mapping=mapping)
        rc.expire(_PER_SYM_PREFIX + sym_u, 600)
    except Exception as e:
        logger.debug("breadth: write_per_symbol fail %s: %s", sym, e)


def _main_loop() -> int:
    if not _env_bool("BREADTH_PRODUCER_ENABLED", True):
        logger.info("breadth: disabled")
        return 0

    interval = max(10, _env_int("BREADTH_INTERVAL_SEC", 30))
    max_syms = max(1, _env_int("BREADTH_MAX_SYMBOLS", 30))
    csv_syms = (os.environ.get("BREADTH_SYMBOLS") or "").strip()

    rc = _redis()

    if start_http_server is not None:
        try:
            start_http_server(_env_int("BREADTH_PROM_PORT", 9876))
        except Exception as e:
            logger.warning("prom server fail: %s", e)

    stop = {"flag": False}

    def _sig(_s, _f):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "breadth producer started: interval=%ds max_syms=%d csv_syms=%s",
        interval, max_syms, csv_syms or "(scan)",
    )

    fetch_n = max(500, _env_int("BREADTH_SHARED_STREAM_FETCH", 5000))
    while not stop["flag"]:
        try:
            # Pull all recent 1m klines from shared `candles:data` and bucket
            # by symbol. Producers (Go ingester) emit ~1 entry/min/symbol.
            per_sym_kls = _read_shared_klines(rc, n_fetch=fetch_n)
            if csv_syms:
                allowed = {s.strip().upper() for s in csv_syms.split(",") if s.strip()}
                per_sym_kls = {s: v for s, v in per_sym_kls.items() if s in allowed}
            else:
                # Limit cardinality (pick top by data volume).
                if len(per_sym_kls) > max_syms:
                    top = sorted(per_sym_kls.items(), key=lambda x: len(x[1]), reverse=True)[:max_syms]
                    per_sym_kls = dict(top)

            # candles:data shared stream MAXLEN=10000 → ~1-2 min history per
            # 250+ symbols. ret_1m нужен минимум 2 bars; ret_5m появится после
            # 6+ minutes накопления. ret_1h тоже appears after 60 min.
            min_bars = max(2, _env_int("BREADTH_MIN_BARS", 2))
            btc_kls = per_sym_kls.get("BTCUSDT") or []
            btc_rets = _compute_returns(btc_kls)
            if btc_rets["n"] < min_bars:
                if _cycles_total is not None:
                    _cycles_total.labels(result="btc_no_data").inc()
                logger.debug("breadth: BTC kline data insufficient (n=%d)", btc_rets["n"])

            positives = 0
            actives = 0
            for sym, kls in per_sym_kls.items():
                rets = _compute_returns(kls)
                if rets["n"] < min_bars:
                    continue
                if sym == "BTCUSDT":
                    _write_per_symbol(rc, sym, btc_rets, btc_rets)
                else:
                    _write_per_symbol(rc, sym, rets, btc_rets)
                if rets["ret_5m"] > 0:
                    positives += 1
                actives += 1
            # 3) Market breadth = 2 * (positives / total) - 1, range [-1..1]
            market_breadth = 0.0
            if actives > 0:
                market_breadth = 2.0 * (positives / actives) - 1.0
            _write_global(rc, market_breadth, actives)

            if _last_cycle_ms is not None:
                _last_cycle_ms.set(float(_now_ms()))
            if _active_syms is not None:
                _active_syms.set(float(actives))
            if _market_breadth_g is not None:
                _market_breadth_g.set(float(market_breadth))
            if _cycles_total is not None:
                _cycles_total.labels(result="ok").inc()
            logger.info(
                "breadth cycle: syms=%d actives=%d positives=%d breadth=%.3f btc_ret_5m=%.4f%%",
                len(per_sym_kls), actives, positives, market_breadth, btc_rets["ret_5m"] * 100,
            )
        except Exception as e:
            logger.warning("breadth cycle error: %s", e)
            if _cycles_total is not None:
                try:
                    _cycles_total.labels(result="error").inc()
                except Exception:
                    pass

        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1.0)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    sys.exit(_main_loop())
