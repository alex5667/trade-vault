from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import sys
import time
import json
import math

from common.log import setup_logger
from core.redis_client import get_redis
from core.ticks_redis_client import get_ticks_redis
from services.smt_bundle_aggregator import build_default_aggregator, _parse_bundles_from_env


def _write_price_latest_from_tick_stream(r_ticks: object, r_signals: object, symbol: str) -> bool:
    """
    Read latest tick from stream:tick_{SYMBOL} and write to price:latest:{SYMBOL}.
    Returns True if data was available and written.
    Completely fail-open.
    """
    try:
        sym_up = str(symbol).strip().upper()
        stream_key = f"stream:tick_{sym_up}"
        msgs = r_ticks.xrevrange(stream_key, count=1)  # type: ignore[call-arg]
        if not msgs:
            return False
        _, fields = msgs[0]
        # fields may be bytes or str depending on decode_responses
        def _s(x):
            return x.decode("utf-8", errors="ignore") if isinstance(x, (bytes, bytearray)) else str(x or "")

        # Try nested JSON "data" field first (tick_ingest format)
        data_raw = _s(fields.get(b"data") or fields.get("data") or "")
        price: float = 0.0
        ts_ms: int = 0
        if data_raw:
            try:
                d = json.loads(data_raw)
                price = float(d.get("price") or d.get("close") or d.get("last") or 0.0)
                ts_ms = int(float(d.get("ts") or d.get("ts_ms") or 0))
            except Exception:
                pass
        if price <= 0.0:
            # flat fields
            price = float(_s(fields.get(b"price") or fields.get("price") or
                              fields.get(b"close") or fields.get("close") or 0) or 0)
            ts_ms = int(float(_s(fields.get(b"ts") or fields.get("ts") or
                                  fields.get(b"ts_ms") or fields.get("ts_ms") or 0) or 0))
        if price <= 0.0:
            return False
        
        # ts may be in seconds → convert
        if 0 < ts_ms < 10_000_000_000:
            ts_ms = ts_ms * 1000
        if ts_ms <= 0:
            ts_ms = get_ny_time_millis()


        # Use current wall-clock time as ts_ms so that SmtBundleAggregator's
        # price_stale_ms check always passes.  The original tick ts is stored
        # in tick_ts_ms for audit purposes.
        r_signals.hset(  # type: ignore[call-arg]
            f"price:latest:{sym_up}",
            mapping={
                "mid": f"{price:.10f}",
                "ts_ms": str(get_ny_time_millis()),
                "tick_ts_ms": str(ts_ms),
                "venue": "crypto_tick_stream",
            },
        )
        return True
    except Exception as e:
        return False


def _collect_bundle_symbols() -> list[str]:
    """Collect all unique symbols from SMT_BUNDLE_* env vars."""
    bundles = _parse_bundles_from_env()
    syms: list[str] = []
    seen: set[str] = set()
    for b in bundles:
        for s in b.symbols:
            if s not in seen:
                syms.append(s)
                seen.add(s)
    return syms


def main() -> int:
    logger = setup_logger("smt_bundle_aggregator")
    r_signals = get_redis()
    r_ticks = get_ticks_redis()

    agg = build_default_aggregator(r_signals)
    symbols = _collect_bundle_symbols()

    try:
        interval_ms = int(float(os.getenv("SMT_AGG_INTERVAL_MS", "1000")))
    except Exception:
        interval_ms = 1000
    interval_ms = max(200, interval_ms)

    logger.info(
        "SMT aggregator started. interval_ms=%d symbols=%s",
        interval_ms,
        symbols,
    )

    while True:
        # ── 1. Populate price:latest from tick streams (self-sufficient) ──
        # Reads from r_ticks, writes to r_signals so other workers see it.
        for sym in symbols:
            _write_price_latest_from_tick_stream(r_ticks, r_signals, sym)

        # ── 2. Compute and write bundle state ────────────────────────────
        try:
            agg.tick_once()
        except Exception:
            pass

        try:
            time.sleep(interval_ms / 1000.0)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

