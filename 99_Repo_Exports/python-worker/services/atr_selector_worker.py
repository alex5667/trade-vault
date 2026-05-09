import os
import time

import redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.log import setup_logger
from core.atr_source_selector_v2 import ATRSourceSelector

logger = setup_logger("ATRSelectorWorker")

# Prometheus Metrics
ATR_SEL_RUN_DURATION = Histogram(
    "atr_selector_run_duration_seconds",
    "Duration of ATR selection loop in seconds"
)
ATR_SEL_SYMBOLS_PROCESSED = Gauge(
    "atr_selector_symbols_processed",
    "Number of symbols processed in last run"
)
ATR_SEL_AGE_MS = Gauge(
    "atr_selector_picked_age_ms",
    "Age of the picked ATR source",
    ["symbol", "tf", "src"]
)
ATR_SEL_BPS = Gauge(
    "atr_selector_picked_bps",
    "BPS value of the picked ATR source",
    ["symbol", "tf", "src"]
)
ATR_SEL_SWITCH_TOTAL = Counter(
    "atr_selector_switch_total",
    "Total number of ATR source/TF switches detected",
    ["symbol"]
)
ATR_SEL_ERROR_TOTAL = Counter(
    "atr_selector_error_total",
    "Total number of errors in ATR selector loop",
    ["type"]
)


def main() -> None:
    prometheus_port = int(os.getenv("ATR_SELECTOR_PROMETHEUS_PORT", "9842"))
    try:
        start_http_server(prometheus_port)
        logger.info(f"📊 Prometheus metrics started on port {prometheus_port}")
    except Exception as e:
        logger.error(f"❌ Failed to start Prometheus server: {e}")

    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(url, decode_responses=False)
    sel = ATRSourceSelector(r)

    period = int(os.getenv("ATR_SELECTOR_PERIOD_SEC", "300"))
    price_prefix = os.getenv("ATR_SELECTOR_PRICE_KEY_PREFIX", "cfg:last_px:")
    sym_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")

    logger.info(f"🚀 ATR Selector Worker started. Period: {period}s")

    while True:
        t0 = time.time()
        if os.getenv("ATR_SELECTOR_ENABLE", "0") == "1":
            try:
                syms = list(r.smembers(sym_set) or [])
                ATR_SEL_SYMBOLS_PROCESSED.set(len(syms))

                for s in syms:
                    sym = s.decode("utf-8", "ignore") if isinstance(s, bytes) else str(s)
                    px_raw = r.get(price_prefix + sym)
                    try:
                        px = float(px_raw.decode("utf-8", "ignore") if isinstance(px_raw, bytes) else px_raw or 0.0)
                    except Exception:
                        px = 0.0

                    if px > 0:
                        res = sel.select(sym, px=px)
                        if res:
                            # Update metrics for current selection
                            ATR_SEL_AGE_MS.labels(symbol=sym, tf=res.tf, src=res.src).set(res.age_ms)
                            ATR_SEL_BPS.labels(symbol=sym, tf=res.tf, src=res.src).set(res.atr_bps)

                            # Check for switch (we can use sel._persist_choice logic or just track locally if needed)
                            # For simplicity, we rely on the counter incremented inside sel._persist_choice
                            # if we want exact match, but here we can add another label-based counter.
            except Exception as e:
                logger.error(f"Error in selector loop: {e}")
                ATR_SEL_ERROR_TOTAL.labels(type="loop").inc()

        dt = time.time() - t0
        ATR_SEL_RUN_DURATION.observe(dt)
        time.sleep(max(1, period - int(dt)))


if __name__ == "__main__":
    main()

