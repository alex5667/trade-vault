import logging
import os
import sys

# Ensure we can import from the root if needed
sys.path.append(os.getcwd())

from prometheus_client import start_http_server

from services.signal_outbox_dispatcher import SignalDispatcher

logger = logging.getLogger(__name__)


def _start_metrics_server() -> None:
    """Expose dispatcher_* metrics for Prometheus scrape.

    Without this, dispatcher_schema_version_total (and the rest of the
    SignalDispatcher Counter/Gauge/Histogram set) are created in-process
    but never reachable — Prometheus returns 0 series, and the v1→v2
    cutoff criterion cannot be observed.

    Port: OUTBOX_ROUTER_METRICS_PORT (default 9836). Bind: 0.0.0.0.
    """
    port = int(os.getenv("OUTBOX_ROUTER_METRICS_PORT", "9836"))
    try:
        start_http_server(port)
        logger.info("📊 signal-outbox-router metrics on :%d/metrics", port)
    except OSError as e:
        # Best-effort: do not block dispatcher startup if port is taken.
        logger.error("⚠️ Failed to start metrics server on :%d: %s", port, e)


if __name__ == "__main__":
    _start_metrics_server()
    SignalDispatcher().run()
