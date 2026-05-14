"""Entry point for scanner-gated-out-outcome-tracker."""

import asyncio
import logging
import os

from prometheus_client import start_http_server

from services.gated_out_outcome_tracker.tracker import run

log = logging.getLogger("gated_out_outcome_tracker")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    port = int(os.getenv("GATED_OUT_TRACKER_PROMETHEUS_PORT", "9137"))
    try:
        start_http_server(port)
        log.info("Prometheus metrics server started on port %d", port)
    except Exception as e:
        log.warning("Failed to start Prometheus server on %d: %s", port, e)

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    asyncio.run(run(redis_url))


if __name__ == "__main__":
    main()
