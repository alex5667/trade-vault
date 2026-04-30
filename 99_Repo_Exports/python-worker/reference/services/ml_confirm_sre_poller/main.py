"""
Entry point для ml_confirm_sre_poller.
"""

import os
import asyncio
import logging
from prometheus_client import start_http_server
from services.ml_confirm_sre_poller.poller import poll_loop

log = logging.getLogger("ml_confirm_sre_poller")


def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    # Start Prometheus metrics server
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8005"))
    try:
        start_http_server(prometheus_port)
        log.info(f"Prometheus metrics server started on port {prometheus_port}")
    except Exception as e:
        log.warning(f"Failed to start Prometheus server: {e}")
    
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    asyncio.run(poll_loop(redis_url))


if __name__ == "__main__":
    main()

