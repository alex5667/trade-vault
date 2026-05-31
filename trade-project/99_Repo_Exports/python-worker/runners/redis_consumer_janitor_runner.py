"""Runner for Redis Consumer Group Janitor.

Точка запуска: python -m runners.redis_consumer_janitor_runner

Запускает Prometheus HTTP server на JANITOR_PROMETHEUS_PORT (default 9872),
затем выполняет sweep-цикл через JanitorConfig.
"""

from __future__ import annotations

import logging
import os
import sys

# --- Logging setup must happen before any local imports ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("janitor-runner")


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def main() -> None:
    prom_port = _env_int("JANITOR_PROMETHEUS_PORT", 9872)

    # Start Prometheus HTTP server (fail-soft if prometheus_client unavailable)
    try:
        from prometheus_client import start_http_server
        start_http_server(prom_port)
        logger.info("Prometheus metrics server started on port %d", prom_port)
    except Exception as e:
        logger.warning("Could not start Prometheus server: %s", e)

    from services.redis_consumer_janitor import JanitorConfig, run_loop

    config = JanitorConfig.from_env()
    run_loop(config)


if __name__ == "__main__":
    main()
