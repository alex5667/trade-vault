"""Entry point for scanner-gate-value-reporter.

Supports two modes:
  - default: long-running periodic loop (run_loop)
  - --once : run a single cycle, print report to stdout, exit
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

from prometheus_client import start_http_server
from redis.asyncio import Redis

from services.gate_value_reporter.reporter import run_loop, run_once

log = logging.getLogger("gate_value_reporter")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="gate_value_reporter")
    p.add_argument(
        "--once",
        action="store_true",
        help="Run a single report cycle and exit (CLI/replay mode).",
    )
    p.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Override GATE_VALUE_LOOKBACK_HOURS for one-shot mode.",
    )
    return p.parse_args(argv)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def _once(redis_url: str, lookback_hours: int | None) -> int:
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        report = await run_once(client, lookback_hours=lookback_hours)
    finally:
        try:
            await client.aclose()
        except Exception:
            pass
    json.dump(report, sys.stdout, separators=(",", ":"), default=str)
    sys.stdout.write("\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    args = _parse_args(argv)
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

    enabled = os.getenv("GATE_VALUE_REPORTER_ENABLED", "1").strip()
    if enabled in {"0", "false", "False", ""}:
        log.warning("GATE_VALUE_REPORTER_ENABLED=0 — exiting")
        return 0

    if args.once:
        return asyncio.run(_once(redis_url, args.lookback_hours))

    port = int(os.getenv("GATE_VALUE_REPORTER_PORT", "9141"))
    try:
        start_http_server(port)
        log.info("Prometheus metrics server started on port %d", port)
    except Exception as e:
        log.warning("Failed to start Prometheus server on %d: %s", port, e)

    asyncio.run(run_loop(redis_url))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
