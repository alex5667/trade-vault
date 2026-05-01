#!/usr/bin/env python3
from __future__ import annotations
"""
P5.7 execution audit-chain scheduler / compose runner.

Purpose
-------
Run the existing P5.6 checker periodically and publish the same SoT artifacts:
- JSON report for runbook/UI consumers
- Prometheus textfile for node_exporter textfile collector

This wrapper keeps the checker logic single-sourced in `check_execution_audit_chain.py`.
It only adds orchestration suitable for:
- docker-compose long-running service
- ad-hoc manual loop execution

Systemd timer should still invoke the checker directly as a one-shot job.
"""

import importlib.util
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional


ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = ROOT / "scripts" / "check_execution_audit_chain.py"

logging.basicConfig(
    level=getattr(logging, os.getenv("EXEC_AUDIT_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("execution_audit_chain_scheduler")


def env_int(name: str, default: int) -> int:
    """Read an integer env var with a fallback default; returns default on parse error."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean env var; recognises 1/true/yes/on as truthy."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def load_checker_module():
    """
    Dynamically load `check_execution_audit_chain` from its file path.
    Ensures single-source-of-truth: no audit logic is duplicated here.
    """
    spec = importlib.util.spec_from_file_location("check_execution_audit_chain", CHECKER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load checker from {CHECKER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# Load once at import time so the scheduler can call CHECKER.main() in the loop.
CHECKER = load_checker_module()


def run_once() -> int:
    """
    Build argv list from ENV vars and delegate to the P5.6 checker's `main()`.
    Returns the checker's exit code (0 = ok, 2 = error).
    """
    argv = [
        "--dsn",
        os.getenv("TRADES_DB_DSN", os.getenv("DATABASE_URL", "")),
        "--lookback-hours",
        str(env_int("EXEC_AUDIT_LOOKBACK_HOURS", 24)),
        "--limit",
        str(env_int("EXEC_AUDIT_LIMIT", 10000)),
        "--report-json",
        os.getenv("EXEC_AUDIT_REPORT_JSON", "latest_execution_audit_chain.json"),
        "--report-prom",
        os.getenv("EXEC_AUDIT_REPORT_PROM", "latest_execution_audit_chain.prom"),
    ]
    return int(CHECKER.main(argv))


def sleep_with_stop(total_seconds: int) -> None:
    """Sleep for *total_seconds* in 1-second slices (easy to interrupt in tests)."""
    deadline = time.time() + max(0, int(total_seconds))
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(1.0, remaining))


def main() -> int:
    """
    Main scheduler loop.

    ENV controls:
    - EXEC_AUDIT_LOOP_RUN_ON_START    1/0 — run on first iteration (default 1)
    - EXEC_AUDIT_LOOP_INTERVAL_SECONDS — sleep between ok runs (default 300)
    - EXEC_AUDIT_LOOP_FAILURE_SLEEP_SECONDS — sleep after failed run (default 30)
    - EXEC_AUDIT_LOOP_MAX_ITERATIONS  — exit after N iterations (0 = forever, default 0)
    - EXEC_AUDIT_LOOP_INITIAL_DELAY_SECONDS — sleep before first run (default 0)
    """
    run_on_start = env_bool("EXEC_AUDIT_LOOP_RUN_ON_START", True)
    interval_seconds = env_int("EXEC_AUDIT_LOOP_INTERVAL_SECONDS", 300)
    failure_sleep_seconds = env_int("EXEC_AUDIT_LOOP_FAILURE_SLEEP_SECONDS", 30)
    max_iterations = env_int("EXEC_AUDIT_LOOP_MAX_ITERATIONS", 0)
    initial_delay_seconds = env_int("EXEC_AUDIT_LOOP_INITIAL_DELAY_SECONDS", 0)

    if initial_delay_seconds > 0:
        logger.info("initial sleep before first run: %ss", initial_delay_seconds)
        sleep_with_stop(initial_delay_seconds)

    iteration = 0
    first = True
    while True:
        iteration += 1
        should_run = run_on_start or not first
        first = False
        rc = 0
        if should_run:
            started = time.time()
            try:
                rc = run_once()
            except Exception:
                logger.exception("execution audit-chain run crashed")
                rc = 2
            elapsed = time.time() - started
            if rc == 0:
                logger.info(
                    "execution audit-chain run ok iteration=%s elapsed=%.3fs", iteration, elapsed
                )
            else:
                logger.warning(
                    "execution audit-chain run failed iteration=%s rc=%s elapsed=%.3fs",
                    iteration,
                    rc,
                    elapsed,
                )
        else:
            logger.info("skipping first run due to EXEC_AUDIT_LOOP_RUN_ON_START=0")

        if max_iterations > 0 and iteration >= max_iterations:
            return rc

        sleep_with_stop(interval_seconds if rc == 0 else failure_sleep_seconds)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
