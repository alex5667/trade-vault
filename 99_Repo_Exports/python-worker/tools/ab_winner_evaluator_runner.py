# -*- coding: utf-8 -*-
"""
AB Winner Evaluator Runner (loop inside container).

If you do NOT want systemd on host, run this as a dedicated docker-compose service:
  command: python -m tools.ab_winner_evaluator_runner

It performs:
  - safe-lock (SET NX EX)
  - run evaluator once
  - sleep interval
"""

import os
import asyncio

from tools.ab_winner_evaluator_once import main as run_once


async def runner() -> None:
    interval_sec = int(os.getenv("AB_EVAL_INTERVAL_SEC", "3600"))
    if interval_sec < 60:
        interval_sec = 60
    while True:
        try:
            await run_once()
        except Exception:
            pass
        await asyncio.sleep(interval_sec)


if __name__ == "__main__":
    asyncio.run(runner())
