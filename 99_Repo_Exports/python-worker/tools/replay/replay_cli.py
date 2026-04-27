#!/usr/bin/env python3
"""
CLI для record & replay.

Запуск:
  python -m tools.replay.replay_cli --input ctx.jsonl --output signals.jsonl

или:
  python tools/replay/replay_cli.py --input ctx.jsonl --output signals.jsonl
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from tools.replay.replay_runner import run_replay


def main() -> None:
    ap = argparse.ArgumentParser(description="Record & Replay CLI")
    ap.add_argument("--input", required=True, help="Input ctx.jsonl file")
    ap.add_argument("--output", help="Output signals.jsonl file")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = ap.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )
    logger = logging.getLogger("replay_cli")

    try:
        result = run_replay(
            input_jsonl=args.input,
            logger=logger,
            output_signals_jsonl=args.output,
        )

        print(f"Processed: {result.processed}")
        print(f"Published: {result.published}")

        if args.output:
            print(f"Signals written to: {args.output}")

    except Exception as e:
        logger.exception(f"Replay failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
