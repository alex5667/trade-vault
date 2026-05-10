#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""P58 wrapper: archive trades:closed stream to NDJSON.

Env overrides:
  REDIS_URL
  TRADES_CLOSED_STREAM (default: trades:closed)
  TRADES_CLOSED_ARCHIVE_DIR (default: /var/lib/trade/archives/trades_closed)
  TRADES_CLOSED_ARCHIVER_GROUP (default: closed_archiver_v1)
"""


import os
import sys

# Ensure we can import from ml_analysis
sys.path.append("/app")

from ml_analysis.tools import stream_archiver_ndjson_v1


def main(argv: list[str] | None = None) -> int:
    os.environ.setdefault("ARCHIVE_STREAM", os.environ.get("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED))
    os.environ.setdefault(
        "ARCHIVE_DIR",
        os.environ.get("TRADES_CLOSED_ARCHIVE_DIR", "/var/lib/trade/archives/trades_closed"),
    )
    os.environ.setdefault("ARCHIVER_GROUP", os.environ.get("TRADES_CLOSED_ARCHIVER_GROUP", "closed_archiver_v1"))
    os.environ.setdefault("PAYLOAD_FIELD", os.environ.get("TRADES_CLOSED_PAYLOAD_FIELD", "payload"))
    os.environ.setdefault("ONCE", "1")
    os.environ.setdefault("BATCH", os.environ.get("TRADES_CLOSED_ARCHIVE_BATCH", "2000"))
    os.environ.setdefault("MAX_MESSAGES", os.environ.get("TRADES_CLOSED_ARCHIVE_MAX_MESSAGES", "200000"))
    return stream_archiver_ndjson_v1.main(argv)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
