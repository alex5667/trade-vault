#!/usr/bin/env python3
from __future__ import annotations
"""P58 wrapper: archive signals:of:inputs stream to NDJSON.

Runs ml_analysis.tools.stream_archiver_ndjson_v1 with sensible defaults.
Intended to be called by of_timers_worker (periodic drain) or manually.

Env overrides:
  REDIS_URL
  SIGNAL_STREAM (default: signals:of:inputs)
  SIGNAL_ARCHIVE_DIR (default: /var/lib/trade/archives/signals_of_inputs)
  SIGNAL_ARCHIVER_GROUP (default: sig_archiver_v1)
  MAX_MESSAGES / BATCH / GZIP / etc (passed through via env or CLI)
"""


import os
import sys
from typing import Optional, List

# Ensure we can import from ml_analysis
sys.path.append("/app")

from ml_analysis.tools import stream_archiver_ndjson_v1


def main(argv: Optional[List[str]] = None) -> int:
    os.environ.setdefault("ARCHIVE_STREAM", os.environ.get("SIGNAL_STREAM", "signals:of:inputs"))
    os.environ.setdefault("ARCHIVE_DIR", os.environ.get("SIGNAL_ARCHIVE_DIR", "/var/lib/trade/archives/signals_of_inputs"))
    os.environ.setdefault("ARCHIVER_GROUP", os.environ.get("SIGNAL_ARCHIVER_GROUP", "sig_archiver_v1"))
    os.environ.setdefault("PAYLOAD_FIELD", os.environ.get("SIGNAL_PAYLOAD_FIELD", "payload"))
    # periodic drain default
    os.environ.setdefault("ONCE", "1")
    os.environ.setdefault("BATCH", os.environ.get("SIGNAL_ARCHIVE_BATCH", "2000"))
    os.environ.setdefault("MAX_MESSAGES", os.environ.get("SIGNAL_ARCHIVE_MAX_MESSAGES", "200000"))
    return stream_archiver_ndjson_v1.main(argv)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
