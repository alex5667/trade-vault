#!/usr/bin/env python3
"""P58: Build edge-stack dataset (JSONL) with archive fallback.

This is a light wrapper around:
  ml_analysis.tools.build_edge_stack_dataset_from_redis

Env:
  REDIS_URL
  SIGNAL_STREAM (default: signals:of:inputs)
  TRADES_CLOSED_STREAM (default: trades:closed)

  SIGNALS_ARCHIVE_DIR (default: /var/lib/trade/archives/signals_of_inputs)
  TRADES_CLOSED_ARCHIVE_DIR (default: /var/lib/trade/archives/trades_closed)

  EDGE_DATASET_DIR (default: /var/lib/trade/ml_datasets/edge_stack_v1)
  TRAIN_WINDOW_HOURS (default: 72)
  SIGNALS_COUNT / CLOSES_COUNT (caps for Redis read)
  FILE_MAX_RECORDS / ARCHIVE_LOOKBACK_DAYS (caps for file fallback)

Output:
  EDGE_DATASET_DIR/YYYYMMDD_HHMM/latest.jsonl + report.json + feature_cols.json
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, List

# Ensure we can import from ml_analysis
sys.path.append("/app")

from ml_analysis.tools import build_edge_stack_dataset_from_redis as builder


def _now_ms() -> int:
    return get_ny_time_millis()


def main(argv: Optional[List[str]] = None) -> int:
    dataset_root = Path(os.getenv("EDGE_DATASET_DIR", "/var/lib/trade/ml_datasets/edge_stack_v1")).expanduser()
    dataset_root.mkdir(parents=True, exist_ok=True)

    window_h = int(os.getenv("TRAIN_WINDOW_HOURS", "72"))
    end_ms = _now_ms()
    start_ms = end_ms - window_h * 3600 * 1000

    ts_tag = time.strftime("%Y%m%d_%H%M", time.gmtime(end_ms / 1000))
    out_dir = dataset_root / ts_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    out_jsonl = str(out_dir / "latest.jsonl")
    out_report = str(out_dir / "report.json")
    out_cols = str(out_dir / "feature_cols.json")
    out_quarantine = str(out_dir / "quarantine.jsonl")

    args = [
        "--redis_url",
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        "--signal_stream",
        os.getenv("SIGNAL_STREAM", "signals:of:inputs"),
        "--closed_stream",
        os.getenv("TRADES_CLOSED_STREAM", "trades:closed"),
        "--signals_count",
        os.getenv("SIGNALS_COUNT", "200000"),
        "--closes_count",
        os.getenv("CLOSES_COUNT", "200000"),
        "--since_ms",
        str(start_ms),
        "--until_ms",
        str(end_ms),
        "--signal_archive_dir",
        os.getenv("SIGNALS_ARCHIVE_DIR", "/var/lib/trade/archives/signals_of_inputs"),
        "--closed_archive_dir",
        os.getenv("TRADES_CLOSED_ARCHIVE_DIR", "/var/lib/trade/archives/trades_closed"),
        "--file_fallback",
        os.getenv("FILE_FALLBACK", "1"),
        "--archive_lookback_days",
        os.getenv("ARCHIVE_LOOKBACK_DAYS", "7"),
        "--file_max_records",
        os.getenv("FILE_MAX_RECORDS", "500000"),
        "--out_jsonl",
        out_jsonl,
        "--out_report_json",
        out_report,
        "--emit_feature_cols_json",
        out_cols,
        "--out_quarantine_jsonl",
        out_quarantine,
    ]

    rc = builder.main(args)
    # keep a small pointer file for downstream scripts
    (dataset_root / "LATEST_DIR").write_text(str(out_dir), encoding="utf-8")
    print(json.dumps({"ok": True, "out_dir": str(out_dir), "rc": int(rc)}, ensure_ascii=False))
    return int(rc)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
