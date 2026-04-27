#!/usr/bin/env python3
"""export_replay_inputs_ndjson_v1.py

Phase2 helper: export a time-window slice of replay inputs archive into a single NDJSON.

This is intended to feed ml_analysis/tools/build_dataset_from_inputs_outcomes_v2.py.

Input source:
  - archive directory produced by ml_analysis/tools/replay_inputs_archiver.py
    (typically archives of the signals:of:inputs stream).

Output format:
  - Each line is a JSON dict (payload) containing keys like sid, ts_ms, symbol, direction, ...

Usage:
  python3 ml_analysis/tools/export_replay_inputs_ndjson_v1.py \
    --archive-dir /var/lib/trade/replay_inputs_archives \
    --start-ts-ms 1700000000000 --end-ts-ms 1700864000000 \
    --out /tmp/inputs.ndjson

Env:
  - None
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
from typing import Any, Dict, Optional

from ml_analysis.tools.replay_inputs_reader_v1 import ReplayInputsReader


def _open_out(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", encoding="utf-8")
    return open(path, "w", encoding="utf-8")


def _as_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    # Archive reader already parses JSON lines.
    # Some variants may store payload as a JSON string.
    v = obj.get("payload")
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.lstrip().startswith("{"):
        try:
            d = json.loads(v)
            return d if isinstance(d, dict) else obj
        except Exception:
            return obj
    return obj


def _sym_from_payload(p: Dict[str, Any]) -> str:
    s = str(p.get("symbol") or "").upper().strip()
    return s


def main() -> None:
    ap = argparse.ArgumentParser(description="Export replay inputs NDJSON slice")
    ap.add_argument("--archive-dir", required=True, help="archive directory (from replay_inputs_archiver)")
    ap.add_argument("--start-ts-ms", type=int, required=True, help="start timestamp in ms (inclusive)")
    ap.add_argument("--end-ts-ms", type=int, required=True, help="end timestamp in ms (exclusive)")
    ap.add_argument("--out", required=True, help="output ndjson path (optionally .gz)")
    ap.add_argument("--symbol", default="", help="optional symbol filter (e.g. BTCUSDT)")
    ap.add_argument("--max-records", type=int, default=0, help="optional cap (0=unlimited)")

    args = ap.parse_args()

    archive_dir = str(args.archive_dir)
    if not os.path.isdir(archive_dir):
        raise SystemExit(f"archive-dir not found: {archive_dir}")

    sym_filter = str(args.symbol or "").upper().strip()
    nmax = int(args.max_records or 0)

    reader = ReplayInputsReader(archive_dir=archive_dir)

    out_path = str(args.out)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    n = 0
    n_written = 0

    with _open_out(out_path) as f:
        for rec in reader.read_records(start_ts_ms=int(args.start_ts_ms), end_ts_ms=int(args.end_ts_ms)):
            if not isinstance(rec, dict):
                continue
            n += 1
            payload = _as_payload(rec)
            if sym_filter:
                if _sym_from_payload(payload) != sym_filter:
                    continue
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            n_written += 1
            if nmax and n_written >= nmax:
                break

    print(json.dumps({
        "archive_dir": archive_dir,
        "start_ts_ms": int(args.start_ts_ms),
        "end_ts_ms": int(args.end_ts_ms),
        "symbol": sym_filter,
        "scanned": n,
        "written": n_written,
        "out": out_path,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
