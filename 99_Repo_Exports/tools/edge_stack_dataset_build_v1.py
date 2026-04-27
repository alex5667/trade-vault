#!/usr/bin/env python3
"""tools.edge_stack_dataset_build_v1

Wrapper around `ml_analysis.tools.build_edge_stack_dataset_from_redis`.

Purpose:
- Produce a recent dataset JSONL + report JSON + inferred feature_cols.json
- Keep the historical `python -m tools.edge_stack_dataset_build_v1` entrypoint stable.

ENV (passed through)
  REDIS_URL

Defaults are conservative and intended for nightly jobs; override via args/env.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def _now_ms() -> int:
    return int(time.time() * 1000)


def _run(cmd: list[str]) -> int:
    p = subprocess.run(cmd, text=True)
    return int(p.returncode)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=float(os.getenv("EDGE_STACK_DATASET_HOURS", "168")))
    ap.add_argument("--max-rows", type=int, default=int(os.getenv("EDGE_STACK_DATASET_MAX_ROWS", "200000")))
    ap.add_argument("--y-min-r", type=float, default=float(os.getenv("EDGE_STACK_Y_MIN_R", "0.5")))
    ap.add_argument("--symbol", type=str, default=os.getenv("EDGE_STACK_SYMBOL", ""))
    ap.add_argument("--source", type=str, default=os.getenv("EDGE_STACK_SOURCE", ""))
    ap.add_argument(
        "--out-dir",
        type=str,
        default=os.getenv("EDGE_STACK_DATASET_OUT_DIR", "/var/lib/trade/of_reports/out/edge_stack/dataset"),
    )
    ap.add_argument("--emit-feature-cols", type=int, default=1)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tag = f"{args.symbol or 'ALL'}_{args.source or 'ALL'}".replace("/", "_")
    ts = _now_ms()
    dataset_path = out_dir / f"edge_stack_dataset_{tag}_{ts}.jsonl"
    report_path = out_dir / f"edge_stack_dataset_report_{tag}_{ts}.json"
    feat_path = out_dir / f"edge_stack_feature_cols_{tag}_{ts}.json" if int(args.emit_feature_cols) == 1 else None

    cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.build_edge_stack_dataset_from_redis",
        "--hours",
        str(args.hours),
        "--max_rows",
        str(args.max_rows),
        "--y_min_r",
        str(args.y_min_r),
        "--out_jsonl",
        str(dataset_path),
        "--out_report_json",
        str(report_path),
    ]
    if args.symbol:
        cmd += ["--symbol", str(args.symbol)]
    if args.source:
        cmd += ["--source", str(args.source)]
    if feat_path is not None:
        cmd += ["--emit_feature_cols_json", str(feat_path)]

    rc = _run(cmd)
    if rc != 0:
        return rc

    print(f"dataset_jsonl={dataset_path}")
    print(f"report_json={report_path}")
    if feat_path is not None:
        print(f"feature_cols_json={feat_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
