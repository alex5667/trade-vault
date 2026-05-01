# python-worker/tools/nightly_meta_pipeline_v2.py
from __future__ import annotations
"""
Unified nightly pipeline (v2): train -> quality report (regime-aware) -> auto-ramp (regime-aware).

This script is opt-in: use it by switching your timer/compose entrypoint from nightly_meta_pipeline_v1.py.
Defaults keep backward compatibility: schema defaults to meta_feat_v3 and auto-apply is off.
"""

import argparse
import os
import subprocess
import sys
from typing import List


def _run(cmd: List[str]) -> None:
    print("[cmd]", " ".join(cmd))
    subprocess.check_call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-parquet", required=True)
    ap.add_argument("--label-col", default=os.environ.get("META_LABEL_COL", "y"))
    ap.add_argument("--schema", default=os.environ.get("META_SCHEMA", "meta_feat_v3"))
    ap.add_argument("--out-model-json", required=True)
    ap.add_argument("--out-report-json", required=True)
    ap.add_argument("--topk", default=os.environ.get("META_REPORT_TOPK", "200"))
    ap.add_argument("--ece-bins", default=os.environ.get("META_REPORT_ECE_BINS", "10"))
    ap.add_argument("--group-cols", default=os.environ.get("META_REPORT_GROUP_COLS", "regime_bucket,session_bucket"))
    ap.add_argument("--min-group-n", default=os.environ.get("META_REPORT_MIN_GROUP_N", "200"))
    ap.add_argument("--apply-ramp", type=int, default=int(os.environ.get("META_RAMP_APPLY", "0")))
    ap.add_argument("--ramp-ignore-guard", type=int, default=int(os.environ.get("META_RAMP_IGNORE_GUARD", "0")))
    args = ap.parse_args()

    # 1) Train (existing v4 trainer)
    _run([
        sys.executable, "python-worker/tools/train_meta_model_lr_v4.py",
        "--in-parquet", args.in_parquet,
        "--out-json", args.out_model_json,
        "--schema", args.schema,
        "--label-col", args.label_col,
    ])

    # 2) Report v2 (regime-aware)
    prom_textfile = os.environ.get("META_REPORT_PROM_TEXTFILE", "")
    cmd = [
        sys.executable, "python-worker/tools/meta_model_quality_report_v2.py",
        "--model-json", args.out_model_json,
        "--dataset-parquet", args.in_parquet,
        "--label-col", args.label_col,
        "--group-cols", args.group_cols,
        "--min-group-n", str(args.min_group_n),
        "--topk", str(args.topk),
        "--ece-bins", str(args.ece_bins),
        "--out-json", args.out_report_json,
    ]
    if prom_textfile:
        cmd += ["--prom-textfile", prom_textfile]
    _run(cmd)

    # 3) Auto-ramp v2 (optional)
    if int(args.apply_ramp) == 1:
        _run([
            sys.executable, "python-worker/tools/meta_auto_ramp_v2.py",
            "--report-json", args.out_report_json,
            "--apply", "1",
            "--ignore-guard", str(int(args.ramp_ignore_guard)),
        ])


if __name__ == "__main__":
    main()
