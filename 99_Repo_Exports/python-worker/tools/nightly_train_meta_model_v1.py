#!/usr/bin/env python3
from __future__ import annotations
"""Nightly trainer wrapper for MetaModelLR (meta-labeling).

Goal:
  - Train MetaModelLR JSON using train_meta_model_lr_v3.py (schema meta_feat_v1)
  - Write output atomically (tmp -> os.replace)
  - Fail-open: if dataset too small, keep current model and exit 0 (default)
  - Produce a small sidecar meta file with dataset stats and model hash.

This script is intended to be called by cron/systemd timer / CI runner.
It does NOT touch Redis streams to avoid traffic duplication.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass
class DatasetStats:
    n_rows: int
    n_pos: int
    n_neg: int


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_label_stats(parquet_path: Path, label_col: str) -> DatasetStats:
    # Minimal read: only label column.
    import pandas as pd  # local import: training env only

    df = pd.read_parquet(str(parquet_path), columns=[label_col])
    y = df[label_col].astype(int)
    n = int(len(y))
    n_pos = int((y == 1).sum())
    n_neg = int(n - n_pos)
    return DatasetStats(n_rows=n, n_pos=n_pos, n_neg=n_neg)


def _atomic_replace(src_tmp: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(src_tmp), str(dst))


def _trainer_path() -> Path:
    # .../python-worker/tools/nightly_train_meta_model_v1.py -> python-worker
    py_worker = Path(__file__).resolve().parents[1]
    return py_worker / "tools" / "train_meta_model_lr_v4.py"


def _run_trainer(
    trainer: Path,
    parquet_path: Path,
    label_col: str,
    out_json_tmp: Path,
    C: float,
    max_iter: int,
    self_check: bool,
    self_check_n: int,
    schema: str,
) -> Tuple[int, str]:
    cmd = [
        sys.executable,
        "-u",
        str(trainer),
        "--in-parquet",
        str(parquet_path),
        "--label-col",
        str(label_col),
        "--out-json",
        str(out_json_tmp),
        "--schema",
        str(schema),
        "--C",
        str(C),
        "--max-iter",
        str(max_iter),
        "--self-check",
        "1" if self_check else "0",
        "--self-check-n",
        str(int(self_check_n)),
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return int(p.returncode), str(p.stdout or "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-parquet", required=True, help="Parquet dataset for meta LR training")
    ap.add_argument("--label-col", required=True, help="Binary label column")
    ap.add_argument("--out-json", required=True, help="Output champion MetaModelLR JSON path")
    ap.add_argument("--out-meta", default="", help="Optional sidecar JSON with stats/hash (default: <out>.meta.json)")
    ap.add_argument("--min-samples", type=int, default=int(os.getenv("META_TRAIN_MIN_SAMPLES", "200")))
    ap.add_argument("--min-pos", type=int, default=int(os.getenv("META_TRAIN_MIN_POS", "40")))
    ap.add_argument("--on-small", choices=["skip", "fail"], default=os.getenv("META_TRAIN_ON_SMALL", "skip"))

    ap.add_argument("--C", type=float, default=float(os.getenv("META_TRAIN_LR_C", "1.0")))
    ap.add_argument("--max-iter", type=int, default=int(os.getenv("META_TRAIN_LR_MAX_ITER", "2000")))

    ap.add_argument("--self-check", type=int, default=int(os.getenv("META_TRAIN_SELF_CHECK", "1")))
    ap.add_argument("--self-check-n", type=int, default=int(os.getenv("META_TRAIN_SELF_CHECK_N", "2000")))

    # Default to meta_feat_v3 if not specified
    ap.add_argument("--schema", default=os.getenv("META_SCHEMA", "meta_feat_v3"))

    args = ap.parse_args()

    parquet_path = Path(args.in_parquet).expanduser().resolve()
    out_json = Path(args.out_json).expanduser().resolve()
    out_meta = Path(args.out_meta).expanduser().resolve() if args.out_meta else out_json.with_suffix(out_json.suffix + ".meta.json")

    if not parquet_path.exists():
        raise SystemExit(f"dataset_not_found path={parquet_path}")

    stats = _load_label_stats(parquet_path, str(args.label_col))
    if stats.n_rows < int(args.min_samples) or stats.n_pos < int(args.min_pos):
        msg = (
            f"dataset_too_small n={stats.n_rows} pos={stats.n_pos} neg={stats.n_neg} "
            f"need_n>={int(args.min_samples)} need_pos>={int(args.min_pos)}"
        )
        if str(args.on_small) == "fail":
            raise SystemExit(msg)
        # skip: keep current model, but write sidecar for observability
        out_meta.parent.mkdir(parents=True, exist_ok=True)
        out_meta.write_text(
            json.dumps(
                {
                    "ts_ms": get_ny_time_millis(),
                    "status": "skipped_dataset_too_small",
                    "dataset": {"n": stats.n_rows, "pos": stats.n_pos, "neg": stats.n_neg},
                    "out_json": str(out_json),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(msg)
        return 0

    trainer = _trainer_path()
    if not trainer.exists():
        raise SystemExit(f"trainer_not_found path={trainer}")

    out_json.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix="meta_lr_", suffix=".json", dir=str(out_json.parent), delete=False) as tf:
        out_tmp = Path(tf.name)

    rc, out = _run_trainer(
        trainer=trainer,
        parquet_path=parquet_path,
        label_col=str(args.label_col),
        out_json_tmp=out_tmp,
        C=float(args.C),
        max_iter=int(args.max_iter),
        self_check=bool(int(args.self_check)),
        self_check_n=int(args.self_check_n),
        schema=str(args.schema),
    )

    if rc != 0:
        # keep tmp for debugging
        out_meta.parent.mkdir(parents=True, exist_ok=True)
        out_meta.write_text(
            json.dumps(
                {
                    "ts_ms": get_ny_time_millis(),
                    "status": "train_failed",
                    "rc": int(rc),
                    "dataset": {"n": stats.n_rows, "pos": stats.n_pos, "neg": stats.n_neg},
                    "tmp_model": str(out_tmp),
                    "stdout": out[-2000:],  # cap
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(out)
        return int(rc)

    # atomic publish
    _atomic_replace(out_tmp, out_json)
    model_sha = _sha256_file(out_json)

    out_meta.parent.mkdir(parents=True, exist_ok=True)
    out_meta.write_text(
        json.dumps(
            {
                "ts_ms": get_ny_time_millis(),
                "status": "ok",
                "dataset": {"n": stats.n_rows, "pos": stats.n_pos, "neg": stats.n_neg},
                "out_json": str(out_json),
                "sha256": model_sha,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"trained_ok out={out_json} sha256={model_sha} n={stats.n_rows} pos={stats.n_pos}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
