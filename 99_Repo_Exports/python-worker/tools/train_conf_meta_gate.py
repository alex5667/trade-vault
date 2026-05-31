"""CLI: train the confidence meta-gate v1 model from an NDJSON dataset.

Usage:
    python -m tools.train_conf_meta_gate \
        --in /tmp/conf_meta_gate_dataset.ndjson \
        --candidate-path /var/lib/trade/ml_models/conf_meta_gate_v1_candidate.json \
        --live-path /var/lib/trade/ml_models/conf_meta_gate_v1.json \
        --calibrator platt --target y_util_pos --promote-if-passed

The trainer ALWAYS writes the candidate artifact (audit trail). It writes
the live artifact only when (a) all hard gates pass and (b) `--promote-if-passed`
is set — operator must opt in to live promotion.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from calibration.conf_meta_gate_trainer import (
    TrainConfig,
    build_artifact_json,
    train,
    write_artifact,
)


def _iter_ndjson(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="input", required=True)
    p.add_argument("--candidate-path", required=True)
    p.add_argument("--live-path", default="",
                   help="when set with --promote-if-passed, also write live")
    p.add_argument("--target", default="y_util_pos",
                   choices=["y_win", "y_util_pos"])
    p.add_argument("--calibrator", default="platt",
                   choices=["platt", "isotonic", "identity"])
    p.add_argument("--n-cv-blocks", type=int, default=6)
    p.add_argument("--embargo-ms", type=int, default=600_000)
    p.add_argument("--min-rows", type=int, default=1000)
    p.add_argument("--min-coverage", type=float, default=0.30)
    p.add_argument("--min-p-win", type=float, default=0.56)
    p.add_argument("--min-expected-r", type=float, default=0.02)
    p.add_argument("--min-expected-edge-bps", type=float, default=1.5)
    p.add_argument("--promote-if-passed", action="store_true")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    rows = list(_iter_ndjson(args.input))
    print(f"loaded {len(rows)} rows from {args.input}")
    if not rows:
        print("empty dataset", file=sys.stderr)
        return 2

    cfg = TrainConfig(
        target=args.target,
        calibrator=args.calibrator,
        n_cv_blocks=args.n_cv_blocks,
        embargo_ms=args.embargo_ms,
        min_rows=args.min_rows,
        min_coverage=args.min_coverage,
    )

    try:
        result = train(rows, cfg=cfg)
    except ValueError as e:
        print(f"train failed: {e}", file=sys.stderr)
        return 3

    payload = build_artifact_json(
        result,
        cfg=cfg,
        min_p_win=args.min_p_win,
        min_expected_r=args.min_expected_r,
        min_expected_edge_bps=args.min_expected_edge_bps,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.candidate_path)) or ".", exist_ok=True)
    write_artifact(payload, args.candidate_path)
    print(f"candidate artifact: {args.candidate_path}")
    print(json.dumps({
        "n_rows": result.n_rows,
        "pos_rate": result.pos_rate,
        "oos_auc": result.oos_auc,
        "oos_brier": result.oos_brier,
        "oos_ece": result.oos_ece,
        "top5_expectancy_r": result.top5_expectancy_r,
        "pass_rate": result.pass_rate_at_default,
        "feature_cols": list(result.feature_cols),
        "promotion_passed": result.promotion_passed,
        "promotion_reasons": result.promotion_reasons,
    }, indent=2))

    if args.promote_if_passed and result.promotion_passed and args.live_path:
        os.makedirs(os.path.dirname(os.path.abspath(args.live_path)) or ".", exist_ok=True)
        write_artifact(payload, args.live_path)
        print(f"promoted to live: {args.live_path}")
        return 0

    if args.promote_if_passed and not result.promotion_passed:
        print(f"NOT promoting — gate reasons: {result.promotion_reasons}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
