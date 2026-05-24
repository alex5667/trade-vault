#!/usr/bin/env python3
"""Generic posterior isotonic calibrator refit for any ml_confirm kind.

Companion to the kind-specific `refit_meta_lr_blend_calibrator.py`. This
module exposes the SAME fitting/evaluation/writing logic but accepts a
`--kind` label (free-form string used for report metadata, metric labels
and log messages) so it can be invoked for `meta_lr`, `edge_stack_v1`,
`meta_lr_blend`, etc.

Sibling-discovery in `services/ml_confirm/config_loader.py` is itself
kind-agnostic: any `calibrator.json` next to a `model_path` is picked up
on the next cfg cache refresh.

Typical usage:
    python -m tools.refit_ml_calibrator \\
        --kind meta_lr \\
        --model-path /var/lib/trade/ml_models/<meta_lr_dir>/meta_lr.joblib \\
        --target-version "" \\
        --lookback-hours 168 --min-n 300 --apply

For meta_lr_blend you can still use the original script — both honour the
same Brier/ECE acceptance gates and produce identical artifact format.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

# Reuse all internals — keep one canonical implementation.
from tools.refit_meta_lr_blend_calibrator import (  # noqa: F401
    _atomic_write_json,
    _brier,
    _ece,
    _emit_metric,
    _read_trades_closed,
    fit_and_evaluate,
)

log = logging.getLogger("refit_ml_calibrator")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--kind", required=True,
        help="Kind label (meta_lr | meta_lr_blend | edge_stack_v1 | util_mh ...). "
             "Used for report metadata and metric labels. Sibling-discovery is "
             "kind-agnostic — the actual kind comes from the model artifact.",
    )
    parser.add_argument(
        "--model-path", required=True,
        help="Path to the model artifact. calibrator.json will be written next to it.",
    )
    parser.add_argument("--stream", default=os.getenv("REFIT_STREAM", "trades:closed"))
    parser.add_argument("--lookback-hours", type=int,
                        default=int(os.getenv("REFIT_LOOKBACK_HOURS", "168")))
    parser.add_argument("--min-n", type=int,
                        default=int(os.getenv("REFIT_MIN_N", "300")))
    parser.add_argument(
        "--target-version", default=os.getenv("REFIT_TARGET_VERSION", ""),
        help='Substring match against ml_version field. "" accepts any '
             "(use during bootstrap when ml_version has not propagated yet).",
    )
    parser.add_argument("--require-brier-improvement", type=float, default=0.005)
    parser.add_argument("--require-ece-improvement", type=float, default=0.01)
    parser.add_argument(
        "--report-dir", default=os.getenv("REFIT_REPORT_DIR", "/var/lib/trade/of_reports"),
    )
    parser.add_argument("--apply", action="store_true",
                        help="Promote artifact on success. Default: dry-run.")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    if not os.path.exists(args.model_path):
        log.error("model not found: %s", args.model_path)
        return 2

    from core.redis_client import get_redis
    r = get_redis()
    pairs = _read_trades_closed(
        r,
        stream=args.stream,
        lookback_hours=args.lookback_hours,
        target_version_substr=args.target_version,
    )

    result = fit_and_evaluate(
        pairs,
        min_n=args.min_n,
        require_brier_improvement=args.require_brier_improvement,
        require_ece_improvement=args.require_ece_improvement,
    )
    result["kind"] = args.kind
    result["model_path"] = args.model_path
    result["lookback_hours"] = args.lookback_hours
    result["target_version"] = args.target_version
    result["ts_ms"] = int(time.time() * 1000)

    log.info("refit kind=%s summary: %s", args.kind, json.dumps(
        {k: v for k, v in result.items() if k != "calibrator"},
    ))

    report_path = ""
    try:
        os.makedirs(args.report_dir, exist_ok=True)
        report_path = os.path.join(
            args.report_dir, f"ml_calibrator_refit_{args.kind}_{result['ts_ms']}.json",
        )
        _atomic_write_json(report_path, result)
        log.info("wrote report: %s", report_path)
    except Exception as e:
        log.warning("report write failed: %s", e)

    _emit_metric(
        result.get("accepted", False), result.get("reason", "unknown"), result.get("n", 0),
    )

    if not result.get("accepted"):
        log.warning("refit kind=%s not accepted: %s", args.kind, result.get("reason"))
        return 1

    if not args.apply:
        log.info("dry-run kind=%s: not writing calibrator (use --apply)", args.kind)
        return 0

    cal_path = os.path.join(os.path.dirname(args.model_path), "calibrator.json")
    artifact = dict(result["calibrator"])
    artifact["meta"] = {
        "kind": f"{args.kind}_posterior",
        "n": result["n"],
        "brier_raw": result["brier_raw"],
        "brier_cal": result["brier_cal"],
        "ece_raw": result["ece_raw"],
        "ece_cal": result["ece_cal"],
        "pos_rate": result["pos_rate"],
        "fit_ts_ms": result["ts_ms"],
        "lookback_hours": args.lookback_hours,
        "target_version": args.target_version,
    }
    _atomic_write_json(cal_path, artifact)
    log.info("wrote calibrator: %s", cal_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
