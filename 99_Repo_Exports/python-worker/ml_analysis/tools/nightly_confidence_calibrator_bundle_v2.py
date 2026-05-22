from utils.time_utils import get_ny_time_millis

"""Nightly confidence calibrator bundle V2 (Dataset -> Train -> Valid -> Promote).

Runs nightly (or periodically).
1. Build Dataset from Redis (via build_edge_stack_dataset_from_redis).
2. Train V2 Bundle (train_confidence_calibrator_v2.py).
3. Validate (Guardrails on global ECE/Brier).
4. Promote (atomic replace of champion bundle).

Outputs:
  versions/conf_cal_v2_YYYYMMDD_HHMMSS.json
  conf_cal_bundle_latest.json
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("nightly_conf_cal_v2")

def _now_ms() -> int:
    return get_ny_time_millis()

def _run(module: str, args: list[str], timeout: int = 3600) -> tuple[bool, str, str]:
    cmd = [sys.executable, "-m", module] + args
    logger.info(f"Running: {' '.join(cmd)}")
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        logger.error(f"Command failed code={p.returncode}\nSTDOUT: {p.stdout}\nSTDERR: {p.stderr}")
    return (p.returncode == 0), (p.stdout or ""), (p.stderr or "")

def main():
    parser = argparse.ArgumentParser()
    # Data Config
    parser.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--lookback_days", type=int, default=7)
    parser.add_argument("--signals_count", type=int, default=200000)

    # Training Config
    parser.add_argument("--method", default="platt")
    parser.add_argument("--bucket_by", default="session_regime")
    parser.add_argument("--key", default="confidence_v1")

    # Paths
    parser.add_argument("--out_dir", default=os.environ.get("CONF_CAL_OUT_DIR", "/var/lib/trade/of_calibrators"))
    parser.add_argument("--reports_dir", default=os.environ.get("CONF_CAL_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal"))
    parser.add_argument("--champion_name", default="conf_cal_bundle_latest.json")

    # Guardrails
    parser.add_argument("--guard_min_ece_abs", type=float, default=0.001)
    parser.add_argument("--guard_min_brier_abs", type=float, default=0.0005)

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.reports_dir, exist_ok=True)
    versions_dir = os.path.join(args.out_dir, "versions")
    os.makedirs(versions_dir, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    now_ms = _now_ms()
    since_ms = now_ms - (args.lookback_days * 86400 * 1000)

    # Temporary files
    dataset_jsonl = os.path.join(args.reports_dir, f"dataset_{stamp}.jsonl")
    dataset_report = os.path.join(args.reports_dir, f"dataset_report_{stamp}.json")
    bundle_tmp = os.path.join(args.reports_dir, f"bundle_v2_{stamp}_tmp.json")
    bundle_ver = os.path.join(versions_dir, f"conf_cal_v2_{stamp}.json")
    bundle_champion = os.path.join(args.out_dir, args.champion_name)

    # 1. Build Dataset
    logger.info("Step 1: Building dataset from Redis...")
    ok, out, err = _run(
        "ml_analysis.tools.build_edge_stack_dataset_from_redis",
        [
            "--redis_url", args.redis_url,
            "--out_jsonl", dataset_jsonl,
            "--out_report_json", dataset_report,
            "--since_ms", str(since_ms),
            "--until_ms", str(now_ms),
            "--signals_count", str(args.signals_count),
             # Defaults for others
        ]
    )
    if not ok:
        sys.exit(1)

    # 2. Train V2 Bundle
    logger.info(f"Step 2: Training V2 Bundle ({args.method}, {args.bucket_by})...")
    ok, out, err = _run(
        "ml_analysis.tools.train_confidence_calibrator_v2",
        [
            "--in_jsonl", dataset_jsonl,
            "--out_bundle", bundle_tmp,
            "--method", args.method,
            "--key", args.key,
        ]
    )
    if not ok:
        sys.exit(1)

    # 3. Guardrails
    logger.info("Step 3: Verifying guardrails...")
    try:
        with open(bundle_tmp, encoding="utf-8") as f:
            data = json.load(f)

        # Check Global metrics
        g_metrics = data.get("buckets", {}).get("global", {}).get("metrics", {})
        if not g_metrics:
            # Empty bundle means dataset had no confidence_v1 rows — do NOT promote.
            # Common cause: signals:of:inputs stale OR confidence_v1 key missing from signals.
            logger.error(
                "Trained bundle has no global metrics (buckets: {} or missing confidence_v1 in dataset). "
                "Skipping promotion to preserve last valid bundle."
            )
            sys.exit(0)
        else:
            raw_ece = g_metrics["raw"]["ece"]
            cal_ece = g_metrics["cal"]["ece"]
            raw_brier = g_metrics["raw"]["brier"]
            cal_brier = g_metrics["cal"]["brier"]

            # Improvement check
            # We want cal metrics to be BETTER (lower) than raw, by at least margin?
            # Or just not worse?
            # "guard is: cal_ece <= raw_ece - guard_min_ece_abs" implies strict improvement required.

            ece_ok = cal_ece <= (raw_ece - args.guard_min_ece_abs)
            brier_ok = cal_brier <= (raw_brier - args.guard_min_brier_abs)

            logger.info(f"Guard: ECE {raw_ece:.4f}->{cal_ece:.4f} (OK={ece_ok}), Brier {raw_brier:.4f}->{cal_brier:.4f} (OK={brier_ok})")

            if not (ece_ok and brier_ok):
                # If training on small data, maybe just fallback to identity?
                # But here we fail promotion to keep old safe bundle.
                logger.error("Guardrails FAILED. Promotion aborted.")
                # We do NOT exit(1), we just exit(0) without promoting,
                # effectively keeping the old one. This is "fail-open" in terms of "service continues with old config".
                # But maybe logic implies "fail safe"?
                # "Else -> keeps previous calibrator (fail-open)" matches logic.
                sys.exit(0)

    except Exception as e:
        logger.error(f"Error checking guardrails: {e}")
        sys.exit(1)

    # 4. Promote
    logger.info("Step 4: Promoting bundle...")
    # First save to versions
    shutil.copy2(bundle_tmp, bundle_ver)

    # Atomically replace champion
    # Create tmp champion then rename
    champ_tmp = bundle_champion + ".tmp"
    shutil.copy2(bundle_ver, champ_tmp)
    os.replace(champ_tmp, bundle_champion)

    logger.info(f"SUCCESS. Promoted {bundle_ver} to {bundle_champion}")

    # Cleanup
    try:
        os.remove(dataset_jsonl)
        os.remove(bundle_tmp)
    except Exception:
        pass

if __name__ == "__main__":
    main()
