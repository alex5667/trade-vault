#!/usr/bin/env python3
"""
Nightly Meta Pipeline V1.

Orchestrates:
1. Train (train_meta_model_lr_v4) -> model.json
2. Report (meta_model_quality_report_v1) -> report.json + prom
3. Ramp (meta_auto_ramp_v1) -> ramp_decision.json + redis (optional)

Usage:
  python -m tools.nightly_meta_pipeline_v1 \
    --in-parquet /path/to/dataset.parquet \
    --label-col y \
    --out-model-json /path/to/meta_model.json \
    --out-report-json /path/to/meta_report.json \
    --prom-textfile /var/lib/.../meta_quality.prom \
    --apply-ramp \
    --ramp-state /path/to/meta_ramp_state.json \
    --ramp-dry-run
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nightly_meta")


def run_cmd(cmd: list[str], check: bool = True) -> None:
    cmd_str = " ".join(cmd)
    logger.info(f"RUN: {cmd_str}")
    subprocess.run(cmd, check=check)


def _module_exists(py_file_name: str) -> bool:
    # tools/*.py live next to this file
    here = Path(__file__).resolve().parent
    return (here / py_file_name).exists()


def _pick_module(preferred: str, fallback: str, preferred_py: str) -> str:
    """Return module name to execute with -m."""
    return preferred if _module_exists(preferred_py) else fallback


def main() -> int:
    ap = argparse.ArgumentParser()
    # Input/Output
    ap.add_argument("--in-parquet", required=True, help="Path to input parquet dataset")
    ap.add_argument("--label-col", default="y", help="Label column name")
    ap.add_argument("--out-model-json", required=True, help="Path to save trained model JSON")
    ap.add_argument("--out-report-json", required=True, help="Path to save quality report JSON")
    
    # Data Build Params
    ap.add_argument("--build-dataset", action="store_true", help="Build dataset from Redis/Streams before training")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--since-hours", type=float, default=72)
    ap.add_argument("--canary-symbols", default="BTCUSDT,ETHUSDT")
    ap.add_argument("--tmp-dir", default="/tmp", help="Temp dir for intermediate files")
    ap.add_argument("--pg-fallback", type=int, default=1, help="Fallback to Postgres if Redis export < N trades")

    # Training Params
    ap.add_argument("--schema", default=os.getenv("META_SCHEMA", "meta_feat_v3"), help="Feature schema (v1/v2/v3/v4)")
    ap.add_argument("--C", default="1.0", help="Inverse regularization strength")
    
    # Reporting Params
    ap.add_argument("--prom-textfile", default="", help="Path to Prometheus textfile for metrics")
    
    # Model Promotion (optional)
    ap.add_argument("--promote-model", action="store_true", help="Promote trained model JSON into a stable dir (atomic copy)")
    ap.add_argument("--promote-dir", default=os.getenv("META_PROMOTE_DIR", ""), help="Directory for promoted model artifacts")
    ap.add_argument("--promote-link-latest", action="store_true", help="Create/replace latest_<schema>.json symlink in promote-dir")

    ap.add_argument("--group-cols", default=os.getenv("META_REPORT_GROUP_COLS", "regime_bucket,session_bucket"),
                    help="Comma-separated group cols for report v2 (ignored by v1)")
    ap.add_argument("--min-group-n", type=int, default=int(os.getenv("META_REPORT_MIN_GROUP_N", "200")),
                    help="Minimum samples per group for report v2 (ignored by v1)")
    ap.add_argument("--include-dq-bucket", action="store_true",
                   default=(os.getenv("META_REPORT_INCLUDE_DQ_BUCKET", "0") == "1"),
                   help="If set, append dq_health_bucket to group-cols (report v3).")
    ap.add_argument("--dq-health-key", default=os.getenv("META_REPORT_DQ_HEALTH_KEY", "dq_health_score"))
    ap.add_argument("--dq-health-fallback-key", default=os.getenv("META_REPORT_DQ_HEALTH_FALLBACK_KEY", "data_health"))
    
    # Ramp Params
    ap.add_argument("--apply-ramp", action="store_true", help="Run auto-ramp step")
    ap.add_argument("--ramp-state", default="", help="Path to save ramp decision JSON")
    ap.add_argument("--ramp-dry-run", action="store_true", help="If set, ramp will NOT write to Redis (overrides --apply)")
    ap.add_argument("--apply-guard", action="store_true", help="Run guardrails step")

    # Status Params
    ap.add_argument("--out-status-json", help="Path to save aggregate meta status JSON")
    ap.add_argument("--status-prom-textfile", help="Path to save meta status Prom metrics")
    
    args = ap.parse_args()

    # 0. Build Dataset (Optional)
    if args.build_dataset:
        logger.info("=== Step 0: Build Dataset ===")
        import time
        import json
        ts = time.strftime("%Y%m%d_%H%M%S")
        run_dir = Path(args.tmp_dir) / f"nightly_{ts}"
        run_dir.mkdir(parents=True, exist_ok=True)
        
        inputs_raw = run_dir / "of_inputs_raw.ndjson"
        inputs_can = run_dir / "of_inputs_canary.ndjson"
        trades_out = run_dir / "closed_trades.ndjson"
        
        # If in-parquet is provided, use it as target. Else use a temp one.
        # But if user provided --in-parquet, they might expect it to be READ from.
        # If --build-dataset is ON, we should probably WRITE to --in-parquet? 
        # Or write to temp and then move?
        # Let's write to the path specified by --in-parquet.
        dataset_out = Path(args.in_parquet)
        dataset_out.parent.mkdir(parents=True, exist_ok=True)
        
        # 0.1 Export Inputs
        logger.info("Exporting OF inputs...")
        export_cmd = [
            sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
            "--redis-url", args.redis_url,
            "--out", str(inputs_raw),
            "--stream", "signals:of:confirm",
            "--field", "payload",
            "--resume",
            "--since-hours", str(args.since_hours),
        ]
        run_cmd(export_cmd)
        
        # 0.2 Filter Canary
        logger.info("Filtering canary symbols...")
        allow = {s.strip().upper() for s in args.canary_symbols.split(",") if s.strip()}
        n_can = 0
        with open(inputs_raw, "r", encoding="utf-8") as f_in, open(inputs_can, "w", encoding="utf-8") as f_out:
            for line in f_in:
                if not line.strip(): continue
                try:
                    row = json.loads(line)
                    p = row.get("payload")
                    inp = json.loads(p) if isinstance(p, str) else (p if isinstance(p, dict) else row)
                    sym = str(inp.get("symbol", "")).upper()
                    if "*" in allow or sym in allow:
                        f_out.write(json.dumps(inp, ensure_ascii=False) + "\n")
                        n_can += 1
                except Exception:
                    continue
        logger.info(f"Canary inputs: {n_can}")

        # 0.3 Export Trades
        logger.info("Exporting trades...")
        trades_cmd = [
            sys.executable, "tools/export_trade_closed_ndjson.py",
            "--since-hours", str(args.since_hours),
            "--out", str(trades_out),
            "--stream", "events:trades",
            "--redis-url", args.redis_url,
            "--pg-fallback", str(args.pg_fallback),
        ]
        run_cmd(trades_cmd)

        # 0.4 Build Dataset
        logger.info("Building parquet...")
        build_cmd = [
            sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v2",
            "--inputs", str(inputs_can),
            "--closed", str(trades_out),
            "--out", str(dataset_out),
            "--r-min", "0.0001",
        ]
        run_cmd(build_cmd)
        
        logger.info(f"Dataset built: {dataset_out}")

    if not Path(args.in_parquet).exists():
        logger.error(f"Input parquet not found: {args.in_parquet}")
        return 1

    # Check if dataset is empty or valid
    try:
        import pandas as pd
        df_check = pd.read_parquet(args.in_parquet)
        logger.info(f"Dataset check: shape={df_check.shape} cols={list(df_check.columns)}")
        
        if df_check.empty:
            logger.warning(f"Dataset {args.in_parquet} is empty (0 rows). Skipping pipeline (success).")
            return 0
            
        if args.label_col not in df_check.columns:
            logger.warning(f"Dataset {args.in_parquet} missing label col '{args.label_col}'. Skipping pipeline (success).")
            return 0
            
        # Check class balance
        y_vals = df_check[args.label_col].unique()
        if len(y_vals) < 2:
            logger.warning(f"Dataset {args.in_parquet} has only 1 class ({y_vals}). Skipping training (success).")
            return 0
            
    except ImportError:
        logger.warning("Pandas not found for check, proceeding...")
    except Exception as e:
        logger.warning(f"Could not verify dataset size: {e}. Proceeding to train...")

    # 1. Train
    logger.info("=== Step 1: Train Meta Model ===")
    train_cmd = [
        sys.executable, "-m", "tools.train_meta_model_lr_v4",
        "--in-parquet", args.in_parquet,
        "--out-json", args.out_model_json,
        "--label-col", args.label_col,
        "--schema", args.schema,
        "--C", args.C,
        # Default flags for simplicity
    ]
    run_cmd(train_cmd)

    if not Path(args.out_model_json).exists():
        logger.error(f"Model file not found: {args.out_model_json}")
        return 1

    # 1.5 Promote (atomic) so meta_model_path can point to stable artifact
    model_json_path = args.out_model_json
    promote_env = os.getenv("META_PROMOTE_MODEL", "0")
    if args.promote_model or promote_env == "1":
        if not args.promote_dir:
            logger.warning("META_PROMOTE_MODEL enabled but promote-dir is empty; skipping promotion")
        else:
            logger.info("=== Step 1.5: Promote Model (atomic) ===")
            manifest_path = str(Path(args.tmp_dir) / f"meta_promote_{args.schema}.json")
            promote_cmd = [
                sys.executable, "-m", "tools.meta_model_promote_v1",
                "--in-json", args.out_model_json,
                "--schema", args.schema,
                "--out-dir", args.promote_dir,
                "--out-manifest-json", manifest_path,
            ]
            if args.promote_link_latest:
                promote_cmd.append("--link-latest")
            run_cmd(promote_cmd)
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    man = json.load(f)
                model_json_path = man.get("promoted_model_json", model_json_path)
                logger.info(f"Promoted model_json_path={model_json_path}")
            except Exception as e:
                logger.warning(f"Failed to read promotion manifest; using original model. err={e}")

    # 2. Report
    logger.info("=== Step 2: Quality Report ===")
    
    # Dynamic V3 -> V2 -> V1 dispatch
    report_mod = _pick_module(
        preferred="tools.meta_model_quality_report_v3",
        fallback="tools.meta_model_quality_report_v1",  # safety fallback
        preferred_py="meta_model_quality_report_v3.py",
    )
    # Check v2 if v3 missing
    if report_mod == "tools.meta_model_quality_report_v1" and _module_exists("meta_model_quality_report_v2.py"):
         report_mod = "tools.meta_model_quality_report_v2"

    if report_mod.endswith("_v3") or report_mod.endswith("_v2"):
        # v3/v2 expects dataset-parquet + group settings
        group_cols = args.group_cols
        if args.include_dq_bucket:
            parts = [p.strip() for p in str(group_cols).split(",") if p.strip()]
            if "dq_health_bucket" not in parts:
                parts.append("dq_health_bucket")
                group_cols = ",".join(parts)

        report_cmd = [
            sys.executable, "-m", report_mod,
            "--dataset-parquet", args.in_parquet,
            "--model-json", model_json_path,
            "--label-col", args.label_col,
            "--out-json", args.out_report_json,
            "--group-cols", group_cols,
            "--min-group-n", str(args.min_group_n),
        ]
        if report_mod.endswith("_v3"):
            report_cmd.extend([
                "--dq-health-key", str(args.dq_health_key),
                "--dq-health-fallback-key", str(args.dq_health_fallback_key),
                "--dq-health-bucket-col", "dq_health_bucket",
            ])
    else:
        # v1 expects in-parquet
        report_cmd = [
            sys.executable, "-m", report_mod,
            "--in-parquet", args.in_parquet,
            "--model-json", model_json_path,
            "--label-col", args.label_col,
            "--out-json", args.out_report_json,
        ]

    if args.prom_textfile:
        report_cmd.extend(["--prom-textfile", args.prom_textfile])
        
    run_cmd(report_cmd)

    if not Path(args.out_report_json).exists():
        logger.error(f"Report file not found: {args.out_report_json}")
        return 1

    # 2.5 Guardrails (Optional)
    # Check existence
    has_guard = Path("tools/meta_guardrails_v1.py").exists() or Path("python-worker/tools/meta_guardrails_v1.py").exists()
    
    if args.apply_guard and has_guard:
        logger.info("=== Step 2.5: Guardrails ===")
        # apply=1 unless ramp-dry-run is ON (which implies we don't want side effects?)
        # Or standard P11 logic: guard updates redis if --apply=1.
        guard_apply = "0" if args.ramp_dry_run else "1"
        
        guard_cmd = [
            sys.executable, "-m", "tools.meta_guardrails_v1",
            "--model-json", model_json_path,
            "--dataset-parquet", args.in_parquet,
            "--report-json", args.out_report_json,
            "--apply", guard_apply,
            "--redis-url", args.redis_url,
            "--expected-schema", args.schema,
        ]
        
        if args.prom_textfile:
             # Guardrails can append or use separate file.
             # Use _guard.prom suffix to avoid race validation
             guard_prom = args.prom_textfile.replace(".prom", "_guard.prom")
             guard_cmd.extend(["--prom-textfile", guard_prom])
             
        run_cmd(guard_cmd, check=True)
    elif args.apply_guard:
        logger.warning("Guardrails requested but script not found. Skipping.")

    # 3. Ramp (Optional)
    # 3. Ramp (Optional)
    if args.apply_ramp:
        logger.info("=== Step 3: Auto Ramp ===")
        # Determine apply flag: 1 if validation passed AND not dry-run, else 0
        apply_val = "0" if args.ramp_dry_run else "1"

        if str(os.environ.get("META_RAMP_CONTROLLED", "0")) == "1" and _module_exists("meta_ramp_apply_v3.py"):
            ramp_mod = "tools.meta_ramp_apply_v3"
            logger.info("Using P19 Controlled Ramp (meta_ramp_apply_v3)")
        else:
            ramp_mod = _pick_module(
                preferred="tools.meta_auto_ramp_v2",
                fallback="tools.meta_auto_ramp_v1",
                preferred_py="meta_auto_ramp_v2.py",
            )
        
        ramp_cmd = [
            sys.executable, "-m", ramp_mod,
            "--report-json", args.out_report_json,
            "--apply", apply_val,
        ]

        if ramp_mod == "tools.meta_ramp_apply_v3":
            ramp_cmd.extend(["--model-json", model_json_path])
            if args.redis_url:
                ramp_cmd.extend(["--redis-url", args.redis_url])
        
        # Output handling
        logger.info(f"RUN: {' '.join(ramp_cmd)} > {args.ramp_state or 'stdout'}")
        
        if args.ramp_state:
            with open(args.ramp_state, "w") as f_out:
                subprocess.run(ramp_cmd, check=True, stdout=f_out)
        else:
            subprocess.run(ramp_cmd, check=True)

    # 4. Status Snapshot
    if args.out_status_json or args.status_prom_textfile:
        logger.info("=== Step 4: Meta Status Snapshot ===")
        status_cmd = [
            sys.executable, "-m", "tools.meta_status_snapshot_v1",
            "--model-json", model_json_path,
            "--report-json", args.out_report_json,
            "--redis-url", args.redis_url,
        ]
        if args.ramp_state:
            status_cmd.extend(["--ramp-json", args.ramp_state])
        if args.out_status_json:
            status_cmd.extend(["--out-json", args.out_status_json])
        if args.status_prom_textfile:
            status_cmd.extend(["--prom-textfile", args.status_prom_textfile])
        
        # Don't fail the pipeline if snapshot fails
        run_cmd(status_cmd, check=False)

    # 5. Promote Dir Check (Optional, Best Effort)
    # P24: meta_promote_dir_check_v1 -> writes to prom textfile
    promote_check_out = os.getenv("META_PROMOTE_DIR_CHECK", "")
    if promote_check_out:
        logger.info("=== Step 5: Promote Dir Check ===")
        try:
            from tools.meta_promote_dir_check_v1 import check_promote_dir, write_metrics
            # Need promote_dir. If not passed in args, use env or default
            p_dir = args.promote_dir or os.getenv("META_PROMOTE_DIR", "/var/lib/trade/of_reports/models/promoted")
            
            logger.info(f"Checking promote dir: {p_dir}")
            metrics = check_promote_dir(p_dir)
            if write_metrics(metrics, promote_check_out):
                logger.info(f"Promote check metrics written to {promote_check_out}")
            else:
                logger.error(f"Failed to write promote check metrics to {promote_check_out}")
        except Exception as e:
            logger.error(f"Promote dir check failed (non-fatal): {e}")

    # 6. Promote Retention (Optional, Best Effort)
    # P24: cleanup_promoted_models_v1
    if os.getenv("META_PROMOTE_RETENTION_ENABLE", "0") == "1":
        logger.info("=== Step 6: Promote Retention Cleanup ===")
        try:
            from tools.cleanup_promoted_models_v1 import cleanup_promoted_models
            p_dir = args.promote_dir or os.getenv("META_PROMOTE_DIR", "/var/lib/trade/of_reports/models/promoted")
            keep_last = int(os.getenv("META_PROMOTE_RETENTION_KEEP_LAST", "80"))
            keep_days = int(os.getenv("META_PROMOTE_RETENTION_KEEP_DAYS", "14"))
            
            logger.info(f"Cleaning up promoted models in {p_dir} (keep_last={keep_last}, keep_days={keep_days})...")
            cleanup_promoted_models(p_dir, keep_last=keep_last, keep_days=keep_days, dry_run=False)
        except Exception as e:
            logger.error(f"Promote retention cleanup failed (non-fatal): {e}")

    logger.info("=== Nightly Pipeline Completed Successfully ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
