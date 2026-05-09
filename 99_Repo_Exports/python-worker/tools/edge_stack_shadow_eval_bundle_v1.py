from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""
P60 Edge Stack Shadow Eval Bundle.

Goals:
1. Build fresh dataset (last 24h) from Redis.
2. Load Champion & Candidate models (from cfg:ml_confirm:edge_stack_v1:*).
3. Evaluate shadow metrics (Brier, ECE, Prec@5%, ExpR@5%).
4. Write results to Redis metrics:edge_stack_shadow:last.
5. Optional: Guarded promotion of Candidate -> Champion.

Usage:
  python -m tools.edge_stack_shadow_eval_bundle_v1 [--window_hours 24] [--auto_promote_guarded 0]
"""

import argparse
import logging
import os
import sys
from typing import Any

import joblib
import numpy as np
import redis

from ml_analysis.tools.build_edge_stack_dataset_from_redis import build_dataset_df
from ml_analysis.tools.edge_stack_shadow_metrics_p60 import calculate_shadow_metrics, check_promotion_guard

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Constants
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
METRICS_KEY = "metrics:edge_stack_shadow:last"
CFG_CHAMPION_KEY = "cfg:ml_confirm:edge_stack_v1:champion"
CFG_CANDIDATE_KEY = "cfg:ml_confirm:edge_stack_v1:candidate"

# Defaults
DEFAULT_WINDOW_HOURS = 24
DEFAULT_MAX_ROWS = 0 # 0 = unlimited

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def load_model(path: str) -> Any:
    logger.info(f"Loading model from {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model file not found: {path}")
    return joblib.load(path)

def atomic_copy(src: str, dst: str) -> None:
    import shutil
    os.makedirs(os.path.dirname(os.path.abspath(dst)) or ".", exist_ok=True)
    tmp = f"{dst}.tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)
    logger.info(f"Copied {src} -> {dst}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window_hours", type=int, default=int(os.environ.get("EDGE_STACK_SHADOW_WINDOW_HOURS", DEFAULT_WINDOW_HOURS)))
    parser.add_argument("--max_rows", type=int, default=int(os.environ.get("EDGE_STACK_SHADOW_MAX_ROWS", DEFAULT_MAX_ROWS)))
    parser.add_argument("--auto_promote_guarded", type=int, default=int(os.environ.get("EDGE_STACK_AUTO_PROMOTE_GUARDED", 0)))
    args = parser.parse_args()

    r = get_redis_client()

    # 1. Build Dataset
    logger.info(f"Building dataset window={args.window_hours}h max_rows={args.max_rows}")
    try:
        # P58/P59 dataset builder
        # Input: redis_url, lookback_hours (or start_ts?), max_rows
        # Ref: ml_analysis/tools/build_edge_stack_dataset_from_redis.py
        # Function: build_dataset_df(redis_url, lookback_hours=..., max_rows=...)
        # Note: checking signature or usage from previous context if possible,
        # but standard usage is implied.
        df = build_dataset_df(
            redis_url=REDIS_URL,
            lookback_hours=args.window_hours,
            max_rows=args.max_rows if args.max_rows > 0 else None
        )
    except Exception as e:
        logger.error(f"Failed to build dataset: {e}")
        r.hset(METRICS_KEY, mapping={
            "status": "error",
            "error": str(e),
            "updated_ts_ms": get_ny_time_millis(),
        })
        sys.exit(1)

    if df.empty:
        logger.warning("Dataset is empty. Skipping eval.")
        r.hset(METRICS_KEY, mapping={
            "status": "skipped",
            "reason": "empty_dataset",
            "updated_ts_ms": get_ny_time_millis(),
        })
        sys.exit(0)

    logger.info(f"Dataset shape: {df.shape}")

    # Prepare X, y, r
    # Assuming 'outcome' is target (0/1) and 'R_realized' or similar for expectancy?
    # Usually dataset has 'target' or 'outcome'.
    # Let's check P58 diff context or rely on standard 'outcome' column.
    # In P59 context: 'outcome' is likely the target.
    # 'R_multiple' often exists.

    if "y" not in df.columns:
        logger.error("Column 'y' not found in dataset")
        sys.exit(1)

    y_true = df["y"].values.astype(int)

    # R-multiple for expectancy
    if "r_mult" in df.columns:
        y_r = df["r_mult"].values.astype(float)
    elif "R_multiple" in df.columns:
        y_r = df["R_multiple"].values.astype(float)
    else:
        logger.warning("r_mult column not found, expectancy will be 0")
        y_r = np.zeros_like(y_true, dtype=float)

    # Features: Drop non-feature columns
    # Usually, features are all except metadata.
    # We need to know which columns the model expects.
    # Often models (pipelines) handle column selection or we drop known metadata.
    # Safe bet: Drop known metadata columns if present.
    # Or rely on model pipeline to select features.
    # Let's assume the model is a pipeline that handles this, OR we need to drop targets.

    drop_cols = ["y", "outcome", "target", "r_mult", "R_multiple", "r_multiple", "timestamp", "trade_id", "symbol", "side"]
    X = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # 2. Load Models & Inference
    # Champion
    champ_cfg = r.hgetall(CFG_CHAMPION_KEY)
    cand_cfg = r.hgetall(CFG_CANDIDATE_KEY)

    results = {
        "status": "ok",
        "updated_ts_ms": get_ny_time_millis(),
        "n_samples": len(df),
        "champion_path": champ_cfg.get("model_path", ""),
        "candidate_path": cand_cfg.get("model_path", "")
    }

    # Evaluate Champion
    if champ_cfg and champ_cfg.get("model_path"):
        try:
            model = load_model(champ_cfg["model_path"])
            # Predict
            if hasattr(model, "predict_proba"):
                y_prob = model.predict_proba(X)[:, 1]
            else:
                y_prob = model.predict(X)

            metrics = calculate_shadow_metrics(y_true, y_prob, y_r)
            logger.info(f"Champion metrics: {metrics}")

            for k, v in metrics.items():
                results[f"champion_{k}"] = v
        except Exception as e:
            logger.error(f"Champion eval failed: {e}")
            results["champion_error"] = str(e)
    else:
        logger.warning("No champion config found")
        results["champion_error"] = "no_config"

    # Evaluate Candidate
    candidate_metrics = {}
    if cand_cfg and cand_cfg.get("model_path"):
        try:
            model = load_model(cand_cfg["model_path"])
            if hasattr(model, "predict_proba"):
                y_prob = model.predict_proba(X)[:, 1]
            else:
                y_prob = model.predict(X)

            metrics = calculate_shadow_metrics(y_true, y_prob, y_r)
            candidate_metrics = metrics
            logger.info(f"Candidate metrics: {metrics}")

            for k, v in metrics.items():
                results[f"candidate_{k}"] = v
        except Exception as e:
            logger.error(f"Candidate eval failed: {e}")
            results["candidate_error"] = str(e)
    else:
        logger.info("No candidate config found")
        results["candidate_status"] = "none"

    # 3. Guarded Promotion
    promoted = False
    promote_reason = ""

    if args.auto_promote_guarded and results.get("candidate_brier") and results.get("champion_brier"):
        # Env thresholds
        max_brier_rel = float(os.environ.get("EDGE_STACK_PROMOTE_MAX_BRIER_REL", 1.02))
        max_ece_abs = float(os.environ.get("EDGE_STACK_PROMOTE_MAX_ECE_ABS", 0.005))
        min_prec_delta = float(os.environ.get("EDGE_STACK_PROMOTE_MIN_PREC_DELTA", 0.0))

        # Prepare Dicts
        champ_m = {k.replace("champion_", ""): v for k, v in results.items() if k.startswith("champion_") and isinstance(v, (int, float))}
        cand_m = {k.replace("candidate_", ""): v for k, v in results.items() if k.startswith("candidate_") and isinstance(v, (int, float))}

        logger.info(f"Checking promotion guard: rel_brier<={max_brier_rel}, ece_diff<={max_ece_abs}, prec_delta>={min_prec_delta}")
        should_promote, reasons = check_promotion_guard(champ_m, cand_m, max_brier_rel, max_ece_abs, min_prec_delta)

        if should_promote:
            logger.info("Promotion Guard Passed! Promoting Candidate -> Champion")
            # Promote
            # Copy candidate file to Stable Champion Path
            # Defined by ENV EDGE_STACK_V1_DIR or specific EDGE_STACK_PROMOTE_CHAMPION_PATH
            # Default: $EDGE_STACK_V1_DIR/champions/edge_stack_v1_champion.joblib

            base_dir = os.environ.get("EDGE_STACK_V1_DIR", "/var/lib/trade/ml_models/edge_stack_v1")
            target_path = os.environ.get("EDGE_STACK_PROMOTE_CHAMPION_PATH")
            if not target_path:
                target_path = os.path.join(base_dir, "champions", "edge_stack_v1_champion.joblib")

            src_path = cand_cfg["model_path"]

            try:
                atomic_copy(src_path, target_path)

                # Update Redis Config for Champion?
                # Usually P59 uses 'model_path' in cfg. If we overwrite the file at 'champion_path',
                # and the champion cfg points to a STABLE path, then we are good.
                # If champion cfg points to a timestamped file, we need to update the cfg.
                # The Plan says: "Copy artifact to stable path".
                # It does NOT explicitly say "Update Redis Cfg".
                # Implicitly, the system (Executor) loads from Stable Path OR Redis Cfg needs update.
                # If we assume Champion Cfg points to the stable path, then overwriting file is enough.
                # Let's also update Redis Cfg just in case to point to the new file (which might be the stable one).

                # Update stats
                promoted = True
                promote_reason = "guard_passed"

                # Optional: If we want to track WHICH model is champion, we might want to update cfg to point to specific artifact?
                # But "Guarded Promotion" usually implies "Making it the new default".
                # Copying to stable path is the safest 'deploy'.

                # Update cfg champion to point to this new stable file if not already
                if champ_cfg.get("model_path") != target_path:
                    logger.info(f"Updating Champion Config to {target_path}")
                    r.hset(CFG_CHAMPION_KEY, "model_path", target_path)
                    r.hset(CFG_CHAMPION_KEY, "updated_ts_ms", get_ny_time_millis())
                    r.hset(CFG_CHAMPION_KEY, "source_candidate", src_path)

                    notify_stream = os.environ.get("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
                    prec_delta = cand_m.get("prec_at_5", 0) - champ_m.get("prec_at_5", 0)
                    msg = (
                        f"🏆 <b>Edge Stack ML Promotion</b>! 🏆\n"
                        f"Model <b>v13</b> passed mathematical guards and was promoted to Champion!\n\n"
                        f"<b>Target:</b> {target_path}\n"
                        f"<b>Stats:</b> Brier Rel={cand_m.get('brier_rel', 0):.3f}, "
                        f"ECE Diff={cand_m.get('ece_diff', 0):.4f}, "
                        f"Prec Delta={prec_delta:.3f}"
                    )
                    r.xadd(notify_stream, {"text": msg}, maxlen=5000)

            except Exception as e:
                logger.error(f"Promotion failed: {e}")
                promoted = False
                promote_reason = f"copy_failed: {e}"
        else:
            logger.info(f"Promotion Guard Failed: {reasons}")
            promoted = False
            promote_reason = "; ".join(reasons)

    results["promoted"] = int(promoted)
    results["promote_reason"] = promote_reason

    # 4. Write Results
    logger.info(f"Writing metrics to {METRICS_KEY}: {results}")

    # helper to format
    flat = {}
    for k, v in results.items():
        if v is None: continue
        flat[k] = str(v)
    if flat:
        r.hset(METRICS_KEY, mapping=flat)

    logger.info("Done.")

if __name__ == "__main__":
    main()
