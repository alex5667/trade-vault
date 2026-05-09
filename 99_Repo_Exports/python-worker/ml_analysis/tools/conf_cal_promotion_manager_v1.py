from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
"""
conf_cal_promotion_manager_v1.py

Goal:
  - Automate the promotion of a 'candidate' confidence calibration bundle to 'champion'.
  - Verify performance metrics (ECE, Brier, Precision) on recent data (24h).
  - Generate a 'proof' file that the Strategy can use for 'calibrated gating'.

Usage:
  python3 conf_cal_promotion_manager_v1.py [--dry-run] [--force]

Env Vars:
  SIGNALS_PG_DSN: Postgres DSN for signals
  CONF_CAL_CHAMPION_BUNDLE_PATH: Path to current champion bundle (read/write)
  CONF_CAL_CANDIDATE_BUNDLE_PATH: Path to candidate bundle (read only)
  CONF_CAL_PROOF_STATE_PATH: Path to write proof state (JSON)
  CONF_CAL_PROMOTION_STATUS_PATH: Path to write promotion status (JSON)
"""

import argparse
import json
import logging
import math
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from typing import Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - optional in unit tests
    psycopg2 = None  # type: ignore
    RealDictCursor = None  # type: ignore

from ml_analysis.calibration_extended import report as extended_calibration_report

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("conf_cal_promoter")

# ---------------------------------------------------------------------------
# Configuration / Defaults
# ---------------------------------------------------------------------------
DEFAULT_MIN_N_24H = 400
DEFAULT_MAX_ECE = 0.06
DEFAULT_MAX_BRIER = 0.24
DEFAULT_MIN_PREC_TOP5P = 0.55
DEFAULT_MIN_DELTA_ECE = 0.005  # Candidate must be better by this much to promote

# ---------------------------------------------------------------------------
# Data Fetching
# ---------------------------------------------------------------------------
def fetch_recent_signals(dsn: str, hours: int = 24) -> list[dict[str, Any]]:
    """
    Fetches signals from the last N hours.
    Returns list of dicts with:
      - raw_conf: float
      - label: int (0 or 1)
      - context: dict (for bucketing)

    Query design notes:
      - Uses exit_ts_ms (raw bigint) instead of exit_ts (generated column) so
        PostgreSQL can use idx_trades_closed_exit_ts_ms_rm partial index.
      - Uses config_json ? 'indicators' (GIN key-existence) which is index-able
        via idx_trades_closed_config_gin, faster than ->key IS NOT NULL.
      - The partial index already filters r_multiple IS NOT NULL so the planner
        can do an Index-Only Scan for the r_multiple value.
    """
    # exit_ts_ms is epoch milliseconds; convert hours to ms for the WHERE clause
    query = """
    SELECT
        config_json,
        r_multiple
    FROM trades_closed
    WHERE
        exit_ts_ms >= (EXTRACT(EPOCH FROM NOW()) - %s * 3600) * 1000
        AND config_json ? 'indicators'
        AND r_multiple IS NOT NULL
    """

    signals_data = []

    if psycopg2 is None:
        logger.error("psycopg2 is not installed")
        return []

    try:
        with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (hours,))
            rows = cur.fetchall()

        for row in rows:
            config_json = row.get('config_json') or {}
            indicators = config_json.get('indicators') or {}

            if not indicators:
                continue

            raw_conf_str = indicators.get('confidence_v1') or indicators.get('confidence')
            if raw_conf_str is None:
                continue

            try:
                raw_conf = float(raw_conf_str)
                r_multiple = float(row.get('r_multiple'))
            except (ValueError, TypeError):
                continue

            label = 1 if r_multiple > 0 else 0

            # Context for bucketing
            context = {
                "session_bucket": indicators.get("session_bucket") or indicators.get("session") or indicators.get("sessionBucket"),
                "regime_bucket": indicators.get("regime_bucket") or indicators.get("regime") or indicators.get("regimeBucket"),
                "symbol": config_json.get("symbol") or indicators.get("symbol") or indicators.get("sym"),
            }

            signals_data.append({
                "raw_conf": raw_conf,
                "label": label,
                "context": context
            })

    except Exception as e:
        logger.error(f"Failed to fetch signals: {e}")
        return []

    return signals_data

# ---------------------------------------------------------------------------
# Calibration Runtime (Mini-version of ConfidenceCalibratorBundleRuntime)
# ---------------------------------------------------------------------------
class SimpleBundleRuntime:
    def __init__(self, bundle: dict[str, Any]):
        self.bundle = bundle
        self.meta = bundle.get("meta", {})
        self.buckets = bundle.get("buckets", {})
        self.bucket_by = self.meta.get("bucket_by", "none")

    def predict(self, raw_conf: float, context: dict[str, Any]) -> float:
        # Determine Bucket Key
        bkey = "global"
        if self.bucket_by == "session":
            bkey = (context.get("session", "OFF"))
        elif self.bucket_by == "regime":
            bkey = (context.get("regime", "neutral"))
        elif self.bucket_by == "session_regime":
            bkey = f"{(context.get('session', 'OFF'))}_{(context.get('regime', 'neutral'))}"
        elif self.bucket_by == "symbol":
            bkey = (context.get("symbol", "unknown"))

        # Find Calibrator
        cal_cfg = self.buckets.get(bkey)
        if not cal_cfg:
            cal_cfg = self.buckets.get("global")
            if not cal_cfg:
                return raw_conf

        # Apply Method
        method = cal_cfg.get("method", "identity")
        params = cal_cfg.get("params", {})

        val = raw_conf

        if method == "platt":
            a = float(params.get("a", 1.0))
            b = float(params.get("b", 0.0))
            logit = a * raw_conf + b
            logit = max(-100.0, min(100.0, logit))
            val = 1.0 / (1.0 + math.exp(-logit))
        elif method == "isotonic":
            # Simplified isotonic (nearest/linear)
            boundaries = params.get("boundaries", [])
            values = params.get("values", [])
            if boundaries:
                 # Linear interp
                 x = raw_conf
                 if x <= boundaries[0]: val = values[0]
                 elif x >= boundaries[-1]: val = values[-1]
                 else:
                     for i in range(len(boundaries) - 1):
                         if boundaries[i] <= x <= boundaries[i+1]:
                             x0, x1 = boundaries[i], boundaries[i+1]
                             y0, y1 = values[i], values[i+1]
                             if x1 != x0:
                                 val = y0 + (x - x0) * (y1 - y0) / (x1 - x0)
                             else:
                                 val = y0
                             break
        # Add other methods if needed...

        return max(0.0, min(1.0, val))

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def compute_metrics(y_true: list[int], y_prob: list[float]) -> dict[str, float]:
    if not y_true:
        return {
            "ece": 1.0,
            "mce": 1.0,
            "brier": 1.0,
            "calibration_slope": float("nan"),
            "calibration_intercept": float("nan"),
            "sharpness_mean": 0.0,
            "sharpness_entropy": 1.0,
            "prob_mass_near_half": 1.0,
            "precision_top5p": 0.0,
            "n": 0,
        }

    ext = extended_calibration_report(y_true, y_prob, bins=20, near_half_width=0.05)
    n = len(y_true)
    desc_indices = sorted(range(n), key=lambda i: y_prob[i], reverse=True)
    top_n = max(1, int(n * 0.05))
    top_indices = desc_indices[:top_n]
    hits = sum(y_true[i] for i in top_indices)
    prec_top5p = hits / top_n

    return {
        "ece": float(ext.get("ece", float("nan"))),
        "mce": float(ext.get("mce", float("nan"))),
        "brier": float(ext.get("brier", float("nan"))),
        "calibration_slope": float(ext.get("calibration_slope", float("nan"))),
        "calibration_intercept": float(ext.get("calibration_intercept", float("nan"))),
        "sharpness_mean": float(ext.get("sharpness_mean", float("nan"))),
        "sharpness_entropy": float(ext.get("sharpness_entropy", float("nan"))),
        "prob_mass_near_half": float(ext.get("prob_mass_near_half", float("nan"))),
        "precision_top5p": prec_top5p,
        "n": n,
    }

def _as_str(x: Any) -> str:
    if x is None:
        return "na"
    s = str(x).strip()
    return s if s else "na"

def _cohort_key(ctx: dict[str, Any]) -> str:
    sym = _as_str(ctx.get("symbol") or ctx.get("sym") or ctx.get("ticker") or "na").upper()
    session = _as_str(ctx.get("session_bucket") or ctx.get("session") or ctx.get("sess") or "na")
    regime = _as_str(ctx.get("regime_bucket") or ctx.get("regime") or "na")
    return f"{sym}|{session}|{regime}"

def compute_cohort_deltas(
    data: list[dict[str, Any]],
    *,
    champ_probs: list[float],
    cand_probs: list[float],
    min_n_cohort: int,
    top_k: int,
) -> dict[str, Any]:
    """Matched-cohort evaluation (world practice).

    Candidate vs champion on the *same* labeled samples; then aggregate deltas
    across cohorts (symbol×session×regime) to reduce mix/regime bias.
    """
    n = min(len(data), len(champ_probs), len(cand_probs))
    if n <= 0:
        return {"items": [], "agg": {"cohort_n": 0, "n": 0}, "worst": {}}

    groups: dict[str, list[int]] = {}
    for i in range(n):
        ctx = data[i].get("context") or {}
        if not isinstance(ctx, dict):
            ctx = {}
        k = _cohort_key(ctx)
        groups.setdefault(k, []).append(i)

    items: list[dict[str, Any]] = []
    w_sum = 0.0
    w_ece = 0.0
    w_brier = 0.0
    worst_key = None
    worst_n = 0
    worst_ece = None
    worst_brier = None

    for k, idxs in groups.items():
        nn = len(idxs)
        if nn <= 0:
            continue
        y = [int(data[i].get("label") or 0) for i in idxs]
        p_ch = [float(champ_probs[i]) for i in idxs]
        p_ca = [float(cand_probs[i]) for i in idxs]

        m_ch = compute_metrics(y, p_ch)
        m_ca = compute_metrics(y, p_ca)

        d_ece = float(m_ca["ece"]) - float(m_ch["ece"])
        d_brier = float(m_ca["brier"]) - float(m_ch["brier"])

        items.append({
            "key": k,
            "n": int(nn),
            "champ": m_ch,
            "cand": m_ca,
            "delta": {"ece_cal": d_ece, "brier_cal": d_brier},
        })

        if nn >= int(min_n_cohort):
            w = float(nn)
            w_sum += w
            w_ece += w * d_ece
            w_brier += w * d_brier
            if worst_ece is None or d_ece > float(worst_ece):
                worst_key = k
                worst_n = int(nn)
                worst_ece = float(d_ece)
            if worst_brier is None or d_brier > float(worst_brier):
                worst_brier = float(d_brier)

    items.sort(key=lambda r: (-int(r.get("n", 0)), -float(r.get("delta", {}).get("ece_cal", 0.0))))
    if top_k > 0 and len(items) > int(top_k):
        items = items[: int(top_k)]

    agg: dict[str, Any] = {
        "cohort_n": sum(1 for r in items if int(r.get("n", 0)) >= int(min_n_cohort)),
        "n": n,
        "min_n_cohort": int(min_n_cohort),
    }
    if w_sum > 0.0:
        agg["delta_ece_cal_wmean"] = float(w_ece / w_sum)
        agg["delta_brier_cal_wmean"] = float(w_brier / w_sum)

    worst: dict[str, Any] = {}
    if worst_key is not None:
        worst = {
            "key": str(worst_key),
            "n": int(worst_n),
            "delta_ece_cal_max": float(worst_ece) if worst_ece is not None else None,
            "delta_brier_cal_max": float(worst_brier) if worst_brier is not None else None,
        }

    return {"items": items, "agg": agg, "worst": worst}

# ---------------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Do not overwrite champion bundle")
    parser.add_argument("--force", action="store_true", help="Ignore degradation checks (dangerous)")
    parser.add_argument("--variant", default="v1", help="Variant tag")
    args = parser.parse_args()

    # Paths
    champ_path = os.getenv("CONF_CAL_CHAMPION_BUNDLE_PATH")
    cand_path = os.getenv("CONF_CAL_CANDIDATE_BUNDLE_PATH")
    proof_path = os.getenv("CONF_CAL_PROOF_STATE_PATH", "/tmp/conf_cal_proof_state.json")
    status_path = os.getenv("CONF_CAL_PROMOTION_STATUS_PATH", "/tmp/conf_cal_promo_status.json")
    dsn = os.getenv("SIGNALS_PG_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))

    if not dsn:
        logger.error("Missing SIGNALS_PG_DSN/PG_DSN env var")
        sys.exit(1)

    if not cand_path or not os.path.exists(cand_path):
        logger.error(f"Candidate bundle not found: {cand_path}")
        sys.exit(1)

    # 1. Load Bundles
    try:
        with open(cand_path) as f:
            cand_bundle = json.load(f)
    except Exception as e:
        logger.error(f"Error loading candidate bundle: {e}")
        sys.exit(1)

    champ_bundle = None
    if champ_path and os.path.exists(champ_path):
        try:
            with open(champ_path) as f:
                champ_bundle = json.load(f)
        except Exception as e:
            logger.warning(f"Error loading champion bundle (will treat as missing): {e}")

    # 2. Fetch Data
    logger.info("Fetching signals data...")
    data = fetch_recent_signals(dsn)
    if not data:
        logger.error("No signals data found in last 24h")
        sys.exit(0) # Not an error, just nothing to do (or DB issue handled inside fetch)

    logger.info(f"Fetched {len(data)} signals")

    # 3. Evaluate Candidate
    cand_rt = SimpleBundleRuntime(cand_bundle)
    cand_probs = [cand_rt.predict(d["raw_conf"], d["context"]) for d in data]
    labels = [d["label"] for d in data]

    cand_metrics = compute_metrics(labels, cand_probs)

    # 4. Evaluate Champion (if exists)
    champ_metrics = None
    if champ_bundle:
        champ_rt = SimpleBundleRuntime(champ_bundle)
        champ_probs = [champ_rt.predict(d["raw_conf"], d["context"]) for d in data]
        champ_metrics = compute_metrics(labels, champ_probs)
    else:
        # Fake terrible metrics to ensure promotion. Keep champion probs aligned
        # to candidate samples so cohort/arm bookkeeping remains deterministic.
        champ_probs = list(cand_probs)
        champ_metrics = {"ece": 1.0, "mce": 1.0, "brier": 1.0, "calibration_slope": float("nan"), "calibration_intercept": float("nan"), "sharpness_mean": 0.0, "sharpness_entropy": 1.0, "prob_mass_near_half": 1.0, "precision_top5p": 0.0, "n": len(data)}

    # 5. Check Thresholds (P2: extended metrics added)
    thresholds = {
        "min_n": float(os.getenv("CONF_CAL_PROOF_MIN_N_24H", DEFAULT_MIN_N_24H)),
        "max_ece": float(os.getenv("CONF_CAL_PROOF_MAX_ECE_24H", DEFAULT_MAX_ECE)),
        "max_mce": float(os.getenv("CONF_CAL_PROOF_MAX_MCE_24H", "0.12")),
        "max_brier": float(os.getenv("CONF_CAL_PROOF_MAX_BRIER_24H", DEFAULT_MAX_BRIER)),
        "min_prec": float(os.getenv("CONF_CAL_PROOF_MIN_PREC_TOP5P_24H", DEFAULT_MIN_PREC_TOP5P)),
        "min_cal_slope": float(os.getenv("CONF_CAL_PROOF_MIN_CAL_SLOPE_24H", "0.70")),
        "max_abs_cal_intercept": float(os.getenv("CONF_CAL_PROOF_MAX_ABS_CAL_INTERCEPT_24H", "0.20")),
        "min_sharpness_mean": float(os.getenv("CONF_CAL_PROOF_MIN_SHARPNESS_MEAN_24H", "0.02")),
        "max_prob_mass_near_half": float(os.getenv("CONF_CAL_PROOF_MAX_PROB_MASS_NEAR_HALF_24H", "0.60")),
        "min_delta_ece": float(os.getenv("CONF_CAL_PROMO_MIN_DELTA_ECE", DEFAULT_MIN_DELTA_ECE)),
        "max_mce_regression": float(os.getenv("CONF_CAL_PROMO_MAX_MCE_REGRESSION", "0.002")),
        "max_sharpness_drop": float(os.getenv("CONF_CAL_PROMO_MAX_SHARPNESS_DROP", "0.05")),
    }

    # Validation (Proof) of Candidate
    is_valid = True
    reasons = []

    if cand_metrics["n"] < thresholds["min_n"]:
        is_valid = False
        reasons.append(f"n={cand_metrics['n']} < {thresholds['min_n']}")
    if cand_metrics["ece"] > thresholds["max_ece"]:
        is_valid = False
        reasons.append(f"ece={cand_metrics['ece']:.4f} > {thresholds['max_ece']}")
    if cand_metrics["mce"] > thresholds["max_mce"]:
        is_valid = False
        reasons.append(f"mce={cand_metrics['mce']:.4f} > {thresholds['max_mce']}")
    if cand_metrics["brier"] > thresholds["max_brier"]:
        is_valid = False
        reasons.append(f"brier={cand_metrics['brier']:.4f} > {thresholds['max_brier']}")
    if cand_metrics["precision_top5p"] < thresholds["min_prec"]:
        is_valid = False
        reasons.append(f"prec={cand_metrics['precision_top5p']:.4f} < {thresholds['min_prec']}")
    if math.isfinite(float(cand_metrics.get("calibration_slope", float("nan")))) and cand_metrics["calibration_slope"] < thresholds["min_cal_slope"]:
        is_valid = False
        reasons.append(f"calibration_slope={cand_metrics['calibration_slope']:.4f} < {thresholds['min_cal_slope']}")
    if math.isfinite(float(cand_metrics.get("calibration_intercept", float("nan")))) and abs(cand_metrics["calibration_intercept"]) > thresholds["max_abs_cal_intercept"]:
        is_valid = False
        reasons.append(f"abs(calibration_intercept)={abs(cand_metrics['calibration_intercept']):.4f} > {thresholds['max_abs_cal_intercept']}")
    if math.isfinite(float(cand_metrics.get("sharpness_mean", float("nan")))) and cand_metrics["sharpness_mean"] < thresholds["min_sharpness_mean"]:
        is_valid = False
        reasons.append(f"sharpness_mean={cand_metrics['sharpness_mean']:.4f} < {thresholds['min_sharpness_mean']}")
    if math.isfinite(float(cand_metrics.get("prob_mass_near_half", float("nan")))) and cand_metrics["prob_mass_near_half"] > thresholds["max_prob_mass_near_half"]:
        is_valid = False
        reasons.append(f"prob_mass_near_half={cand_metrics['prob_mass_near_half']:.4f} > {thresholds['max_prob_mass_near_half']}")

    degrade_review = False
    degrade_review_reasons: list[str] = []

    # Promotion Decision
    should_promote = False
    promo_reason = ""

    if args.force:
        should_promote = True
        promo_reason = "FORCE"
    elif not is_valid:
        should_promote = False
        promo_reason = f"Candidate invalid: {', '.join(reasons)}"
    else:
        ece_imp = champ_metrics["ece"] - cand_metrics["ece"]
        mce_reg = cand_metrics["mce"] - champ_metrics["mce"]
        sharp_drop = champ_metrics.get("sharpness_mean", float("nan")) - cand_metrics.get("sharpness_mean", float("nan"))
        if math.isfinite(mce_reg) and mce_reg > thresholds["max_mce_regression"]:
            should_promote = False
            degrade_review = True
            degrade_review_reasons.append(f"mce_regression={mce_reg:.4f}")
            promo_reason = f"MCE regression {mce_reg:.4f} > {thresholds['max_mce_regression']}"
        elif math.isfinite(sharp_drop) and sharp_drop > thresholds["max_sharpness_drop"]:
            should_promote = False
            degrade_review = True
            degrade_review_reasons.append(f"sharpness_drop={sharp_drop:.4f}")
            promo_reason = f"Sharpness drop {sharp_drop:.4f} > {thresholds['max_sharpness_drop']}"
        elif ece_imp > thresholds["min_delta_ece"]:
            should_promote = True
            promo_reason = f"Improvement ECE {ece_imp:.4f} > {thresholds['min_delta_ece']}"
        elif champ_metrics["n"] < 10:
            should_promote = True
            promo_reason = "First valid champion"
        else:
            should_promote = False
            promo_reason = f"No significant improvement (ECE delta {ece_imp:.4f})"

    logger.info(f"Candidate Metrics: {cand_metrics}")
    logger.info(f"Champion Metrics: {champ_metrics}")
    logger.info(f"Valid: {is_valid}, Promote: {should_promote}, Reason: {promo_reason}")

    # 6. Act (Promote)
    promoted = False
    if should_promote and not args.dry_run and champ_path:
        try:
            # Atomic swap simulation: write candidate to temp, rename to champion
            # Actually, shutil.copy matches the "cp" logic.
            # Backup old champion
            if os.path.exists(champ_path):
                backup = champ_path + f".bak.{int(time.time())}"
                shutil.copy2(champ_path, backup)

            # Copy candidate to champion
            shutil.copy2(cand_path, champ_path)
            promoted = True
            logger.info(f"Promoted candidate to {champ_path}")
        except Exception as e:
            logger.error(f"Failed to promote: {e}")
            promoted = False

    # 7. Write Proof State (guardrails / autopromo friendly)
    # World practice additions:
    # - arm-aware metrics: candidate vs champion on identical samples
    # - matched-cohort deltas: symbol×session×regime (reduces mix bias)

    min_n_cohort = int(os.getenv("CONF_CAL_COHORT_MIN_N", "200"))
    top_k = int(os.getenv("CONF_CAL_COHORT_TOP_K", "20"))
    cohort_report = compute_cohort_deltas(
        data,
        champ_probs=champ_probs,
        cand_probs=cand_probs,
        min_n_cohort=min_n_cohort,
        top_k=top_k,
    )

    arm_delta = {
        "ece_cal": float(cand_metrics["ece"]) - float(champ_metrics["ece"]),
        "mce_cal": float(cand_metrics["mce"]) - float(champ_metrics["mce"]),
        "brier_cal": float(cand_metrics["brier"]) - float(champ_metrics["brier"]),
        "precision_top5p": float(cand_metrics["precision_top5p"]) - float(champ_metrics["precision_top5p"]),
        "sharpness_mean": float(cand_metrics.get("sharpness_mean", float("nan"))) - float(champ_metrics.get("sharpness_mean", float("nan"))),
        "prob_mass_near_half": float(cand_metrics.get("prob_mass_near_half", float("nan"))) - float(champ_metrics.get("prob_mass_near_half", float("nan"))),
        "n": int(min(cand_metrics["n"], champ_metrics["n"])),
    }

    now_ms = get_ny_time_millis()
    active_metrics = cand_metrics if (promoted or not champ_bundle) else champ_metrics
    proof_valid = is_valid
    proof_reasons = reasons

    active_version = cand_bundle.get("version") if (promoted or not champ_bundle) else (champ_bundle.get("version") if champ_bundle else "none")
    active_arm = {
        "ece_cal": float(active_metrics["ece"]),
        "mce_cal": float(active_metrics.get("mce", float("nan"))),
        "brier_cal": float(active_metrics["brier"]),
        "precision_top5p": float(active_metrics["precision_top5p"]),
        "calibration_slope": float(active_metrics.get("calibration_slope", float("nan"))),
        "calibration_intercept": float(active_metrics.get("calibration_intercept", float("nan"))),
        "sharpness_mean": float(active_metrics.get("sharpness_mean", float("nan"))),
        "sharpness_entropy": float(active_metrics.get("sharpness_entropy", float("nan"))),
        "prob_mass_near_half": float(active_metrics.get("prob_mass_near_half", float("nan"))),
        "n": int(active_metrics["n"]),
    }
    champion_arm = {
        "ece_cal": float(champ_metrics["ece"]),
        "mce_cal": float(champ_metrics.get("mce", float("nan"))),
        "brier_cal": float(champ_metrics["brier"]),
        "precision_top5p": float(champ_metrics["precision_top5p"]),
        "calibration_slope": float(champ_metrics.get("calibration_slope", float("nan"))),
        "calibration_intercept": float(champ_metrics.get("calibration_intercept", float("nan"))),
        "sharpness_mean": float(champ_metrics.get("sharpness_mean", float("nan"))),
        "sharpness_entropy": float(champ_metrics.get("sharpness_entropy", float("nan"))),
        "prob_mass_near_half": float(champ_metrics.get("prob_mass_near_half", float("nan"))),
        "n": int(champ_metrics["n"]),
    }
    challenger_arm = {
        "ece_cal": float(cand_metrics["ece"]),
        "mce_cal": float(cand_metrics.get("mce", float("nan"))),
        "brier_cal": float(cand_metrics["brier"]),
        "precision_top5p": float(cand_metrics["precision_top5p"]),
        "calibration_slope": float(cand_metrics.get("calibration_slope", float("nan"))),
        "calibration_intercept": float(cand_metrics.get("calibration_intercept", float("nan"))),
        "sharpness_mean": float(cand_metrics.get("sharpness_mean", float("nan"))),
        "sharpness_entropy": float(cand_metrics.get("sharpness_entropy", float("nan"))),
        "prob_mass_near_half": float(cand_metrics.get("prob_mass_near_half", float("nan"))),
        "n": int(cand_metrics["n"]),
    }

    proof_data = {
        "ts_ms": now_ms,
        "generated_at": datetime.now(UTC).isoformat(),
        "degrade": 0 if proof_valid else 1,
        "degrade_review": bool(degrade_review),
        "degrade_review_reasons": list(degrade_review_reasons),
        "status": {
            "degrade": 0 if proof_valid else 1,
            "valid": bool(proof_valid),
            "reasons": proof_reasons if not proof_valid else [],
            "promoted_just_now": bool(promoted),
            "promotion_reason": promo_reason,
            "degrade_review": bool(degrade_review),
            "degrade_review_reasons": list(degrade_review_reasons),
        },
        **active_arm,
        "bundle_version": active_version,
        "champion_version": champ_bundle.get("version") if champ_bundle else "none",
        "candidate_version": cand_bundle.get("version") if cand_bundle else "none",
        "GLOBAL": {
            "ts_ms": now_ms,
            **active_arm,
            "champion": champion_arm,
            "challenger": challenger_arm,
            "delta": arm_delta,
            "cohorts": {"agg": cohort_report.get("agg") or {}, "worst": cohort_report.get("worst") or {}},
        },
        "arms": {"active": active_arm, "champion": champion_arm, "challenger": challenger_arm, "delta": arm_delta},
        "cohorts": cohort_report,
        "metrics": active_metrics,
        "valid": bool(proof_valid),
        "thresholds": thresholds,
    }

    try:
        with open(proof_path + ".tmp", "w") as f:
            json.dump(proof_data, f)
        os.rename(proof_path + ".tmp", proof_path)
        logger.info(f"Written proof state to {proof_path}")
    except Exception as e:
        logger.error(f"Failed to write proof: {e}")

    # 8. Write Status (for Ops + exporter)
    status_data = {
        "last_run": int(time.time()),
        "promoted": promoted,
        "reason": promo_reason,
        "degrade_review": bool(degrade_review),
        "degrade_review_reasons": list(degrade_review_reasons),
        "candidate_metrics": cand_metrics,
        "champion_metrics": champ_metrics,
        "delta": arm_delta,
        "thresholds": thresholds,
    }
    try:
        with open(status_path, "w") as f:
            json.dump(status_data, f)
    except Exception:
        pass

if __name__ == "__main__":
    main()
