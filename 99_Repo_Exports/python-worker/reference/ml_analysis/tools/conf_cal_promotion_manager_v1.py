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

import os
import sys
import json
import time
import logging
import argparse
import math
import shutil
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

# Setup Logging
logging.basicConfig(
    level=logging.INFO
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
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
def fetch_recent_signals(dsn: str, hours: int = 24) -> List[Dict[str, Any]]:
    """
    Fetches signals from the last N hours.
    Returns list of dicts with:
      - raw_conf: float
      - label: int (0 or 1)
      - context: dict (for bucketing)
    """
    query = """
    SELECT
        raw_ctx
    FROM signals
    WHERE
        ts_signal >= NOW() - INTERVAL '%s hours'
        AND (raw_ctx->>'indicators' IS NOT NULL OR raw_ctx->'extra'->>'indicators' IS NOT NULL)
    """
    
    signals_data = []
    
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (hours,))
                rows = cur.fetchall()
                
        for row in rows:
            raw_ctx = row.get('raw_ctx') or {}
            
            # Extract Confidence
            # Try extra->indicators first, then indicators
            indicators = raw_ctx.get('extra', {}).get('indicators')
            if not indicators:
                indicators = raw_ctx.get('indicators')
            
            if not indicators:
                continue
                
            # Prefer v1 for raw baseline? Or whatever the strategy used?
            # Usually we want raw uncalibrated confidence. 
            # If the strategy logged 'confidence_v1' or just 'confidence'.
            raw_conf = float(indicators.get('confidence_v1') or indicators.get('confidence', 0.0))
            
            # Extract Label (Outcome)
            # This depends on your outcome logging convention.
            # Assuming 'target_hit' or 'realized_R' in raw_ctx
            label = None
            
            # Option A: Explicit 'outcome' in extra
            outcome = raw_ctx.get('extra', {}).get('outcome')
            if outcome:
                 if 'target_hit' in outcome:
                     label = 1 if outcome['target_hit'] else 0
            
            # Option B: 'result' in extra
            if label is None:
                res = raw_ctx.get('extra', {}).get('result')
                if res and 'realized_R' in res:
                     label = 1 if float(res['realized_R']) > 0 else 0
            
            # Option C: generic 'target_hit' in indicators (some versions)
            if label is None and 'target_hit' in indicators:
                label = 1 if indicators['target_hit'] else 0

            if label is None:
                continue

            # Context for bucketing
            context = {
                "session_bucket": indicators.get("session_bucket") or indicators.get("session") or indicators.get("sessionBucket")
                "regime_bucket": indicators.get("regime_bucket") or indicators.get("regime") or indicators.get("regimeBucket")
                "symbol": (raw_ctx.get("symbol") or indicators.get("symbol") or indicators.get("sym"))
            }

            signals_data.append({
                "raw_conf": raw_conf
                "label": label
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
    def __init__(self, bundle: Dict[str, Any]):
        self.bundle = bundle
        self.meta = bundle.get("meta", {})
        self.buckets = bundle.get("buckets", {})
        self.bucket_by = self.meta.get("bucket_by", "none")

    def predict(self, raw_conf: float, context: Dict[str, Any]) -> float:
        # Determine Bucket Key
        bkey = "global"
        if self.bucket_by == "session":
            bkey = str(context.get("session", "OFF"))
        elif self.bucket_by == "regime":
            bkey = str(context.get("regime", "neutral"))
        elif self.bucket_by == "session_regime":
            bkey = f"{str(context.get('session', 'OFF'))}_{str(context.get('regime', 'neutral'))}"
        elif self.bucket_by == "symbol":
            bkey = str(context.get("symbol", "unknown"))

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
def compute_metrics(y_true: List[int], y_prob: List[float]) -> Dict[str, float]:
    if not y_true:
        return {"ece": 1.0, "brier": 1.0, "precision_top5p": 0.0, "n": 0}

    n = len(y_true)
    
    # Brier Score: mean((prob - outcome)^2)
    brier = sum((p - y)**2 for p, y in zip(y_prob, y_true)) / n
    
    # ECE (Equal Mass, 10 bins)
    # Simple implementation
    indices = sorted(range(n), key=lambda i: y_prob[i])
    y_true_sorted = [y_true[i] for i in indices]
    y_prob_sorted = [y_prob[i] for i in indices]
    
    bin_size = max(1, n // 10)
    ece_sum = 0.0
    for i in range(0, n, bin_size):
        end = min(i + bin_size, n)
        chunk_y = y_true_sorted[i:end]
        chunk_p = y_prob_sorted[i:end]
        if not chunk_y: continue
        
        avg_p = sum(chunk_p) / len(chunk_p)
        avg_y = sum(chunk_y) / len(chunk_y)
        ece_sum += len(chunk_y) * abs(avg_p - avg_y)
        
    ece = ece_sum / n
    
    # Precision Top 5%
    # Sort descending by prob
    desc_indices = sorted(range(n), key=lambda i: y_prob[i], reverse=True)
    top_n = max(1, int(n * 0.05))
    top_indices = desc_indices[:top_n]
    
    hits = sum(y_true[i] for i in top_indices)
    prec_top5p = hits / top_n
    
    return {
        "ece": ece
        "brier": brier
        "precision_top5p": prec_top5p
        "n": n
    }

def _as_str(x: Any) -> str:
    if x is None:
        return "na"
    s = str(x).strip()
    return s if s else "na"

def _cohort_key(ctx: Dict[str, Any]) -> str:
    sym = _as_str(ctx.get("symbol") or ctx.get("sym") or ctx.get("ticker") or "na").upper()
    session = _as_str(ctx.get("session_bucket") or ctx.get("session") or ctx.get("sess") or "na")
    regime = _as_str(ctx.get("regime_bucket") or ctx.get("regime") or "na")
    return f"{sym}|{session}|{regime}"

def compute_cohort_deltas(
    data: List[Dict[str, Any]]
    *
    champ_probs: List[float]
    cand_probs: List[float]
    min_n_cohort: int
    top_k: int
) -> Dict[str, Any]:
    """Matched-cohort evaluation (world practice).

    Candidate vs champion on the *same* labeled samples; then aggregate deltas
    across cohorts (symbol×session×regime) to reduce mix/regime bias.
    """
    n = min(len(data), len(champ_probs), len(cand_probs))
    if n <= 0:
        return {"items": [], "agg": {"cohort_n": 0, "n": 0}, "worst": {}}

    groups: Dict[str, List[int]] = {}
    for i in range(n):
        ctx = data[i].get("context") or {}
        if not isinstance(ctx, dict):
            ctx = {}
        k = _cohort_key(ctx)
        groups.setdefault(k, []).append(i)

    items: List[Dict[str, Any]] = []
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
            "key": k
            "n": int(nn)
            "champ": m_ch
            "cand": m_ca
            "delta": {"ece_cal": d_ece, "brier_cal": d_brier}
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

    agg: Dict[str, Any] = {
        "cohort_n": sum(1 for r in items if int(r.get("n", 0)) >= int(min_n_cohort))
        "n": n
        "min_n_cohort": int(min_n_cohort)
    }
    if w_sum > 0.0:
        agg["delta_ece_cal_wmean"] = float(w_ece / w_sum)
        agg["delta_brier_cal_wmean"] = float(w_brier / w_sum)

    worst: Dict[str, Any] = {}
    if worst_key is not None:
        worst = {
            "key": str(worst_key)
            "n": int(worst_n)
            "delta_ece_cal_max": float(worst_ece) if worst_ece is not None else None
            "delta_brier_cal_max": float(worst_brier) if worst_brier is not None else None
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
        with open(cand_path, "r") as f:
            cand_bundle = json.load(f)
    except Exception as e:
        logger.error(f"Error loading candidate bundle: {e}")
        sys.exit(1)

    champ_bundle = None
    if champ_path and os.path.exists(champ_path):
        try:
            with open(champ_path, "r") as f:
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
        # Fake terrible metrics to ensure promotion
        champ_metrics = {"ece": 1.0, "brier": 1.0, "precision_top5p": 0.0, "n": len(data)}

    # 5. Check Thresholds
    thresholds = {
        "min_n": float(os.getenv("CONF_CAL_PROOF_MIN_N_24H", DEFAULT_MIN_N_24H))
        "max_ece": float(os.getenv("CONF_CAL_PROOF_MAX_ECE_24H", DEFAULT_MAX_ECE))
        "max_brier": float(os.getenv("CONF_CAL_PROOF_MAX_BRIER_24H", DEFAULT_MAX_BRIER))
        "min_prec": float(os.getenv("CONF_CAL_PROOF_MIN_PREC_TOP5P_24H", DEFAULT_MIN_PREC_TOP5P))
        "min_delta_ece": float(os.getenv("CONF_CAL_PROMO_MIN_DELTA_ECE", DEFAULT_MIN_DELTA_ECE))
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
    if cand_metrics["brier"] > thresholds["max_brier"]:
        is_valid = False
        reasons.append(f"brier={cand_metrics['brier']:.4f} > {thresholds['max_brier']}")
    if cand_metrics["precision_top5p"] < thresholds["min_prec"]:
        is_valid = False
        reasons.append(f"prec={cand_metrics['precision_top5p']:.4f} < {thresholds['min_prec']}")

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
        # Check against champion
        ece_imp = champ_metrics["ece"] - cand_metrics["ece"]
        if ece_imp > thresholds["min_delta_ece"]:
            should_promote = True
            promo_reason = f"Improvement ECE {ece_imp:.4f} > {thresholds['min_delta_ece']}"
        elif champ_metrics["n"] < 10: # No valid champion
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
        data
        champ_probs=champ_probs
        cand_probs=cand_probs
        min_n_cohort=min_n_cohort
        top_k=top_k
    )

    arm_delta = {
        "ece_cal": float(cand_metrics["ece"]) - float(champ_metrics["ece"])
        "brier_cal": float(cand_metrics["brier"]) - float(champ_metrics["brier"])
        "precision_top5p": float(cand_metrics["precision_top5p"]) - float(champ_metrics["precision_top5p"])
        "n": int(min(cand_metrics["n"], champ_metrics["n"]))
    }

    now_ms = get_ny_time_millis()
    active_metrics = cand_metrics if promoted else (champ_metrics if champ_metrics else cand_metrics)
    proof_valid = is_valid
    proof_reasons = reasons

    proof_data = {
        "ts_ms": now_ms
        "generated_at": datetime.now(timezone.utc).isoformat()
        "degrade": 0 if proof_valid else 1
        "status": {
            "degrade": 0 if proof_valid else 1
            "valid": bool(proof_valid)
            "reasons": proof_reasons if not proof_valid else []
            "promoted_just_now": bool(promoted)
            "promotion_reason": promo_reason
        }
        "ece_cal": float(active_metrics["ece"])
        "brier_cal": float(active_metrics["brier"])
        "precision_top5p": float(active_metrics["precision_top5p"])
        "n": int(active_metrics["n"])
        "bundle_version": cand_bundle.get("version") if promoted else (champ_bundle.get("version") if champ_bundle else "none")
        "champion_version": champ_bundle.get("version") if champ_bundle else "none"
        "candidate_version": cand_bundle.get("version") if cand_bundle else "none"
        "GLOBAL": {
            "ts_ms": now_ms
            "n": int(active_metrics["n"])
            "ece_cal": float(active_metrics["ece"])
            "brier_cal": float(active_metrics["brier"])
            "precision_top5p": float(active_metrics["precision_top5p"])
            "champion": {
                "ece_cal": float(champ_metrics["ece"])
                "brier_cal": float(champ_metrics["brier"])
                "precision_top5p": float(champ_metrics["precision_top5p"])
                "n": int(champ_metrics["n"])
            }
            "challenger": {
                "ece_cal": float(cand_metrics["ece"])
                "brier_cal": float(cand_metrics["brier"])
                "precision_top5p": float(cand_metrics["precision_top5p"])
                "n": int(cand_metrics["n"])
            }
            "delta": arm_delta
            "cohorts": {"agg": cohort_report.get("agg") or {}, "worst": cohort_report.get("worst") or {}}
        }
        "arms": {"champion": proof_data_champion_placeholder, "challenger": proof_data_challenger_placeholder, "delta": arm_delta}
        "cohorts": cohort_report
        "metrics": active_metrics,  # legacy
        "valid": bool(proof_valid)
    }

    # Fix the arms referencing itself
    proof_data["arms"]["champion"] = proof_data.get("GLOBAL", {}).get("champion")
    proof_data["arms"]["challenger"] = proof_data.get("GLOBAL", {}).get("challenger")

    try:
        with open(proof_path + ".tmp", "w") as f:
            json.dump(proof_data, f)
        os.rename(proof_path + ".tmp", proof_path)
        logger.info(f"Written proof state to {proof_path}")
    except Exception as e:
        logger.error(f"Failed to write proof: {e}")

    # 8. Write Status (for Ops)
    status_data = {
        "last_run": int(time.time())
        "promoted": promoted
        "reason": promo_reason
        "candidate_metrics": cand_metrics
        "champion_metrics": champ_metrics
    }
    try:
        with open(status_path, "w") as f:
            json.dump(status_data, f)
    except Exception:
        pass

if __name__ == "__main__":
    main()
