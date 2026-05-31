#!/usr/bin/env python3
"""
Stage 4: A/B Winner Evaluator (v1)

Deterministic nightly evaluator comparing Champion vs Challenger meta-models.
"""
import argparse
import json
import logging
import os
import sys
from typing import Any

import redis

# Add python-worker to sys.path to allow imports from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from core.meta_model_lr import MetaModelLR
except ImportError:
    # Fallback for local testing or structural differences
    MetaModelLR = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ab_winner_evaluator")

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _iter_ndjson(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def compute_metrics(p: list[float], r_mult: list[float]) -> dict[str, float]:
    """Compute expectancy and tail risk from predicted probabilities and outcomes."""
    if not p:
        return {"exp_r": 0.0, "tail_risk": 0.0, "n": 0}

    n = len(p)
    # Expectancy: sum(p_i * r_i) / n?
    # Actually, the user says "expected return vs tail risk".
    # Usually in this project, r_mult is the realized return for that sample.
    # So we compute mean(r_mult) for samples where p >= p_threshold?
    # No, usually we want to evaluate the "model performance" on the whole dataset or filtered set.
    # The prompt says "A/B Winner evaluator ... at a given meta_p_min".
    # This implies we filter samples where p >= p_min and then compute mean(r_mult).

    return {
        "exp_r": sum(r_mult) / n,
        "tail_risk": sum(1.0 for r in r_mult if r <= -1.0) / n,
        "n": n
    }

def evaluate_models(
    dataset_path: str,
    p_min: float,
    model_champ: MetaModelLR,
    model_challenger: MetaModelLR
) -> dict[str, Any]:
    """Evaluate both models on the same dataset."""
    champ_p: list[float] = []
    champ_r: list[float] = []

    chall_p: list[float] = []
    chall_r: list[float] = []

    count = 0
    eligible_count = 0

    for row in _iter_ndjson(dataset_path):
        count += 1
        # Filter eligible samples (ok == 1)
        if int(_safe_float(row.get("ok"), 0)) != 1:
            continue

        eligible_count += 1

        r_mult = _safe_float(row.get("r_mult"), 0.0)
        features = row.get("indicators") or row.get("features") or row

        # Predict Champion
        p_c = model_champ.predict_proba(features)
        if p_c is not None and p_c >= p_min:
            champ_p.append(p_c)
            champ_r.append(r_mult)

        # Predict Challenger
        p_ch = model_challenger.predict_proba(features)
        if p_ch is not None and p_ch >= p_min:
            chall_p.append(p_ch)
            chall_r.append(r_mult)

    m_champ = compute_metrics(champ_p, champ_r)
    m_chall = compute_metrics(chall_p, chall_r)

    return {
        "n_total": count,
        "n_eligible": eligible_count,
        "champion": m_champ,
        "challenger": m_chall,
        "p_min": p_min
    }

def make_decision(
    metrics: dict[str, Any],
    min_delta_exp_r: float,
    tail_slack: float
) -> tuple[str, str]:
    """
    Decision logic:
    Challenger wins if Delta ExpR > min_delta AND Delta TailRisk < tail_slack.
    """
    m_champ = metrics["champion"]
    m_chall = metrics["challenger"]

    if m_champ["n"] == 0 or m_chall["n"] == 0:
        return "no_data", "Insufficient samples for comparison"

    delta_exp = m_chall["exp_r"] - m_champ["exp_r"]

    # Tail risk slack: user example "1.1x chance of R <= -1.0 fallback"
    # This suggests a ratio comparison.
    tail_champ = m_champ["tail_risk"]
    tail_chall = m_chall["tail_risk"]

    # Avoid division by zero
    if tail_champ > 0:
        tail_ratio = tail_chall / tail_champ
    else:
        # If champion has 0 tail risk, any challenger tail risk is a "risk"
        tail_ratio = 1.0 + tail_chall if tail_chall > 0 else 1.0

    exp_ok = delta_exp >= min_delta_exp_r
    risk_ok = tail_ratio <= (1.0 + tail_slack) # tail_slack e.g. 0.1 for 1.1x

    reason = f"ExpR Δ={delta_exp:.4f} (req >={min_delta_exp_r}), TailRisk ratio={tail_ratio:.2f} (max <={1.0+tail_slack:.2f})"

    if exp_ok and risk_ok:
        return "challenger", f"Challenger wins (Stable improvement): {reason}"
    if not exp_ok and risk_ok:
        return "champion", f"Champion wins (Challenger underperforms): {reason}"
    if exp_ok and not risk_ok:
        return "champion", f"Champion wins (Challenger too risky): {reason}"

    return "champion", f"Champion wins (Challenger underperforms and too risky): {reason}"

def update_redis(redis_url: str, winner: str, current_share: float, ramp_step: float, share_max: float) -> float:
    """Update meta_ab_challenger_share in Redis."""
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        new_share = current_share

        if winner == "challenger":
            new_share = min(share_max, current_share + ramp_step)
        elif winner == "champion":
            new_share = max(0.0, current_share - ramp_step)

        # Round to avoid float precision drift in Redis
        new_share = round(new_share, 4)

        # In this project, dynamic config is usually in a hash 'settings:dynamic_cfg'
        r.hset("settings:dynamic_cfg", "meta_ab_challenger_share", str(new_share))
        logger.info(f"Updated meta_ab_challenger_share in Redis: {current_share} -> {new_share}")
        return new_share
    except Exception as e:
        logger.error(f"Failed to update Redis: {e}")
        return current_share

def main():
    parser = argparse.ArgumentParser(description="A/B Winner Evaluator")
    parser.add_argument("--dataset", required=True, help="Path to ndjson dataset")
    parser.add_argument("--p-min", type=float, default=0.6, help="Deployable threshold p_min")
    parser.add_argument("--model-champion", required=True, help="Path to champion MetaModelLR JSON")
    parser.add_argument("--model-challenger", required=True, help="Path to challenger MetaModelLR JSON")
    parser.add_argument("--min-delta-exp-r", type=float, default=0.005, help="Min improvement in expected return")
    parser.add_argument("--tail-slack", type=float, default=0.1, help="Max allowable tail risk increase ratio (e.g. 0.1 for 1.1x)")
    parser.add_argument("--ramp-step", type=float, default=0.05, help="Step size for share adjustment")
    parser.add_argument("--share-max", type=float, default=0.5, help="Maximum allowed challenger share")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"), help="Redis URL")
    parser.add_argument("--apply", action="store_true", help="Apply changes to Redis")

    args = parser.parse_args()

    if MetaModelLR is None:
        logger.error("MetaModelLR class not found. Ensure core.meta_model_lr is accessible.")
        sys.exit(1)

    try:
        champ = MetaModelLR.load(args.model_champion)
        chall = MetaModelLR.load(args.model_challenger)
    except Exception as e:
        logger.error(f"Failed to load models: {e}")
        sys.exit(1)

    logger.info(f"Evaluating {args.dataset} at p_min={args.p_min}")
    results = evaluate_models(args.dataset, args.p_min, champ, chall)

    winner, explanation = make_decision(results, args.min_delta_exp_r, args.tail_slack)

    logger.info(f"Decision: {winner.upper()}")
    logger.info(f"Explanation: {explanation}")
    logger.info(f"Metrics: Champion ExpR={results['champion']['exp_r']:.5f}, Challenger ExpR={results['challenger']['exp_r']:.5f}")
    logger.info(f"Samples: Champ={results['champion']['n']}, Chall={results['challenger']['n']} (Eligible total: {results['n_eligible']})")

    if args.apply:
        try:
            r = redis.Redis.from_url(args.redis_url, decode_responses=True)
            current_share = _safe_float(r.hget("settings:dynamic_cfg", "meta_ab_challenger_share"), 0.0)
            update_redis(args.redis_url, winner, current_share, args.ramp_step, args.share_max)
        except Exception as e:
            logger.error(f"Failed to connect to Redis for current share: {e}")

if __name__ == "__main__":
    main()
