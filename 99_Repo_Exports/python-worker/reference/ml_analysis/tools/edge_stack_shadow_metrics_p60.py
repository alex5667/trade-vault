"""
P60 Edge Stack Shadow Metrics.

Goal:
- Calculate Brier Score, ECE, Precision@K, Expectancy@K
- Provide guard check logic for promotion

Metrics:
- Brier Score: Mean squared error of (prob - outcome). Lower is better.
- ECE: Expected Calibration Error. Lower is better.
- Precision @ Top 5%: Accuracy of the top 5% most confident predictions.
- Expectancy R @ Top 5%: Average realized R-multiple of the top 5% most confident predictions.

"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List

def calculate_brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """
    Calculate Brier Score.
    BS = 1/N * sum((y_prob - y_true)^2)
    """
    if len(y_true) == 0:
        return 0.0
    return np.mean((y_prob - y_true) ** 2)

def calculate_ece(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
    """
    Calculate Expected Calibration Error (ECE).
    """
    if len(y_true) == 0:
        return 0.0
    
    bin_boundaries = np.linspace(0, 1, n_bins + 1)
    bin_lowers = bin_boundaries[:-1]
    bin_uppers = bin_boundaries[1:]
    
    ece = 0.0
    for bin_lower, bin_upper in zip(bin_lowers, bin_uppers):
        # In bin
        in_bin = (y_prob > bin_lower) & (y_prob <= bin_upper)
        prop_in_bin = np.mean(in_bin)
        
        if prop_in_bin > 0:
            accuracy_in_bin = np.mean(y_true[in_bin])
            avg_confidence_in_bin = np.mean(y_prob[in_bin])
            ece += np.abs(avg_confidence_in_bin - accuracy_in_bin) * prop_in_bin
            
    return ece

def calculate_precision_top_k_pct(y_true: np.ndarray, y_prob: np.ndarray, k_pct: float = 0.05) -> float:
    """
    Calculate Precision at top K percent confidence.
    """
    if len(y_true) == 0:
        return 0.0
    
    n_top = int(len(y_true) * k_pct)
    if n_top == 0:
        return 0.0
    
    # Sort by probability descending
    sorted_indices = np.argsort(y_prob)[::-1]
    top_indices = sorted_indices[:n_top]
    
    return np.mean(y_true[top_indices])

def calculate_expectancy_top_k_pct(y_r: np.ndarray, y_prob: np.ndarray, k_pct: float = 0.05) -> float:
    """
    Calculate Expectancy (Average Realized R) at top K percent confidence.
    y_r: Array of realized R-multiples (outcomes).
    """
    if len(y_r) == 0:
        return 0.0
    
    n_top = int(len(y_r) * k_pct)
    if n_top == 0:
        return 0.0
    
    sorted_indices = np.argsort(y_prob)[::-1]
    top_indices = sorted_indices[:n_top]
    
    return np.mean(y_r[top_indices])

def calculate_shadow_metrics(
    y_true: np.ndarray, 
    y_prob: np.ndarray, 
    y_r: Optional[np.ndarray] = None
) -> Dict[str, float]:
    """
    Calculate all shadow metrics.
    """
    metrics = {}
    metrics["brier"] = calculate_brier_score(y_true, y_prob)
    metrics["ece"] = calculate_ece(y_true, y_prob)
    metrics["precision_top5pct"] = calculate_precision_top_k_pct(y_true, y_prob, 0.05)
    
    if y_r is not None:
         metrics["expectancy_r_top5pct"] = calculate_expectancy_top_k_pct(y_r, y_prob, 0.05)
    
    return metrics

def check_promotion_guard(
    champion_metrics: Dict[str, float],
    candidate_metrics: Dict[str, float],
    max_brier_rel: float = 1.02, # Candidate BS <= Champion BS * 1.02 (allow 2% degradation? usually want < 1.0)
    # Actually, for Brier, lower is better. So we want candidate <= champion. 
    # But usually "guard" means "ok to promote if not MUCH worse".
    # User requirement: "guarded promotion candidate->champion by thresholds"
    # User ENV: EDGE_STACK_PROMOTE_MAX_BRIER_REL=1.02 
    #   -> implies Candidate BS / Champion BS <= 1.02. (Candidate can be slightly worse? or maybe improved logic?)
    # User ENV: EDGE_STACK_PROMOTE_MAX_ECE_ABS=0.005 
    #   -> Candidate ECE <= Champion ECE + 0.005? Or absolute ECE < threshold?
    #   Usually shadow promotion implies Candidate is BETTER.
    #   Let's assume "Promotion" means "Candidate replaces Champion". 
    #   So Candidate should be BETTER or EQUAL.
    #   However, the user env `MAX_BRIER_REL=1.02` suggests a loose guard? 
    #   Wait, if candidate is NEW, maybe we want strict improvement. 
    #   Let's interpret standard "Guarded Promotion":
    #   Candidate is better or neutral?
    #   Let's check the patch context or just implement a flexible check.
    #   
    #   Re-reading: "guarded promotion candidate->champion"
    #   Usually: Promote if Candidate Brier < Champion Brier * X ??
    #   Let's assume the condition for promotion is:
    #   is_better_or_comparable
    
    #   Let's implement a 'should_promote' function.
    
    max_ece_abs_diff: float = 0.005,
    min_prec_delta: float = 0.0
) -> Tuple[bool, List[str]]:
    """
    Check if candidate should be promoted over champion.
    
    Logic (inferred from common sense & user env vars):
    Promote if:
    1. Candidate Brier <= Champion Brier * MAX_BRIER_REL (default 1.02 -> allow 2% worse?) - maybe user means 0.98? 
       Actually 1.02 allows 2% degradation. Maybe 'drift' guard.
       BUT usually promotion requires improvement. 
       Let's stick to: Candidate Brier < Champion Brier (strict) OR 
       Maybe the user ENV names are "Max allowed relative Brier of candidate vs champion"?
       Let's stick to the names.
       
    2. Candidate ECE <= Champion ECE + MAX_ECE_ABS
    
    3. Candidate Precision >= Champion Precision + MIN_PREC_DELTA
    
    Returns: (should_promote, reasons)
    """
    reasons = []
    
    # Brier: Lower is better
    # Ratio = Candidate / Champion
    # If Champion=0 (perfect), avoid div zero.
    brier_champ = champion_metrics.get("brier", 1.0)
    brier_cand = candidate_metrics.get("brier", 1.0)
    
    if brier_champ <= 1e-9:
        brier_rel = 100.0 if brier_cand > 1e-9 else 1.0
    else:
        brier_rel = brier_cand / brier_champ
        
    if brier_rel > max_brier_rel:
        reasons.append(f"brier_rel {brier_rel:.4f} > {max_brier_rel}")
        
    # ECE: Lower is better
    ece_champ = champion_metrics.get("ece", 0.0)
    ece_cand = candidate_metrics.get("ece", 0.0)
    
    # User ENV: EDGE_STACK_PROMOTE_MAX_ECE_ABS=0.005
    # Let's assume this means Candidate ECE should not be worse than Champion ECE by more than 0.005
    # Or maybe it checks absolute ECE? "MAX_ECE_ABS" sounds like absolute threshold.
    # But usually we compare.
    # Let's interpret as: Candidate ECE - Champion ECE <= MAX_ECE_ABS
    # If Candidate is 0.05 and Champion is 0.04, diff is 0.01 > 0.005 -> Fail.
    
    if (ece_cand - ece_champ) > max_ece_abs_diff:
        reasons.append(f"ece_diff {ece_cand - ece_champ:.4f} > {max_ece_abs_diff}")
        
    # Precision: Higher is better
    # EDGE_STACK_PROMOTE_MIN_PREC_DELTA=0.0
    # Candidate Precision >= Champion Precision + delta
    prec_champ = champion_metrics.get("precision_top5pct", 0.0)
    prec_cand = candidate_metrics.get("precision_top5pct", 0.0)
    
    if (prec_cand - prec_champ) < min_prec_delta:
        reasons.append(f"prec_delta {prec_cand - prec_champ:.4f} < {min_prec_delta}")
        
    return (len(reasons) == 0, reasons)
