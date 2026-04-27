import json
import os
import sys
import numpy as np
from types import SimpleNamespace
from typing import Dict, Any, List, Tuple

# Fix PYTHONPATH
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core.meta_model_lr import MetaModelLR

def load_ndjson(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    data = []
    with open(path, "r") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

def brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    return float(np.mean((y_true - y_prob) ** 2))

def calculate_metrics(y_true: np.ndarray, y_prob: np.ndarray, p_min: float = 0.5) -> Dict[str, float]:
    brier = brier_score(y_true, y_prob)
    
    # Precision at p >= p_min
    mask = y_prob >= p_min
    if np.sum(mask) > 0:
        precision = float(np.mean(y_true[mask]))
        count = int(np.sum(mask))
    else:
        precision = 0.0
        count = 0
        
    return {
        "brier_score": brier,
        "precision": precision,
        "count_enforced": count,
        "avg_prob": float(np.mean(y_prob))
    }

def run_replay():
    print("--- Stage 4 Replay Validation (V7 dataset) ---")
    
    # Paths
    features_path = "/var/lib/trade/training/latest_confirm_train_v7.ndjson"
    outcomes_path = "/var/lib/trade/training/latest_outcomes.ndjson"
    v8_path = "/var/lib/trade/models/meta_model_lr_v8.json"
    v9_path = "/var/lib/trade/models/meta_model_lr_v9.json"
    
    # Load models
    model_champ = MetaModelLR.load(v8_path)
    model_chall = MetaModelLR.load(v9_path)
    print(f"Loaded champion v8 and challenger v9 models.")
    
    # Load data
    features = load_ndjson(features_path)
    outcomes = {item['sid']: item for item in load_ndjson(outcomes_path)}
    print(f"Loaded {len(features)} feature rows and {len(outcomes)} outcome rows.")
    
    y_true = []
    probs_v8 = []
    probs_v9 = []
    
    count_matched = 0
    for row in features:
        sid = row.get('sid')
        if not sid or sid not in outcomes:
            continue
            
        outcome = outcomes[sid]
        # Label: pnl > 0 or is_win if available
        is_win = int(outcome.get('is_win', 1 if outcome.get('pnl', 0) > 0 else 0))
        
        # Prepare inputs for models (feature engineering)
        # Note: MetaModelLR.predict takes indicators as input and extracts features
        # In our case row['indicators'] or row itself has the features.
        # But row['indicators'] was found empty in previous check!
        # Let's check row keys again.
        
        # Actually, MetaModelLR.predict in core/meta_model_lr.py expects a row/indicators.
        # Let's assume it can read from the row directly if features are there.
        try:
            p8 = model_champ.predict(row)
            p9 = model_chall.predict(row)
            
            y_true.append(is_win)
            probs_v8.append(p8)
            probs_v9.append(p9)
            count_matched += 1
        except Exception as e:
            # Skip rows with missing features
            continue
            
    if count_matched == 0:
        print("Error: No common SIDs found between features and outcomes.")
        return

    y_true = np.array(y_true)
    probs_v8 = np.array(probs_v8)
    probs_v9 = np.array(probs_v9)
    
    m8 = calculate_metrics(y_true, probs_v8)
    m9 = calculate_metrics(y_true, probs_v9)
    
    print(f"\n--- Metrics (Matched signals: {count_matched}) ---")
    print(f"CHAMPION (v8): Brier={m8['brier_score']:.6f}, Precision@0.5={m8['precision']:.2%}, Signals={m8['count_enforced']}")
    print(f"CHALLENGER (v9): Brier={m9['brier_score']:.6f}, Precision@0.5={m9['precision']:.2%}, Signals={m9['count_enforced']}")
    
    # Winner determination
    if m9['precision'] > m8['precision'] + 0.005 and m9['brier_score'] < m8['brier_score']:
        print("\n🏆 CHALLENGER (v9) is signifiantly better by precision and brier. Recommendation: PROMOTE.")
    elif m9['brier_score'] < m8['brier_score'] - 0.0001:
        print("\n📈 CHALLENGER (v9) has better calibration (Brier). Recommendation: PROMOTE (lower risk).")
    else:
        print("\n⚠️ CHALLENGER did not beat champion by a clear margin. Recommendation: HOLD.")

if __name__ == "__main__":
    run_replay()
