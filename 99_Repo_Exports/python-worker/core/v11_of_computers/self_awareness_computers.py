import math
from typing import List, Any

try:
    import numpy as np
except ImportError:
    np = None
    
def _f(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return 0.0
        return v
    except Exception:
        return 0.0

def compute_conf_ma_ratio(confidence: float, history: List[float]) -> float:
    """confidence / moving_avg_conf_24h — relative signal strength."""
    if not history:
        return 1.0  # neutral if no history
        
    hist = [h for h in history if not math.isnan(_f(h))]
    if not hist:
        return 1.0
        
    avg = sum(hist) / len(hist) 
    if avg == 0:
        return 1.0 if confidence == 0 else 999.0
        
    return min(999.0, confidence / avg)

def compute_signal_cluster_flag(signal_ts_ms: List[int], now_ms: int) -> float:
    """1.0 if 3+ signals within 60s (cluster / noise flag)."""
    if not signal_ts_ms:
        return 0.0
        
    # count how many signals in last 60s
    cutoff = now_ms - 60_000
    count = 0
    for ts in reversed(signal_ts_ms):
        if ts >= cutoff:
            count += 1
        else:
            break
            
    # 3+ signals not counting the current one (if included in history)
    # usually the current one is just generated, so history has previous ones
    if count >= 2: # >=2 in history + current = 3+ total
        return 1.0
        
    return 0.0

def compute_gate_hardness_score(gate_log: List[bool]) -> float:
    """Ratio of hard-veto gates fired vs total gates checked (0-1)."""
    if not gate_log:
        return 0.0
        
    vetos = sum(1 for g in gate_log if g is False)
    total = len(gate_log)
    
    return float(vetos / total)

def compute_model_calibration_err(predicted_confidences: List[float], realized_win_flags: List[float]) -> float:
    """|predicted_confidence - realized_win_rate| rolling 50 trades."""
    if len(predicted_confidences) < 10 or len(realized_win_flags) < 10:
        return 0.0
        
    if len(predicted_confidences) != len(realized_win_flags):
        n = min(len(predicted_confidences), len(realized_win_flags))
        if n == 0:
            return 0.0
        preds = predicted_confidences[-n:]
        wins = realized_win_flags[-n:]
    else:
        preds = predicted_confidences
        wins = realized_win_flags
        
    # use last 50 for rolling calibration
    preds = preds[-50:]
    wins = wins[-50:]
    
    avg_pred = sum(preds) / len(preds)
    win_rate = sum(wins) / len(wins)
    
    return abs(avg_pred - win_rate)
