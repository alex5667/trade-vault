import math
from typing import Dict, Any, Optional

def eval_dq_gate(indicators: Dict[str, Any], cfg2: Dict[str, Any]) -> Dict[str, Any]:
    """
    P14 DQ / time-determinism gate.
    Returns {dq_pen, dq_veto, dq_reason, dq_reason_bucket, dq_health_score, dq_components}.
    """
    enable = int(cfg2.get("dq_gate_enable", 0))
    if not enable:
        return {
            "dq_pen": 0.0,
            "dq_veto": 0,
            "dq_reason": "disabled",
            "dq_reason_bucket": "ok",
            "dq_health_score": 1.0,
            "dq_components": {}
        }

    # Extract indicators with default fail-open values (NaN/Inf -> fail open)
    def _get(k: str, default: float = 0.0, aliases: Optional[list] = None) -> float:
        val = indicators.get(k)
        if val is None and aliases:
            for al in aliases:
                val = indicators.get(al)
                if val is not None:
                    break
        
        if val is None:
            return default
        try:
            fval = float(val)
            if math.isnan(fval) or math.isinf(fval):
                return default
            return fval
        except (ValueError, TypeError):
            return default

    # DQ features (v5 meta)
    data_health = _get("data_health", 1.0)
    book_health_ok = _get("book_health_ok", 1.0)
    tick_time_age_ms = _get("tick_time_age_ms", 0.0)
    
    # Skew/Desync EMAs (higher is worse)
    tick_ts_source_now_ema = _get("tick_ts_source_now_ema", 0.0)
    tick_ts_source_stream_id_ema = _get("tick_ts_source_stream_id_ema", 0.0)
    tick_event_age_abs_ema_ms = _get("tick_event_age_abs_ema_ms", 0.0)
    tick_event_stream_skew_abs_ema_ms = _get("tick_event_stream_skew_abs_ema_ms", 0.0)
    
    # Other quality flags
    tick_unknown_side_ema = _get("tick_unknown_side_ema", 0.0)

    # Thresholds from cfg2
    mode = str(cfg2.get("dq_gate_mode", "penalty")).lower()
    pen_max = float(cfg2.get("dq_pen_max", 0.10))
    
    # 1. Basic Health Check
    health_score = 1.0
    reason = "ok"
    
    # Penalize low data health
    if data_health < float(cfg2.get("dq_data_health_min", 0.85)):
        health_score *= max(0.0, data_health)
        reason = "low_data_health"
        
    # Penalize stale book
    if book_health_ok < 0.5:
        health_score *= 0.5
        reason = "book_stale"

    # 2. Latency/Skew Veto/Penalty
    latency_pen = 0.0
    age_limit = float(cfg2.get("dq_tick_age_ms_max", 5000))
    if tick_time_age_ms > age_limit:
        health_score *= 0.1
        reason = "latency_spike"

    # Skew penalties
    skew_max = float(cfg2.get("dq_skew_ema_ms_max", 1000))
    if tick_ts_source_now_ema > skew_max:
        health_score *= 0.7
        reason = "clock_skew_now"
    
    if tick_ts_source_stream_id_ema > skew_max:
        health_score *= 0.8
        reason = "stream_id_skew"

    # 3. Calculate final penalty
    # Formula: dq_pen = (1.0 - health_score) * pen_max
    dq_pen = (1.0 - health_score) * pen_max
    dq_pen = max(0.0, min(pen_max, dq_pen))

    # 4. Veto Logic
    dq_veto = 0
    if mode in ("enforce", "both", "veto"):
        hard_min = float(cfg2.get("dq_data_health_hard_min", 0.70))
        if health_score < hard_min:
            dq_veto = 1

    return {
        "dq_pen": float(dq_pen),
        "dq_veto": int(dq_veto),
        "dq_reason": str(reason),
        "dq_reason_bucket": _reason_bucket(reason),
        "dq_health_score": float(health_score),
        "dq_components": {
            "data_health": data_health,
            "book_health_ok": book_health_ok,
            "tick_time_age_ms": tick_time_age_ms,
            "skew_now": tick_ts_source_now_ema,
            "skew_stream": tick_ts_source_stream_id_ema
        }
    }

def _reason_bucket(reason: str) -> str:
    if reason == "ok": return "ok"
    if "latency" in reason or "skew" in reason: return "latency"
    if "health" in reason: return "data_health"
    if "book" in reason: return "stale"
    return "other"
