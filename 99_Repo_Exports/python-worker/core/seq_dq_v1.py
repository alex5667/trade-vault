def surface_dq_indicators(runtime, indicators: dict) -> dict:
    """Consolidation for P2/F (Step A): ensure DQ signals available to engine."""
    indicators["tick_gap_count"] = int(getattr(runtime, "tick_id_gap_count", 0) or 0)
    indicators["tick_dup_count"] = int(getattr(runtime, "tick_id_dup_count", 0) or 0)
    indicators["tick_reorder_count"] = int(getattr(runtime, "tick_id_reorder_count", 0) or 0)
    indicators["tick_seq_last_reason"] = int(getattr(runtime, "tick_id_last_reason", 0) or 0)
    
    indicators["tick_missing_seq_ema"] = float(getattr(runtime, "tick_missing_seq_ema", 0.0) or 0.0)
    indicators["tick_gap_p50_ms"] = float(getattr(runtime, "tick_gap_p50_ms", 0.0) or 0.0)
    indicators["tick_gap_p95_ms"] = float(getattr(runtime, "tick_gap_p95_ms", 0.0) or 0.0)
    return indicators
