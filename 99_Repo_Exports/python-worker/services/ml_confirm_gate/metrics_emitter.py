import json
import logging
import time
from typing import Any, Dict
import redis

from .dto import MLConfirmDecision

logger = logging.getLogger("ml_confirm_gate.metrics")

def _stable_hash_u64(s: str) -> int:
    import hashlib
    h = hashlib.md5(s.encode("utf-8")).digest()[:8]
    return int.from_bytes(h, "big", signed=False)

def _json_safe(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            try:
                ks = str(k)
            except Exception:
                ks = "k"
            out[ks] = _json_safe(v)
        return out
    try:
        if hasattr(x, "item"):
            return _json_safe(x.item())
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return str(x)


def emit_metrics(
    r: redis.Redis,
    dec: MLConfirmDecision,
    *,
    symbol: str,
    ts_ms: int,
    direction: str,
    scenario: str,
    rule_score: float,
    rule_have: int,
    rule_need: int,
    cancel_spike_veto: int,
    ok_rule: int,
    sid: str,
    indicators: Dict[str, Any],
    metrics_stream: str,
    metrics_enable: bool,
    metrics_sample: float,
) -> None:
    if not metrics_enable or not r:
        return

    if metrics_sample < 1.0:
        h = _stable_hash_u64(sid)
        if (h % 1000000) / 1000000.0 >= metrics_sample:
            return

    payload = {
        "event_time": int(time.time() * 1000),
        "symbol": str(symbol).upper(),
        "ts_ms": int(ts_ms),
        "direction": str(direction).upper(),
        "scenario": str(scenario),
        "sid": sid,

        "rule_score": float(rule_score),
        "rule_have": int(rule_have),
        "rule_need": int(rule_need),
        "cancel_spike_veto": int(cancel_spike_veto),
        "ok_rule": int(ok_rule),

        "ml_mode": dec.effective_mode,
        "ml_kind": dec.kind,
        "ml_allow": int(dec.allow),
        "ml_status": dec.status,
        "ml_p_edge": float(dec.p_edge),
        "ml_p_min": float(dec.p_min),
        "ml_conf": float(dec.conf),
        "ml_p_margin": float(dec.p_margin),
        "ml_score": float(dec.score),
        "ml_floor": float(dec.floor),
        "ml_bucket": str(dec.bucket),
        "ml_model_run_id": dec.model_run_id,
        "ml_reason": dec.reason,
        "ml_error": dec.error,
        "ml_missing": ",".join(dec.missing) if dec.missing else "",
        "ml_latency_us": int(dec.latency_us),

        "cfg_key_used": str(dec.cfg_key_used),
        "cfg_source": str(dec.cfg_source),
        
        "ml_p_edge_raw": float(dec.p_edge_raw),
        "ml_p_edge_cal": float(dec.p_edge_cal),
        "ml_calib_type": str(dec.calib_type),
        
        "ml_exec_risk_ref_bps": float(dec.exec_risk_ref_bps),
        "ml_exec_risk_bps": float(dec.exec_risk_bps),
        "ml_exec_risk_norm": float(dec.exec_risk_norm),
        "ml_exec_pen": float(dec.exec_pen),
        "ml_score_breakdown_json": str(dec.score_breakdown_json),
    }

    try:
        # P3-FIX: XADD instead of PUBLISH to prevent data loss on collector restart
        r.xadd(metrics_stream, payload, maxlen=100000, approximate=True)
    except Exception:
        pass


def capture_replay_input(
    r: redis.Redis,
    dec: MLConfirmDecision,
    *,
    symbol: str,
    ts_ms: int,
    direction: str,
    scenario: str,
    indicators: Dict[str, Any],
    rule_score: float,
    rule_have: int,
    rule_need: int,
    cancel_spike_veto: int,
    ok_rule: int,
    replay_capture: bool,
    replay_stream: str,
    replay_sample: float,
    replay_maxlen: int,
) -> None:
    if not replay_capture or not r:
        return

    if replay_sample < 1.0:
        h = _stable_hash_u64(f"{symbol}:{ts_ms}")
        if (h % 1000000) / 1000000.0 >= replay_sample:
            return

    safe_inds = _json_safe(indicators)

    payload = {
        "event_time": int(time.time() * 1000),
        "symbol": str(symbol).upper(),
        "ts_ms": int(ts_ms),
        "direction": str(direction).upper(),
        "scenario": str(scenario),
        "rule_score": float(rule_score),
        "rule_have": int(rule_have),
        "rule_need": int(rule_need),
        "cancel_spike_veto": int(cancel_spike_veto),
        "ok_rule": int(ok_rule),
        "indicators_json": json.dumps(safe_inds, separators=(',', ':')),
        "ml_p_edge": float(dec.p_edge),
        "ml_p_min": float(dec.p_min),
        "ml_allow": int(dec.allow),
        "ml_status": dec.status,
    }
    
    try:
        r.xadd(replay_stream, payload, maxlen=replay_maxlen, approximate=True)
    except Exception:
        pass
