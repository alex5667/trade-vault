import collections
import dataclasses
from typing import Dict, Any, Optional
import pytest

# Simulate dependencies
@dataclasses.dataclass
class SymbolRuntime:
    symbol: str = "BTCUSDT"
    config: Dict[str, Any] = dataclasses.field(default_factory=dict)
    last_signal_ts: int = 0
    pending_payload: Optional[Dict[str, Any]] = None
    pending_score: float = 0.0
    pending_ts_ms: int = 0
    pending_replaced: int = 0
    # pressure stuff (mocked)
    pressure: Any = None 

# The logic under test (copied from crypto_orderflow_service.py for verification)
def _score_candidate(ind: Dict[str, Any]) -> float:
    try:
        of_score = float(ind.get("of_confirm_score", 0.0) or 0.0)
    except Exception:
        of_score = 0.0
    try:
        dz = abs(float(ind.get("delta_z", 0.0) or 0.0))
    except Exception:
        dz = 0.0
    try:
        obi = float(ind.get("obi_stable_secs", 0.0) or 0.0)
    except Exception:
        obi = 0.0
    try:
        ice = float(ind.get("iceberg_strict", 0.0) or 0.0)
    except Exception:
        ice = 0.0
    return 2.0 * of_score + 0.3 * dz + 0.2 * min(3.0, obi) + 0.5 * ice

def simulate_signal_emission(runtime: SymbolRuntime, tick_ts: int, indicators: Dict[str, Any], payload: Dict[str, Any]):
    # --- Cooldown (deterministic) + burst best-of ---
    cooldown_ms = int(runtime.config.get("signal_cooldown_sec", 30) or 30) * 1000
    last_ts = int(getattr(runtime, "last_signal_ts", 0) or 0)
    
    age = int(tick_ts) - last_ts if last_ts > 0 else 10**9

    if age < cooldown_ms:
        # Instead of dropping blindly, keep best candidate during cooldown window.
        cand_score = _score_candidate(indicators)
        if runtime.pending_payload is None or cand_score > float(getattr(runtime, "pending_score", 0.0) or 0.0):
            runtime.pending_payload = payload
            runtime.pending_score = float(cand_score)
            runtime.pending_ts_ms = int(tick_ts)
            runtime.pending_replaced += 1
        return None

    # Cooldown window open: if we have a better pending from burst, emit it instead.
    try:
        if runtime.pending_payload is not None:
            pending = runtime.pending_payload
            pending_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            cur_score = _score_candidate(indicators)
            # emit best
            if pending_score >= cur_score:
                payload = pending
            # clear pending either way (window opened)
            runtime.pending_payload = None
            runtime.pending_score = 0.0
            runtime.pending_ts_ms = 0
    except Exception:
        pass

    runtime.last_signal_ts = int(tick_ts)
    return payload

def test_burst_selector_logic():
    rt = SymbolRuntime()
    rt.config = {"signal_cooldown_sec": 10} # 10s cooldown
    
    # 1. First signal (valid)
    # t=1000
    p1 = {"id": "sig1"}
    ind1 = {"of_confirm_score": 1.0} # score = 2.0
    res1 = simulate_signal_emission(rt, 1000, ind1, p1)
    assert res1 == p1
    assert rt.last_signal_ts == 1000
    
    # 2. Second signal (in cooldown) - kept as pending
    # t=2000 (age=1s < 10s)
    p2 = {"id": "sig2"}
    ind2 = {"of_confirm_score": 0.5} # score = 1.0
    res2 = simulate_signal_emission(rt, 2000, ind2, p2)
    assert res2 is None
    assert rt.pending_payload == p2
    assert rt.pending_score == 1.0
    
    # 3. Third signal (in cooldown) - BETTER score -> replace pending
    # t=3000
    p3 = {"id": "sig3"}
    ind3 = {"of_confirm_score": 2.0} # score = 4.0
    res3 = simulate_signal_emission(rt, 3000, ind3, p3)
    assert res3 is None
    assert rt.pending_payload == p3
    assert rt.pending_score == 4.0
    
    # 4. Fourth signal (in cooldown) - WORSE score -> ignore (keep p3)
    # t=4000
    p4 = {"id": "sig4"}
    ind4 = {"of_confirm_score": 0.1}
    res4 = simulate_signal_emission(rt, 4000, ind4, p4)
    assert res4 is None
    assert rt.pending_payload == p3 # Still p3
    assert rt.pending_score == 4.0
    
    # 5. Fifth signal (cooldown expired) -> emit PENDING (p3) because it's better than current
    # t=12000 (age=11s > 10s)
    p5 = {"id": "sig5"}
    ind5 = {"of_confirm_score": 1.5} # score = 3.0 (which is < 4.0)
    res5 = simulate_signal_emission(rt, 12000, ind5, p5)
    
    # Should return p3 (pending) because p3 score(4.0) > p5 score(3.0)
    assert res5 == p3
    assert rt.last_signal_ts == 12000
    assert rt.pending_payload is None
    
def test_burst_selector_current_is_better():
    rt = SymbolRuntime()
    rt.config = {"signal_cooldown_sec": 10}
    
    # 1. Emit initial
    simulate_signal_emission(rt, 1000, {"of_confirm_score": 1.0}, {"id": "sig1"})
    
    # 2. Pending (weak)
    simulate_signal_emission(rt, 2000, {"of_confirm_score": 0.5}, {"id": "weak_pending"})
    
    # 3. Cooldown expired, New signal is STRONG
    # t=12000
    p_strong = {"id": "strong_new"}
    ind_strong = {"of_confirm_score": 5.0} # score=10
    
    res = simulate_signal_emission(rt, 12000, ind_strong, p_strong)
    
    # Pending (score ~1.0) vs Strong (score ~10.0) -> should pick Strong
    assert res == p_strong
    assert rt.pending_payload is None
