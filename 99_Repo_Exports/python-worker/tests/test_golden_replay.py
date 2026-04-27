import pytest
import types
from core.golden_replay import GoldenReplayRunner

def test_golden_replay_determinism():
    """
    Smoke test: running the same case twice should yield identical results.
    """
    runner = GoldenReplayRunner()
    
    # Minimal case input
    case_input = {
        "id": "smoke_1",
        "inputs": {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "runtime_snapshot": {
                "last_obi_event": {"ts_ms": 1000, "obi": 100}
            },
            "cfg": {"of_score_min": 0.5},
            "indicators": {}
        }
    }
    
    res1 = runner.run_case(case_input)
    res2 = runner.run_case(case_input)
    
    # Check that we got results
    assert "result" in res1
    assert "result" in res2
    
    r1 = res1["result"]
    r2 = res2["result"]
    
    # Check key fields equality
    assert r1["ok"] == r2["ok"]
    assert r1["score"] == r2["score"]
    assert r1["scenario"] == r2["scenario"]
    assert r1["have"] == r2["have"]
    assert r1["need"] == r2["need"]
    
    # Ensure no error
    assert "error" not in res1
