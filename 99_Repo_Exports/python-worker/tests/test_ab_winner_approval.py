
from services.ab_winner_approval import decide_approve, active_arm_key, lock_key

def test_approve_reject_min_samples():
    sugg = {"winner_arm":"B","arms":{"B":{"n":10,"mean_r":0.2},"A":{"n":100,"mean_r":0.1}}}
    d = decide_approve(sugg, min_samples=40, min_edge_r=0.05)
    assert d.ok is False
    assert "min_samples" in d.reason

def test_approve_reject_edge_small():
    sugg = {"winner_arm":"B","arms":{"B":{"n":50,"mean_r":0.11},"A":{"n":60,"mean_r":0.10}}}
    d = decide_approve(sugg, min_samples=40, min_edge_r=0.05)
    assert d.ok is False
    assert "edge_too_small" in d.reason

def test_approve_ok_r_edge():
    sugg = {"winner_arm":"B","arms":{"B":{"n":50,"mean_r":0.20},"A":{"n":60,"mean_r":0.10}}}
    d = decide_approve(sugg, min_samples=40, min_edge_r=0.05)
    assert d.ok is True
    assert d.winner == "B"

def test_keys_specific():
    assert active_arm_key(symbol="ethusdt", regime="thin", group="thin").endswith(":ETHUSDT:thin:thin")
    assert lock_key(symbol="ethusdt", regime="thin", group="thin").endswith(":ETHUSDT:thin:thin")
