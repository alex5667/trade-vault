from core.dq_gate_v1 import eval_dq_gate


def _cfg(**kw):
    base = {
        "dq_gate_enable": 1,
        "dq_gate_mode": "enforce",
    }
    base.update(kw)
    return base


def test_bstep_gap_soft_sets_level1_bucket_gap_p95_no_veto():
    cfg = _cfg(dq_tick_gap_p95_soft_ms=3000, dq_tick_gap_p95_hard_ms=10000)
    ind = {"tick_gap_p95_ms": 3500}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1
    assert out["dq_reason_bucket"] == "gap_p95"
    assert out["dq_veto"] == 0


def test_bstep_gap_hard_sets_level2_bucket_gap_p95_veto():
    cfg = _cfg(dq_tick_gap_p95_soft_ms=3000, dq_tick_gap_p95_hard_ms=10000)
    ind = {"tick_gap_p95_ms": 12000}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "gap_p95"
    assert out["dq_veto"] == 1


def test_bstep_tick_seq_soft_sets_level1_bucket_tick_seq():
    cfg = _cfg(dq_tick_missing_seq_soft=2, dq_tick_missing_seq_hard=10)
    ind = {"tick_missing_seq_ema": 2.5}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1
    assert out["dq_reason_bucket"] == "tick_seq"
    assert out["dq_veto"] == 0


def test_bstep_tick_seq_hard_sets_level2_bucket_tick_seq_veto():
    cfg = _cfg(dq_tick_missing_seq_soft=2, dq_tick_missing_seq_hard=10)
    ind = {"tick_missing_seq_ema": 12}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "tick_seq"
    assert out["dq_veto"] == 1


def test_bstep_book_seq_hard_disabled_suppresses_veto():
    cfg = _cfg(dq_book_hard=30, dq_book_veto_enabled=False, dq_observe_only_sec=86400, dq_data_health_hard_min=0.1)
    ind = {"book_missing_seq_ema": 35}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_veto"] == 0
    assert out.get("dq_veto_suppressed") == 1
    assert out.get("dq_veto_suppressed_reason") == "book_veto_disabled"


def test_bstep_book_seq_hard_warmup_suppresses_veto():
    # Make warmup effectively always true in unit-test (uptime_sec is small).
    cfg = _cfg(dq_book_hard=30, dq_book_veto_enabled=True, dq_observe_only_sec=10**9, dq_data_health_hard_min=0.1)
    ind = {"book_missing_seq_ema": 35}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_veto"] == 0
    assert out.get("dq_veto_suppressed") == 1
    assert out.get("dq_veto_suppressed_reason") == "observe_only"


def test_bstep_book_seq_hard_after_window_allows_veto():
    cfg = _cfg(dq_book_hard=30, dq_book_veto_enabled=True, dq_observe_only_sec=0)
    ind = {"book_missing_seq_ema": 35}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_veto"] == 1


def test_bstep_data_health_bucket():
    cfg = _cfg(dq_data_health_min=0.85, dq_data_health_hard_min=0.70)
    ind = {"data_health": 0.6}
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "data_health"
