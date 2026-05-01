from __future__ import annotations

from tick_flow_full.core.dq_gate_v1 import eval_dq_gate


def _base_cfg(dq_mode: str) -> dict:
    return {
        "dq_gate_enable": 1,
        "dq_gate_mode": "penalty",
        "dq_mode": dq_mode,
        "dq_pen_max": 0.10,
        "dq_tick_gap_min_samples": 50,
        "dq_tick_gap_requires_seq": 1,
        "dq_data_health_min": 0.85,
        "dq_data_health_hard_min": 0.70,
        "dq_tick_age_ms_max": 5000,
        "dq_skew_ema_ms_max": 1000,
    }


def _base_ind() -> dict:
    return {
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "tick_time_age_ms": 0.0,
        "tick_ts_source_now_ema": 0.0,
        "tick_ts_source_stream_id_ema": 0.0,
        "tick_unknown_side_ema": 0.0,
        "tick_gap_p95_ms": 0.0,
        "tick_gap_n": 0,
        "tick_missing_seq_ema": 0.0,
        "book_missing_seq_ema": 0.0,
    }


def test_strict_defaults_tick_seq_soft_vs_hard() -> None:
    cfg = _base_cfg("strict")

    ind = _base_ind()
    ind["tick_missing_seq_ema"] = 0.06  # >= 0.05 strict soft
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1
    assert "tick_seq" in out["dq_reasons"]
    assert out["dq_reason_bucket"] in ("tick_seq", "book_seq")

    ind2 = _base_ind()
    ind2["tick_missing_seq_ema"] = 0.15  # >= 0.15 strict hard
    out2 = eval_dq_gate(ind2, cfg)
    assert out2["dq_level"] == 2
    assert out2["dq_veto"] == 1
    assert "tick_seq" in out2["dq_reasons"]


def test_strict_defaults_book_seq_soft_vs_hard() -> None:
    cfg = _base_cfg("strict")

    ind = _base_ind()
    ind["book_missing_seq_ema"] = 0.04  # >= 0.03 strict soft
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1
    assert "book_seq" in out["dq_reasons"]
    assert out["dq_reason_bucket"] == "book_seq"

    ind2 = _base_ind()
    ind2["book_missing_seq_ema"] = 0.10  # >= 0.10 strict hard
    out2 = eval_dq_gate(ind2, cfg)
    assert out2["dq_level"] == 2
    assert out2["dq_veto"] == 1
    assert "book_seq" in out2["dq_reasons"]


def test_strict_gap_p95_soft_hard_extreme_requires_seq() -> None:
    cfg = _base_cfg("strict")

    # Soft by gap
    ind = _base_ind()
    ind["tick_gap_p95_ms"] = 3200
    ind["tick_gap_n"] = 50
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1
    assert "gap_p95" in out["dq_reasons"]
    assert out["dq_reason_bucket"] == "gap_p95"

    # Hard gap alone should stay SOFT when requires_seq=1 and no seq degrade
    ind2 = _base_ind()
    ind2["tick_gap_p95_ms"] = 4600
    ind2["tick_gap_n"] = 50
    out2 = eval_dq_gate(ind2, cfg)
    assert out2["dq_level"] == 1
    assert out2["dq_veto"] == 0

    # Hard gap becomes HARD if seq degrade exists
    ind3 = _base_ind()
    ind3["tick_gap_p95_ms"] = 4600
    ind3["tick_gap_n"] = 50
    ind3["tick_missing_seq_ema"] = 0.06
    out3 = eval_dq_gate(ind3, cfg)
    assert out3["dq_level"] == 2
    assert out3["dq_veto"] == 1

    # Extreme always HARD
    ind4 = _base_ind()
    ind4["tick_gap_p95_ms"] = 9000
    ind4["tick_gap_n"] = 50
    out4 = eval_dq_gate(ind4, cfg)
    assert out4["dq_level"] == 2
    assert out4["dq_veto"] == 1


def test_safe_defaults_match_spec() -> None:
    cfg = _base_cfg("safe")

    # Tick seq soft (>=0.125) vs hard (>=0.25)
    ind = _base_ind()
    ind["tick_missing_seq_ema"] = 0.13
    out = eval_dq_gate(ind, cfg)
    assert out["dq_level"] == 1

    ind2 = _base_ind()
    ind2["tick_missing_seq_ema"] = 0.25
    out2 = eval_dq_gate(ind2, cfg)
    assert out2["dq_level"] == 2

    # Gap soft/hard/extreme (5000/8000/12000)
    ind3 = _base_ind()
    ind3["tick_gap_p95_ms"] = 5100
    ind3["tick_gap_n"] = 50
    out3 = eval_dq_gate(ind3, cfg)
    assert out3["dq_level"] == 1

    ind4 = _base_ind()
    ind4["tick_gap_p95_ms"] = 12000
    ind4["tick_gap_n"] = 50
    out4 = eval_dq_gate(ind4, cfg)
    assert out4["dq_level"] == 2
    assert out4["dq_veto"] == 1
