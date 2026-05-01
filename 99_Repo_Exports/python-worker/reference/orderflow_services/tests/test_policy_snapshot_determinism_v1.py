from core_snapshot.policy_snapshot_v1 import build_dq_policy_snapshot


def test_policy_snapshot_hash_is_order_invariant_v1() -> None:
    cfg_a = {
        "dq_mode": "safe",
        "dq_gate_mode": "enforce",
        "dq_gap_soft_ms": 3000,
        "dq_gap_hard_ms": 10000,
        "BOOK_STREAM_INTERVAL_MS": 100,
        "dq_book_veto_enabled": False,
        "dq_observe_only_sec": 86400,
        # noise keys (must not affect hash)
        "irrelevant": "x",
        "another": 123,
    }
    cfg_b = {
        "another": 123,
        "irrelevant": "x",
        "dq_observe_only_sec": 86400,
        "dq_book_veto_enabled": False,
        "BOOK_STREAM_INTERVAL_MS": 100,
        "dq_gap_hard_ms": 10000,
        "dq_gap_soft_ms": 3000,
        "dq_gate_mode": "enforce",
        "dq_mode": "safe",
    }

    snap_a, h_a = build_dq_policy_snapshot(cfg_a)
    snap_b, h_b = build_dq_policy_snapshot(cfg_b)

    assert h_a == h_b
    assert snap_a.thresholds.dq_book_seq_ema_alpha == snap_b.thresholds.dq_book_seq_ema_alpha


def test_policy_snapshot_alpha_mapping_v1() -> None:
    snap_100, _ = build_dq_policy_snapshot({"BOOK_STREAM_INTERVAL_MS": 100})
    snap_250, _ = build_dq_policy_snapshot({"BOOK_STREAM_INTERVAL_MS": 250})
    snap_500, _ = build_dq_policy_snapshot({"BOOK_STREAM_INTERVAL_MS": 500})
    snap_1000, _ = build_dq_policy_snapshot({"BOOK_STREAM_INTERVAL_MS": 1000})
    assert abs(snap_100.thresholds.dq_book_seq_ema_alpha - 0.10) < 1e-9
    assert abs(snap_250.thresholds.dq_book_seq_ema_alpha - 0.20) < 1e-9
    assert abs(snap_500.thresholds.dq_book_seq_ema_alpha - 0.30) < 1e-9
    assert abs(snap_1000.thresholds.dq_book_seq_ema_alpha - 0.40) < 1e-9
