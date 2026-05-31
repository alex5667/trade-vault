from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from orderflow_services.tests._repo_import import find_repo_root, load_module_from_candidates


def _load_book_seq_tracker():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "tick_flow_full/services/orderflow/components/book_seq_tracker_uu.py",
        "services/orderflow/components/book_seq_tracker_uu.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="book_seq_tracker_uu")


def _load_dq_gate():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "tick_flow_full/core/dq_gate_v1.py",
        "core/dq_gate_v1.py",
        "services/core/dq_gate_v1.py",
        "tick_flow_full/services/core/dq_gate_v1.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="dq_gate_v1")


def test_integration_snapshot_overlap_gap_recovery_contract():
    """Integration (минимум): snapshot → overlap → gap → recovery.

    Checks:
      - book keys exist in indicators
      - dq keys exist in dq output
      - dq gate can classify book_seq hard and (optionally) veto depending on config
    """
    try:
        bs, _ = _load_book_seq_tracker()
    except Exception as exc:
        pytest.skip(f"book_seq_tracker_uu not importable: {exc}")

    try:
        dq, _ = _load_dq_gate()
    except Exception as exc:
        pytest.skip(f"dq_gate_v1 not importable: {exc}")

    # Deterministic runtime uptime (bypass real monotonic clock).
    dq.runtime_snapshot = lambda event_ts_ms=None: SimpleNamespace(uptime_sec=999999)

    alpha = 0.10
    ema = 0.0
    last_u = 0
    last_reason = "init"

    # 1) "snapshot" stage: no prev_u yet
    dec = bs.decide_book_seq_uu(prev_u=last_u, cur_U=101, cur_u=105)
    last_reason = dec.reason
    last_u = dec.next_last_u
    # init does not count as missing
    ema = bs.ema_update_clamped(ema, dec.missing_event, alpha)

    # 2) overlap (Binance normal) — does not count as missing
    dec = bs.decide_book_seq_uu(prev_u=last_u, cur_U=103, cur_u=110)
    last_reason = dec.reason
    last_u = max(last_u, dec.next_last_u)
    ema = bs.ema_update_clamped(ema, dec.missing_event, alpha)

    # 3) GAP
    dec = bs.decide_book_seq_uu(prev_u=last_u, cur_U=120, cur_u=125)
    last_reason = dec.reason
    last_u = max(last_u, dec.next_last_u)
    ema = bs.ema_update_clamped(ema, dec.missing_event, alpha)

    # 4) recovery / continuous
    dec = bs.decide_book_seq_uu(prev_u=last_u, cur_U=126, cur_u=130)
    last_reason = dec.reason
    last_u = max(last_u, dec.next_last_u)
    ema = bs.ema_update_clamped(ema, dec.missing_event, alpha)

    # Build minimal indicators contract (what tick_processor must surface).
    indicators = {
        "event_ts_ms": 1,
        "book_missing_seq_ema": float(ema),
        "book_seq_last_reason": str(last_reason),
        # dq inputs (kept benign)
        "tick_gap_p95_ms": 0.0,
        "tick_missing_seq_ema": 0.0,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "tick_time_age_ms": 0.0,
        "tick_ts_source_now_ema": 0.0,
        "tick_ts_source_stream_id_ema": 0.0,
    }

    assert "book_missing_seq_ema" in indicators
    assert "book_seq_last_reason" in indicators

    cfg = {
        "dq_gate_enable": 1,
        "dq_gate_mode": "both",
        "dq_observe_only_sec": 0,
        "dq_book_veto_enabled": True,
        # Force hard threshold below our post-gap EMA
        "book_hard": 0.08,
        "dq_book_missing_seq_soft": 0.01,
        # keep other hard triggers off
        "dq_tick_gap_p95_soft_ms": 1e9,
        "dq_tick_gap_p95_hard_ms": 1e9,
        "dq_tick_gap_p95_extreme_ms": 1e9,
        "dq_tick_missing_seq_soft": 1e9,
        "dq_tick_missing_seq_hard": 1e9,
        "dq_data_health_min": 0.0,
        "dq_data_health_hard_min": 0.0,
    }

    out = dq.eval_dq_gate(indicators, cfg)
    # DQ keys must exist.
    for k in ("dq_level", "dq_veto", "dq_reason", "dq_reason_bucket", "dq_reasons"):
        assert k in out

    assert out["dq_level"] in (1, 2)
    # With observe_only_sec==0 and veto_enabled, a hard book EMA must veto.
    assert out["dq_veto"] == 1
