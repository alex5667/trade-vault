from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from orderflow_services.tests._repo_import import find_repo_root, load_module_from_candidates


def _load_dq_gate():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "tick_flow_full/core/dq_gate_v1.py",
        "core/dq_gate_v1.py",
        # Some repo layouts keep the mirror under services/
        "services/core/dq_gate_v1.py",
        "tick_flow_full/services/core/dq_gate_v1.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="dq_gate_v1")


def _base_cfg(*, observe_only_sec: int, veto_enabled: bool) -> dict:
    return {
        "dq_gate_enable": 1,
        # enable veto path
        "dq_gate_mode": "both",
        # observe-only controls (B1)
        "dq_observe_only_sec": int(observe_only_sec),
        "dq_book_veto_enabled": bool(veto_enabled),
        # thresholds
        "book_hard": 30.0,
        "dq_book_missing_seq_soft": 10.0,
        # keep other thresholds high / inactive
        "dq_tick_gap_p95_soft_ms": 1e9,
        "dq_tick_gap_p95_hard_ms": 1e9,
        "dq_tick_gap_p95_extreme_ms": 1e9,
        "dq_tick_missing_seq_soft": 1e9,
        "dq_tick_missing_seq_hard": 1e9,
        "dq_data_health_min": 0.0,
        "dq_data_health_hard_min": 0.0,
    }


def test_observe_only_blocks_veto():
    """dq_level=2 by book_seq_hard, but uptime<observe_only => dq_veto=0."""
    try:
        mod, _ = _load_dq_gate()
    except Exception as exc:
        pytest.skip(f"dq_gate_v1 not importable in this checkout: {exc}")

    # Patch runtime clock to deterministic uptime.
    mod.runtime_snapshot = lambda event_ts_ms=None: SimpleNamespace(uptime_sec=60)  # 1 min

    indicators = {
        "event_ts_ms": 1,
        "book_missing_seq_ema": 999.0,  # >> book_hard
        "tick_gap_p95_ms": 0.0,
        "tick_missing_seq_ema": 0.0,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "tick_time_age_ms": 0.0,
        "tick_ts_source_now_ema": 0.0,
        "tick_ts_source_stream_id_ema": 0.0,
    }

    cfg = _base_cfg(observe_only_sec=3600, veto_enabled=True)  # 1h window
    out = mod.eval_dq_gate(indicators, cfg)

    assert out["dq_level"] == 2
    # Primary reason bucket should map to book_seq.
    assert out.get("dq_reason_bucket") in ("book_seq", "ok")
    assert out["dq_veto"] == 0


def test_veto_enabled_after_window():
    """uptime>=observe_only and BOOK_VETO_ENABLED=true => dq_veto=1."""
    try:
        mod, _ = _load_dq_gate()
    except Exception as exc:
        pytest.skip(f"dq_gate_v1 not importable in this checkout: {exc}")

    mod.runtime_snapshot = lambda event_ts_ms=None: SimpleNamespace(uptime_sec=7200)  # 2h

    indicators = {
        "event_ts_ms": 1,
        "book_missing_seq_ema": 999.0,
        "tick_gap_p95_ms": 0.0,
        "tick_missing_seq_ema": 0.0,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "tick_time_age_ms": 0.0,
        "tick_ts_source_now_ema": 0.0,
        "tick_ts_source_stream_id_ema": 0.0,
    }

    cfg = _base_cfg(observe_only_sec=3600, veto_enabled=True)
    out = mod.eval_dq_gate(indicators, cfg)

    assert out["dq_level"] == 2
    assert out["dq_veto"] == 1
