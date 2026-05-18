
import core.runtime_clock as runtime_clock
from core.dq_gate_v1 import eval_dq_gate
from core.dq_observe_only import apply_observe_only_book_veto


def test_apply_observe_only_disabled():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=10_000,
        cfg={},
    )
    assert out.dq_veto == 0
    assert out.suppressed is True
    assert out.suppress_reason == "disabled"


def test_apply_observe_only_warmup_then_enable():
    cfg = {"dq_book_veto_enabled": True, "dq_observe_only_sec": 100}

    out1 = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=99,
        cfg=cfg,
    )
    assert out1.dq_veto == 0
    assert out1.suppressed is True
    assert out1.suppress_reason == "observe_only"

    out2 = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=100,
        cfg=cfg,
    )
    assert out2.dq_veto == 1
    assert out2.suppressed is False


def test_apply_observe_only_non_book_bucket_noop():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="tick_seq",
        dq_reasons=["tick_seq_hard"],
        uptime_sec=0,
        cfg={},
    )
    assert out.dq_veto == 1
    assert out.suppressed is False


def test_runtime_clock_snapshot_deterministic_start(monkeypatch):
    # Make monotonic deterministic for the test.
    monkeypatch.setattr(runtime_clock, "_START_MONO", 100.0)
    monkeypatch.setattr(runtime_clock.time, "monotonic", lambda: 112.34)

    snap = runtime_clock.snapshot(event_ts_ms=1_700_000_000_000)
    assert snap.uptime_sec == 12
    assert snap.runtime_start_ts_ms == 1_700_000_000_000 - 12_000


def test_eval_dq_gate_book_seq_observe_only(monkeypatch):
    # Force uptime < observe-only.
    monkeypatch.setattr(runtime_clock, "_START_MONO", 100.0)
    monkeypatch.setattr(runtime_clock.time, "monotonic", lambda: 105.0)  # uptime=5s

    indicators = {
        "event_ts_ms": 1_700_000_000_000,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "book_missing_seq_ema": 0.30,
    }
    cfg2 = {
        "dq_gate_enable": 1,
        "dq_mode": "safe",
        "book_hard": 0.25,
        "dq_book_veto_enabled": True,
        "dq_observe_only_sec": 100,
        "dq_gate_mode": "enforce",
    }

    out = eval_dq_gate(indicators=indicators, cfg2=cfg2)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_reason"] == "book_seq"
    assert out["dq_veto"] == 0
    assert out.get("dq_veto_suppressed") == 1
    assert out.get("dq_veto_suppressed_reason") == "observe_only"


def test_eval_dq_gate_book_seq_after_warmup(monkeypatch):
    # Force uptime >= observe-only.
    monkeypatch.setattr(runtime_clock, "_START_MONO", 100.0)
    monkeypatch.setattr(runtime_clock.time, "monotonic", lambda: 250.0)  # uptime=150s

    indicators = {
        "event_ts_ms": 1_700_000_000_000,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "book_missing_seq_ema": 0.30,
    }
    cfg2 = {
        "dq_gate_enable": 1,
        "dq_mode": "safe",
        "book_hard": 0.25,
        "dq_book_veto_enabled": True,
        "dq_observe_only_sec": 100,
        "dq_gate_mode": "enforce",
    }

    out = eval_dq_gate(indicators=indicators, cfg2=cfg2)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_veto"] == 1
    assert out.get("dq_veto_suppressed", 0) == 0


def test_eval_dq_gate_book_seq_enabled_false(monkeypatch):
    # Even after warmup, enabled flag must gate veto.
    monkeypatch.setattr(runtime_clock, "_START_MONO", 100.0)
    monkeypatch.setattr(runtime_clock.time, "monotonic", lambda: 250.0)  # uptime=150s

    indicators = {
        "event_ts_ms": 1_700_000_000_000,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        "book_missing_seq_ema": 0.30,
    }
    cfg2 = {
        "dq_gate_enable": 1,
        "dq_mode": "safe",
        "book_hard": 0.25,
        "dq_book_veto_enabled": False,
        "dq_observe_only_sec": 100,
        "dq_gate_mode": "enforce",
    }

    out = eval_dq_gate(indicators=indicators, cfg2=cfg2)
    assert out["dq_level"] == 2
    assert out["dq_reason_bucket"] == "book_seq"
    assert out["dq_veto"] == 0
    assert out.get("dq_veto_suppressed") == 1
    assert out.get("dq_veto_suppressed_reason") == "book_veto_disabled"
