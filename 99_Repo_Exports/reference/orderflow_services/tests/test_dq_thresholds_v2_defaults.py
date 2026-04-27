import sys
from pathlib import Path

# Ensure repo root is on sys.path even under pytest --import-mode=importlib
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_resolve_dq_thresholds_strict_100ms_defaults():
    from core_snapshot.dq_thresholds import resolve_dq_thresholds

    th = resolve_dq_thresholds({"dq_mode": "strict", "book_stream_interval_ms": 100})
    assert th.mode == "strict"
    assert th.gap_soft_ms == 3000
    assert th.gap_hard_ms == 15000
    assert th.gap_extreme_ms == 20000
    assert th.min_samples == 50
    assert abs(th.tick_soft - 0.05) < 1e-12
    assert abs(th.tick_hard - 0.15) < 1e-12
    assert abs(th.book_hard - 0.10) < 1e-12
    # strict ratio ~0.30 => 0.10 -> 0.03
    assert abs(th.book_soft - 0.03) < 1e-12
    assert th.book_stream_interval_ms == 100
    assert abs(th.book_seq_ema_alpha - 0.10) < 1e-12


def test_resolve_dq_thresholds_safe_250ms_defaults_and_alpha():
    from core_snapshot.dq_thresholds import resolve_dq_thresholds

    th = resolve_dq_thresholds({"dq_mode": "safe", "book_stream_interval_ms": 250})
    assert th.mode == "safe"
    assert th.gap_soft_ms == 5000
    assert th.gap_hard_ms == 20000
    assert th.gap_extreme_ms == 30000
    assert th.min_samples == 50
    assert abs(th.tick_hard - 0.25) < 1e-12
    assert abs(th.tick_soft - 0.125) < 1e-12
    assert abs(th.book_hard - 0.35) < 1e-12
    assert abs(th.book_soft - 0.175) < 1e-12
    assert th.book_stream_interval_ms == 250
    assert abs(th.book_seq_ema_alpha - 0.20) < 1e-12


def test_resolve_dq_thresholds_explicit_alpha_override():
    from core_snapshot.dq_thresholds import resolve_dq_thresholds

    th = resolve_dq_thresholds({"dq_mode": "safe", "book_stream_interval_ms": 100, "dq_book_seq_ema_alpha": 0.33})
    assert abs(th.book_seq_ema_alpha - 0.33) < 1e-12
