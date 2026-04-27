from __future__ import annotations

from pathlib import Path

import pytest

from orderflow_services.tests._repo_import import find_repo_root, load_module_from_candidates


class _DummyCounter:
    def __init__(self) -> None:
        self.incs = 0
        self.last_labels = None

    def labels(self, **kwargs):
        self.last_labels = dict(kwargs)
        return self

    def inc(self, v: float = 1.0) -> None:
        self.incs += int(v)


class _Runtime:
    def __init__(self, symbol: str, *, last_u: int, ema: float, alpha: float) -> None:
        self.symbol = symbol
        self.config = {"book_missing_seq_ema_alpha": float(alpha)}
        self.book_seq_last_u = int(last_u)
        self.book_missing_seq_ema = float(ema)
        self.book_seq_last_reason = "init"


def _load_book_processor():
    repo = find_repo_root(Path(__file__).resolve())
    candidates = [
        "tick_flow_full/services/orderflow/components/book_processor.py",
        "services/orderflow/components/book_processor.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="book_processor")


def test_book_processor_gap_detected_increments_events_total():
    """Unit: gap -> events_total.inc() and ema updates."""
    try:
        bp, _ = _load_book_processor()
    except Exception as exc:
        pytest.skip(f"book_processor not importable in this checkout: {exc}")

    dummy = _DummyCounter()
    # Monkeypatch module-level metric.
    bp.book_missing_seq_events_total = dummy

    rt = _Runtime("BTCUSDT", last_u=160, ema=0.0, alpha=0.5)
    proc = bp.BookProcessor()
    proc._update_book_missing_seq(rt, {"U": 170, "u": 175})

    assert rt.book_seq_last_reason in ("gap", "book_seq_gap")
    assert rt.book_seq_last_u == 175
    assert rt.book_missing_seq_ema > 0.0
    assert dummy.incs == 1
    assert dummy.last_labels == {"symbol": "BTCUSDT"}


def test_book_processor_ok_does_not_increment_events_total():
    """Unit: ok/overlap must not count as missing event."""
    try:
        bp, _ = _load_book_processor()
    except Exception as exc:
        pytest.skip(f"book_processor not importable in this checkout: {exc}")

    dummy = _DummyCounter()
    bp.book_missing_seq_events_total = dummy

    rt = _Runtime("BTCUSDT", last_u=160, ema=0.0, alpha=0.5)
    proc = bp.BookProcessor()
    proc._update_book_missing_seq(rt, {"U": 161, "u": 165})

    assert rt.book_seq_last_reason in ("ok", "overlap", "reorder_or_reset", "init", "no_seq_fields", "no_u")
    assert rt.book_seq_last_u == 165
    assert dummy.incs == 0
